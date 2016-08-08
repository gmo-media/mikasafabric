#
# Copyright (c) 2013,2015, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
#

"""Provide key functions to start, stop and check Fabric's availability and
information on available commands.
"""
import getpass
import logging
import logging.handlers
import os.path
import urlparse

from mysql.fabric import (
    backup as _backup,
    config as _config,
    errors as _errors,
    events as _events,
    executor as _executor,
    failure_detector as _failure_detector,
    persistence as _persistence,
    recovery as _recovery,
    services as _services,
    utils as _utils,
    server as _server,
    error_log as _error_log,
    credentials,
    handler as _logging,
    providers as providers,
)

from mysql.fabric.command import (
    Command,
    CommandResult,
    ResultSet,
)

from mysql.fabric.handler import (
    MySQLHandler,
)

from mysql.fabric.node import (
    FabricNode
)

_LOGGER = logging.getLogger(__name__)

# Logging levels.
_LOGGING_LEVELS = {
    "CRITICAL" : logging.CRITICAL,
    "ERROR" : logging.ERROR,
    "WARNING" : logging.WARNING,
    "INFO" : logging.INFO,
    "DEBUG" : logging.DEBUG
}

# Number of concurrent threads that are created to handle requests.
DEFAULT_N_THREADS = 5

# Number of concurrent executors that are created to handle jobs.
DEFAULT_N_EXECUTORS = 5

#Default TTL value
DEFAULT_TTL = 1

# MySQL's port
_MYSQL_PORT = 3306

class Logging(Command):
    """Set logging level.
    """
    group_name = "manage"
    command_name = "logging_level"

    def execute(self, module, level):
        """ Set logging level.

        :param module: Module that will have its logging level changed.
        :param level: The logging level that will be set.
        :return: Return True if the logging level is changed. Otherwise,
        False.
        """
        try:
            __import__(module)
            logger = logging.getLogger(module)
            logger.setLevel(_LOGGING_LEVELS[level.upper()])
        except (KeyError, ImportError) as error:
            _LOGGER.debug(error)
            return CommandResult(str(error))
        return CommandResult(None)


class Ping(Command):
    """Check whether Fabric server is running or not.
    """
    group_name = "manage"
    command_name = "ping"

    def execute(self):
        """Check whether Fabric server is running or not.
        """
        pass


class Start(Command):
    """Start the Fabric server.
    """
    group_name = "manage"
    command_name = "start"

    def dispatch(self, daemonize=False):
        """Start the Fabric server.
        """
        # Configure logging.
        _configure_logging(self.config, daemonize)

        # Configure signal handlers.
        _utils.catch_signals(True)

        # Configure connections.
        _configure_connections(self.config)

        credentials.check_initial_setup(
            self.config, _persistence.MySQLPersister(),
            check_only=True
        )

        # Daemonize ourselves.
        if daemonize:
            _utils.daemonize()

        # Start Fabric server.
        _start(self.options, self.config)
        _services.ServiceManager().wait()


class Setup(Command):
    """Setup Fabric Storage System.

    Create a database and necessary objects.
    """
    group_name = "manage"
    command_name = "setup"

    def dispatch(self):
        """Setup Fabric Storage System.
        """
        # Configure logging.
        _configure_logging(self.config, False)

        # Configure signal handlers.
        _utils.catch_signals(True)

        # Configure connections.
        _configure_connections(self.config)

        # Create database and objects.
        _persistence.setup(config=self.config)

        credentials.check_initial_setup(self.config,
                                        _persistence.MySQLPersister())


class Teardown(Command):
    """Teardown Fabric Storage System.

    Drop database and its objects.
    """
    group_name = "manage"
    command_name = "teardown"

    def dispatch(self):
        """Teardown Fabric Storage System.
        """
        # Configure logging.
        _configure_logging(self.config, False)

        # Configure signal handlers.
        _utils.catch_signals(True)

        # Configure connections.
        _configure_connections(self.config)

        # Drop database and objects.
        _persistence.teardown()


def _create_file_handler(config, info, delay=0):
    """Define a file handler where logging information will be
    sent to.
    """
    from logging.handlers import RotatingFileHandler
    assert info.scheme == 'file'
    if info.netloc:
        raise _errors.ConfigurationError(
            "Malformed file URL '%s'" % (info.geturl(),)
        )
    if os.path.isabs(info.path):
        path = info.path
    else:
        # Relative path, fetch the logdir from the configuration.
        # Using 'logging' section instead of 'DEFAULT' to allow
        # configuration parameters to be overridden in the logging
        # section.
        logdir = config.get('logging', 'logdir')
        path = os.path.join(logdir, info.path)
    return RotatingFileHandler(path, delay=delay)

# A URL provided should either have a path or an address, but not
# both. For example:
#
#   syslog:///dev/log       ---> Address is /dev/log
#   syslog://localhost:555  ---> Address is ('localhost', 555)
#   syslog://my.example.com ---> Address is ('my.example.com', 541)
#
# The following is not allowed:
#
#   syslog://example.com/foo/bar
def _create_syslog_handler(config, info):
    """Define a syslog handler where logging information will be
    sent to.
    """
    from logging.handlers import SYSLOG_UDP_PORT, SysLogHandler
    assert info.scheme == 'syslog'
    if info.netloc and info.path:
        raise _errors.ConfigurationError(
            "Malformed syslog URL '%s'" % (info.geturl(),)
        )
    if info.netloc:
        assert not info.path
        address = info.netloc.split(':')
        if len(address) == 1:
            address.append(SYSLOG_UDP_PORT)
    elif info.path:
        assert not info.netloc
        address = info.path

    return SysLogHandler(address=address)

_LOGGING_HANDLER = {
    'file': _create_file_handler,
    'syslog': _create_syslog_handler,
}

def _configure_logging(config, daemonize):
    """Configure the logging system.
    """
    # Set up the logging information.
    logger = logging.getLogger("mysql.fabric")
    handler = None

    # Set up logging handler
    if daemonize:
        urlinfo = urlparse.urlparse(config.get('logging', 'url'))
        handler = _LOGGING_HANDLER[urlinfo.scheme](config, urlinfo)
    else:
        handler = logging.StreamHandler()
    mysql_handler = _logging.MySQLHandler()

    formatter = logging.Formatter(
        "[%(levelname)s] %(created)f - %(threadName)s "
        "- %(message)s")
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.addHandler(mysql_handler)

    logger.setLevel(_LOGGING_LEVELS["DEBUG"])
    mysql_handler.setLevel(_LOGGING_LEVELS["DEBUG"])
    try:
        level = config.get("logging", "level")
        handler.setLevel(_LOGGING_LEVELS[level.upper()])
    except KeyError:
        handler.setLevel(_LOGGING_LEVELS["INFO"])

def _configure_connections(config):
    """Configure information on database connection and remote
    servers.
    """
    # Configure the number of concurrent executors.
    try:
        number_executors = config.get('executor', "executors")
        number_executors = int(number_executors)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        number_executors = DEFAULT_N_EXECUTORS
    executor = _executor.Executor()
    executor.set_number_executors(number_executors)

    services = {}
    ssl_config = {}

    # XML-RPC service
    try:
        services['protocol.xmlrpc'] = config.get('protocol.xmlrpc', "address")

        try:
            number_threads = config.get('protocol.xmlrpc', "threads")
            number_threads = int(number_threads)
        except (_config.NoOptionError, ValueError):
            number_threads = DEFAULT_N_THREADS

        try:
            for option in ('ssl_ca', 'ssl_key', 'ssl_cert'):
                ssl_config[option] = config.get('protocol.xmlrpc', option)
        except _config.NoOptionError:
            ssl_config = {}
    except _config.NoSectionError:
        raise _errors.ConfigurationError(
            'Configuration for protocol.xmlrpc is required')

    # MySQL-RPC service
    try:
        services['protocol.mysql'] = config.get('protocol.mysql', "address")
    except _config.NoSectionError:
        # No MySQL-RPC configured
        pass

    # Define service configuration
    _services.ServiceManager(services, number_threads, ssl_config)

    # Fetch options to configure the state store.
    address = config.get('storage', 'address')

    try:
        host, port = address.split(':')
        port = int(port)
    except ValueError:
        host = address
        port = _MYSQL_PORT

    user = config.get('storage', 'user')
    database = config.get('storage', 'database')

    try:
        password = config.get('storage', 'password')
    except _config.NoOptionError:
        password = getpass.getpass()

    try:
        connection_timeout = config.get("storage", "connection_timeout")
        connection_timeout = float(connection_timeout)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        connection_timeout = None

    try:
        connection_attempts = config.get("storage", "connection_attempts")
        connection_attempts = int(connection_attempts)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        connection_attempts = None

    try:
        connection_delay = config.get("storage", "connection_delay")
        connection_delay = int(connection_delay)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        connection_delay = None

    try:
        auth_plugin = config.get("storage", "auth_plugin")
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        auth_plugin = None

    # Define state store configuration.
    _persistence.init(
        host=host, port=port, user=user, password=password, database=database,
        connection_timeout=connection_timeout,
        connection_attempts=connection_attempts,
        connection_delay=connection_delay,
        auth_plugin=auth_plugin
    )

def _setup_ttl(config):
    """Read the configured TTL and set its value.
    """
    #configure the TTL to be used for the connectors.
    try:
        ttl = config.get('connector', "ttl")
        ttl = int(ttl)
        _utils.TTL = ttl
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        _utils.TTL = DEFAULT_TTL

def _start(options, config):
    """Start Fabric server.
    """

    # Remove temporary defaults file, which migh have left behind
    # by former runs of Fabric.
    _backup.cleanup_temp_defaults_files()

    #Configure TTL
    _setup_ttl(config)

    # Configure modules that are not dynamic loaded.
    _server.configure(config)
    _error_log.configure(config)
    _failure_detector.configure(config)

    # Load information on all providers.
    providers.find_providers()

    # Load all services into the service manager
    _services.ServiceManager().load_services(options, config)

    # Initilize the state store.
    _persistence.init_thread()

    # Check the maximum number of threads.
    _utils.check_number_threads()

    # Configure Fabric Node.
    fabric = FabricNode()
    reported = _utils.get_time()
    _LOGGER.info(
        "Fabric node version (%s) started. ",
        fabric.version,
        extra={
            'subject' : str(fabric.uuid),
            'category' : MySQLHandler.NODE,
            'type' : MySQLHandler.START,
            'reported' : reported
        }
    )
    fabric.startup = reported

    # Start the executor, failure detector and then service manager. In this
    # scenario, the recovery is sequentially executed after starting the
    # executor and before starting the service manager.
    _events.Handler().start()
    _recovery.recovery()
    _failure_detector.FailureDetector.register_groups()
    _services.ServiceManager().start()


class Stop(Command):
    """Stop the Fabric server.
    """
    group_name = "manage"
    command_name = "stop"

    def execute(self):
        """Stop the Fabric server.
        """
        _shutdown()


def _shutdown():
    """Shutdown Fabric server.
    """
    _failure_detector.FailureDetector.unregister_groups()
    _services.ServiceManager().shutdown()
    _events.Handler().shutdown()
    _events.Handler().wait()
    _LOGGER.info(
        "Fabric node stopped.",
        extra={
            'subject' : 'Node',
            'category' : MySQLHandler.NODE,
            'type' : MySQLHandler.STOP
        }
    )


class FabricLookups(Command):
    """Return a list of Fabric servers.
    """
    group_name = "dump"
    command_name = "fabric_nodes"

    def execute(self, protocol=None):
        """Return a list with all the available Fabric Servers.

        :return: List with existing Fabric Servers.
        :rtype: ["host:port", ...]
        """
        service = _services.ServiceManager()
        rset = ResultSet(names=('host', 'port'),types=(str, int))
        for _, address in service.address(protocol).items():
            rset.append_row(address.split(":"))
        return CommandResult(None, results=rset)
