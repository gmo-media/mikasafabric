#
# Copyright (c) 2013,2014, Oracle and/or its affiliates. All rights reserved.
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

"""Package containing all services available in the manager.

Services are interfaces accessible externally over the network.
"""

import inspect
import logging
import pkgutil

import mysql.fabric.protocols.xmlrpc as _protocol
from mysql.fabric.protocols.mysqlrpc import FabricMySQLServer

from mysql.fabric.utils import (
    Singleton
    )

from mysql.fabric.command import (
    get_groups,
    get_commands,
    get_command,
    )

from mysql.fabric.errors import (
    ServiceError
)

_LOGGER = logging.getLogger(__name__)


def find_commands(config=None):
    """Find which are the available commands.
    """
    for imp, name, ispkg in pkgutil.walk_packages(__path__, __name__ + "."):
        mod = imp.find_module(name).load_module(name)
        _LOGGER.debug("%s %s has got __name__ %s",
            "Package" if ispkg else "Module", name, mod.__name__
        )
        if config is not None:
            for (mem_name, mem_value) in inspect.getmembers(mod):
                if mem_name == "configure" and inspect.isfunction(mem_value):
                    mem_value(config)

def find_client():
    """Return a proxy to access the Fabric server.
    """
    return _protocol.MyClient()


class ServiceManager(Singleton):
    """This is the service manager, which processes service requests.

    The service manager supports XML-RPC and MySQL-RPC using the
    MySQL protocol.

    Services are not automatically loaded when the service manager is
    constructed, so the load_services have to be called explicitly to
    load the services in the package.
    """
    def __init__(self, services, number_threads, ssl=None):
        """Setup all protocol services.
        """
        Singleton.__init__(self)
        self.__services = services

        # XMLRPC
        self.__rpc_server = None
        if 'protocol.xmlrpc' in services:
            host, port = services['protocol.xmlrpc'].split(':')
            self.__rpc_server = _protocol.MyServer(
                host, int(port), number_threads, ssl
            )

        # MySQL Protocol
        self.__mysql_server = None
        if 'protocol.mysql' in services:
            host, port = services['protocol.mysql'].split(':')
            self.__mysql_server = FabricMySQLServer(
                host, int(port), number_threads, ssl
            )

    def address(self, protocol=None):
        """Return addresses in use by the service.

        :param protocol: Address in use by a protocol.
        :return: Address as host:port.
        :rtype: String.
        """
        services = self.__services.copy()

        if protocol is None:
            return services

        if protocol in services:
            return {protocol : services[protocol]}

        raise ServiceError("Protocol (%s) is not supported." % (protocol, ))

    def get_number_sessions(self):
        """Return the number of concurrent sessions.
        """
        return self.__rpc_server.get_number_sessions()

    def start(self):
        """Start all services managed by the service manager.
        """
        if self.__mysql_server:
            try:
                self.__mysql_server.start()
            except Exception as error:
                _LOGGER.error("Error starting thread: (%s).", error)
            finally:
                _LOGGER.debug("MySQL-RPC server thread created")

        if self.__rpc_server:
            try:
                self.__rpc_server.start()
            except Exception as error:
                _LOGGER.error("Error starting thread: (%s).", error)
            finally:
                _LOGGER.debug("XML-RPC server thread created")

    def shutdown(self):
        """Shut down all services managed by the service manager.
        """
        if self.__mysql_server:
            self.__mysql_server.shutdown()
        if self.__rpc_server:
            self.__rpc_server.shutdown()

    def wait(self):
        """Wait until all the sevices are properly finished.
        """
        if self.__rpc_server:
            self.__rpc_server.wait()

    def load_services(self, options, config):
        """Load services into each protocol server.

        :param options: The options for the commands that shall be
                        created.
        :param config: The configuration for the commands that shall
                       be created.
        """
        _LOGGER.info("Loading Services.")

        find_commands(config)

        for group_name in get_groups():
            for command_name in get_commands(group_name):
                command = get_command(group_name, command_name)
                if hasattr(command, "execute"):
                    _LOGGER.debug(
                        "Registering %s.", command.group_name + '.' + \
                        command.command_name
                        )
                    if self.__mysql_server:
                        cmd = command()
                        cmd.setup_server(self.__mysql_server, options, config)
                        self.__mysql_server.register_command(cmd)

                    if self.__rpc_server:
                        cmd = command()
                        cmd.setup_server(self.__rpc_server, options, config)
                        self.__rpc_server.register_command(cmd)
