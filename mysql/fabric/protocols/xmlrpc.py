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

"""Define a XML-RPC server and client.
"""
from uuid import uuid4
from base64 import b64encode, b64decode
import xmlrpclib
import threading
import Queue
import logging
from hashlib import sha1, md5
import re
import os
from datetime import datetime, timedelta
import urllib2
import httplib
import ConfigParser
import traceback

try:
    import ssl
except ImportError:
    # If import fails, we don't have SSL support.
    pass

from SimpleXMLRPCServer import (
    SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
)
from SocketServer import (
    ThreadingMixIn,
)

import mysql.fabric.persistence as _persistence
import mysql.fabric.server_utils as _server_utils
import mysql.fabric.errors as _errors

from mysql.fabric import (
    credentials,
    __version__ as FABRIC_VERSION,
)

from mysql.fabric.utils import (
    FABRIC_UUID,
    TTL,
)

from mysql.fabric.command import CommandResult, ResultSet

_LOGGER = logging.getLogger(__name__)
_RE_DIGEST_AUTH_HEADER = re.compile(r'\s*,(?=(?:[^"]*|"[^"]*"$))\s*')
_RE_METHODNAME = re.compile(r'<methodName>(.*)</methodName>', re.IGNORECASE)
AUTH_EXPIRES = 2 * 60  # in seconds
AUTH_TIME_FMT = "%Y-%m-%dT%H:%M:%S.%f"
PURGE_CLIENTS_TIMER = AUTH_EXPIRES  # in seconds
FORMAT_VERSION = 1

def _encode(result):
    """Encode a CommandResult as an data structure for transfer over XML-RPC.
    """
    # Store the result sets as a list of dictionaries for transfer.
    if result.error:
        packet = [FORMAT_VERSION, str(result.uuid), result.ttl, result.error, []]
    else:
        rows = [
            {
                'info': {
                    'names': [ col.name for col in rs.columns ],
                },
                'rows': list(rs),
            } for rs in result.results
        ]
        packet = [FORMAT_VERSION, str(result.uuid), result.ttl, '', rows]

    _LOGGER.debug("Encoded packet: %s", packet)
    return packet


def _decode(packet):
    """Decode the data structure used with XML-RPC into a CommandResult.

    Since the result sets are merged into a single list of
    dictionaries, we separate them into result sets using the keys for
    the dictionaries.

    """

    _LOGGER.debug("Decode packet: %s", packet)
    version, fabric_uuid, ttl, error, rsets = packet
    if version != FORMAT_VERSION:
        raise TypeError("XML-RPC packet format version was %d, expected %d",
                        version, FORMAT_VERSION)
    result = CommandResult(error, uuid=fabric_uuid, ttl=ttl)
    if len(rsets) > 0:
        for rset in rsets:
            # If the result set contain at least one row, use that to
            # deduce the types for the columns. If not, assume it is
            # all strings.
            if len(rset['rows']) > 0:
                types = [ type(val) for val in rset['rows'][0] ]
            else:
                types = [ str ] * len(rset['info']['names'])

            # Create the result set and add the rows from the packet.
            rs = ResultSet(names=rset['info']['names'], types=types)
            for row in rset['rows']:
                rs.append_row(row)
            result.append_result(rs)
    return result

def _CommandExecuteAndEncode(command):
    """Command result formatter to transform a CommandResult to a format
    for XML-RPC transmission.

    This is actually a function, but it is named as a class since it
    behaves as a class with a __call__ function defined.

    """

    fqn = command.group_name + "." + command.command_name

    def _wrapper(*args):
        # Try to execute the command and return an error if an
        # exception is thrown.
        try:
            result = command.execute(*args)
        except Exception as err:
            message = "Exception raised - '%s'\n%s" % (
                err,
                traceback.format_exc(),
            )
            return [FORMAT_VERSION, str(FABRIC_UUID), TTL, message, []]

        # Check that it really is a CommandResult and return an
        # error otherwise.
        if not isinstance(result, CommandResult):
            message = "Expected %s from command '%s' but got '%s'\n%s" % (
                type(CommandResult), fqn, result,
                traceback.format_exc(),
            )
            packet = [FORMAT_VERSION, str(FABRIC_UUID), TTL, message, []]
            return packet
        return _encode(result)

    return _wrapper


def create_rfc2617_nonce(hashfunc, private_key, secs=AUTH_EXPIRES):
    validity = (datetime.utcnow() + timedelta(0, secs)).strftime(AUTH_TIME_FMT)
    data = "{ts}:{random}:{key}".format(
        ts=validity, random=os.urandom(8), key=private_key)
    return b64encode("{ts};;{data}".format(
        ts=validity, data=hashfunc(data).hexdigest()))


def _digest_nonce(hashfunc, client_address):
    data = hashfunc("{key};{addr};{expires}".format(
            key=uuid4(),
            addr=client_address[0],
            expires=str(AUTH_EXPIRES))
        ).hexdigest()
    return b64encode(data + ';' + str(AUTH_EXPIRES))


def _digest_denonce(nonce):
    data, expires = b64decode(nonce).split(';')
    return data, int(expires)


def _auth_id(client_address, username, realm):
    return sha1(':'.join([
        client_address[0],
        username,
        realm
        ])).hexdigest()


def _parse_digest_header(header):
    (_, attrstr) = header.split(' ', 1)
    matches = _RE_DIGEST_AUTH_HEADER.split(attrstr)

    allowed = ('username', 'realm', 'nonce', 'response',
               'nc', 'cnonce', 'qop', 'algorithm', 'uri',
               'opaque')

    attrs = {}
    if matches:
        for match in matches:
            key, value = [ part.strip() for part in match.split('=', 1)]
            if key not in allowed:
                _LOGGER.error("Illegal key in digest auth: {0}".format(key))
                return {}
            if value[0] in ('"', "'"):
                value = value.replace(value[0], '')
            attrs[key] = value

    return attrs


class MyRequestHandler(SimpleXMLRPCRequestHandler):

    def __init__(self, request, client_address, server):
        self._client_address = client_address
        self._authenticated = False
        self._server = server
        self._user = None
        SimpleXMLRPCRequestHandler.__init__(self, request,
                                            client_address, server)

    def send_authentication(self, nonce):
        self.send_response(401)
        nonce = create_rfc2617_nonce(self._server.hashfunc,
                                     self._server.private_key)
        self.send_header('WWW-Authenticate', "Digest "
            'realm="{realm}", '
            'nonce="{nonce}", '
            'algorithm="{algorithm}", '
            'opaque="{opaque}", '
            'qop="auth"'
            '\r\n'.format(nonce=nonce, algorithm=self._server.digest_algorithm,
                          opaque=self._server.opaque,
                          realm=self._server.realm))
        self.end_headers()

    def do_POST(self):
        if self._server.auth_disabled:
            return SimpleXMLRPCRequestHandler.do_POST(self)

        hashfunc = self._server.hashfunc
        nonce = None

        if self.headers.has_key('authorization'):

            attrs = _parse_digest_header(self.headers['authorization'])
            if not attrs:
                _LOGGER.error(
                    "Illegal digest authentication from {host}".format(
                        host=self._client_address))
                self.send_response(400)
                return
            nonce = attrs['nonce']

            store = _persistence.current_persister()
            user = credentials.User.fetch_user(attrs['username'],
                                               protocol='xmlrpc')
            if not user:
                _LOGGER.error("Authentication failed for {user}@{host}".format(
                    user=attrs['username'], host=self._client_address[0]))
                self.send_response(400)
                return

            ha1 = user.password_hash
            ha2 = hashfunc('POST' + ':' + self.rpc_paths[1]).hexdigest()
            if 'qop' in attrs and attrs['qop'] in ('auth', 'auth-int'):
                data = "{nonce}:{nc}:{cnonce}:{qop}:{ha2}".format(
                    nonce=attrs['nonce'],
                    nc=attrs['nc'],
                    cnonce=attrs['cnonce'],
                    qop=attrs['qop'],
                    ha2=ha2)
                reqdigest = hashfunc(ha1 + ':' + data).hexdigest()
            else:
                kd = hashfunc(ha1 + ':' + nonce + ':' + ha2).hexdigest()

            if reqdigest == attrs['response']:
                self._authenticated = self._server.authorize_client(
                    nonce,
                    self._client_address,
                    attrs['username'], attrs['realm'],
                    nc=int(attrs['nc'], 16))
                self._user = user
            else:
                self.send_response(400)
                return

        if not self._authenticated:
            nonce = _digest_nonce(self._server.hashfunc,
                                  self._client_address)
            self.send_authentication(nonce)
            return

        return SimpleXMLRPCRequestHandler.do_POST(self)

    def decode_request_content(self, data):
        encoding = self.headers.get("content-encoding", "identity").lower()
        if encoding == "identity":
            match = _RE_METHODNAME.search(data)
            if match:
                try:
                    methodname = match.groups()[0]
                    if (methodname == '_some_nonexisting_method' or
                            self._server.auth_disabled):
                        return data
                    (component, function) = methodname.split('.', 2)
                    if not self._user.has_permission('core', component, function):
                        self.send_response(403,
                            "Not allowed to execute '{0}.{1}.{2}'".format(
                                'core', component, function))
                    else:
                        return data
                except Exception as exc:
                    _LOGGER.debug(traceback.format_exc())
            else:
                self.send_response(400)
        else:
            self.send_response(501, "encoding %r not supported" % encoding)
        self.send_header("Content-length", "0")
        self.end_headers()


class MyServer(threading.Thread, ThreadingMixIn, SimpleXMLRPCServer):
    """Multi-threaded XML-RPC server whose threads are created during startup.

    .. note::
       Threads cannot be dynamically created so that the Multi-threaded XML-RPC
       cannot easily adapt to changes in the load.

    :param host: Address used by the XML-RPC Server.
    :param port: Port used by the XML-RPC Server.
    :param number_threads: Number of threads that will be started.
    """
    def __init__(self, host, port, number_threads, ssl_config):
        """Create a MyServer object.
        """
        SimpleXMLRPCServer.__init__(self, (host, port),
                                    requestHandler=MyRequestHandler,
                                    logRequests=False)
        threading.Thread.__init__(self, name="XML-RPC-Server")
        self.register_introspection_functions()
        self.__number_threads = number_threads
        self.__is_running = True
        self.daemon = True
        self.__opaque = uuid4().hex
        self.__digest_algorithm = 'MD5'
        self.__auth_disabled = False
        self.__realm = credentials.FABRIC_REALM_XMLRPC
        self.__private_key = uuid4().hex
        self.__nonces = {}
        self.__auth_clients_lock = threading.Lock()
        self.__auth_clients = {}
        self.__auth_clients_purge_timer = None
        self.__lock = threading.Condition()
        self.__configured = False
        self.__requests = Queue.Queue()

        if ssl_config:
            try:
                self.socket = ssl.wrap_socket(
                    self.socket,
                    keyfile=ssl_config['ssl_key'],
                    certfile=ssl_config['ssl_cert'],
                    ca_certs=ssl_config['ssl_ca'],
                    cert_reqs=ssl.CERT_NONE,
                    ssl_version=ssl.PROTOCOL_SSLv23)
            except NameError:
                raise _errors.Error(
                    "Python installation has no SSL support")
            except (ssl.SSLError, IOError) as err:
                raise _errors.Error('{errno} {strerr}'.format(
                    errno=err.errno, strerr=err.strerror))

    def get_number_sessions(self):
        """Return the number of concurrent sessions.
        """
        return self.__number_threads

    def _configure(self, config):
        """Configure the server using the loaded configuration

        :param config: Instance of config.Config
        """
        try:
            value = config.get('protocol.xmlrpc', 'disable_authentication')
            if value.lower() == 'yes':
                self.__auth_disabled = True
                _LOGGER.warning(
                    "Authentication disabled for XML RPC protocol."
                )
        except:
            self.__auth_disabled = False

        try:
            self.__realm = config.get('protocol.xmlrpc', 'realm')
        except ConfigParser.NoOptionError:
            self.__realm = credentials.FABRIC_REALM_XMLRPC

        self.__configured = True

    def register_command(self, command):
        """Register a command with the server.

        This will register the command under the name
        *group_name*.*command_name*.

        The command's execute method is expected to return a
        CommandResult, so we need to turn it into something to
        transport over the wire.

        :param command: Command to be registered.
        :type command: Command

        """

        if not self.__configured:
            self._configure(command.config)

        # This is the original format that we transported over the
        # wire, so it cannot be changed without breaking backward
        # compatibility.

        formatter = _CommandExecuteAndEncode(command)
        fqn = command.group_name + "." + command.command_name
        self.register_function(formatter, fqn)

    @property
    def hashfunc(self):
        return md5 if self.__digest_algorithm == 'MD5' else sha1

    @property
    def digest_algorithm(self):
        return self.__digest_algorithm

    @property
    def private_key(self):
        return self.__private_key

    @property
    def opaque(self):
        return self.__opaque

    @property
    def realm(self):
        return self.__realm

    @property
    def auth_disabled(self):
        if self.__auth_disabled:
            return True
        return False

    def shutdown(self):
        """Shutdown the server.
        """
        thread = SessionThread.get_reference()
        assert(thread is not None)
        if self.__auth_clients_purge_timer:
            self.__auth_clients_purge_timer.cancel()
        thread.shutdown()

    def shutdown_now(self):
        """Shutdown the server immediately without waiting until any
        ongoing activity finishes.
        """
        # Avoid possible errors if different threads try to stop the
        # server.
        with self.__lock:
            if not self.__is_running:
                return
            if self.__auth_clients_purge_timer:
                self.__auth_clients_purge_timer.cancel()
            self.server_close()
            self.__is_running = False
            self.__lock.notify_all()

    def wait(self):
        """Wait until the server shuts down.
        """
        with self.__lock:
            while self.__is_running:
                self.__lock.wait(10)

    def run(self):
        """Main routine which handles one request at a time.
        """
        _LOGGER.info(
            "XML-RPC protocol server %s started.", self.server_address
        )

        if not self.__auth_clients_purge_timer:
            self.__auth_clients_purge_timer = threading.Timer(
                PURGE_CLIENTS_TIMER, self.__purge_expired_clients)
            self.__auth_clients_purge_timer.name = "Authentication-Timer"
            self.__auth_clients_purge_timer.start()

        self._create_sessions()
        while self.__is_running:
            try:
                client_address = None
                request, client_address = self.get_request()
                self.enqueue_request(request, client_address)
            except Exception as error:
                _LOGGER.debug(
                    "Error accessing request from %s.", client_address
                )

    def dequeue_request(self):
        """Retrieve a request from the request queue.

        :return: Tuple with request to be processed and client's address.
        """
        (request, address) = self.__requests.get()
        return (request, address)

    def enqueue_request(self, request, client_address):
        """Put the request in the request queue that eventually will be
        accessed by session threads.

        :param request: Request to be processed.
        :param client_address: Client's address.
        """
        if self.verify_request(request, client_address):
            self.__requests.put((request, client_address))

    def _create_sessions(self):
        """Create session threads.
        """
        _LOGGER.info("Setting %s XML-RPC session(s).", self.__number_threads)

        for thread_number in range(0, self.__number_threads):
            thread = SessionThread("XML-RPC-Session-%s" % (thread_number, ),
                                   self)
            try:
                thread.start()
            except Exception as error:
                _LOGGER.error("Error starting thread: (%s).", error)

    def __add_client(self, nonce, client_address, username, realm):
        """Adds new client to collection of _authenticated clients

        :param nonce: Nonce (see RFC2617).
        :param client_address: Tuple holding host address and TCP port.
        :param username: Username name used for authentication.
        :param realm: Realm for which we authenticate.

        :return: Dictionary containing client authentication information
        """
        with self.__auth_clients_lock:
            self.__auth_clients[nonce] = {
                'username': username,
                'realm': realm,
                'client_address': client_address,
                'expires': datetime.utcnow() + timedelta(0, AUTH_EXPIRES),
                'next_nonce_counter': 1,
            }
            client = self.__auth_clients[nonce]

        return client

    def __purge_expired_clients(self):
        """Purge expired authorized clients

        This method needs to run periodically and is started using
        threading.Timer(). The period is set using the module variable
        PURGE_CLIENTS_TIMER.
        """
        to_purge = []
        with self.__auth_clients_lock:
            self.__auth_clients_purge_timer = threading.Timer(
                PURGE_CLIENTS_TIMER, self.__purge_expired_clients)
            self.__auth_clients_purge_timer.start()
            now = datetime.utcnow()

            for nonce, info in self.__auth_clients.iteritems():
                if info['expires'] >= now:
                    to_purge.append(nonce)

            for nonce in to_purge:
                del self.__auth_clients[nonce]

        if to_purge:
            _LOGGER.debug("purged {0} expired clients".format(len(to_purge)))

    def authorize_client(self, nonce, client_address, username, realm, nc):
        """Authorize a client

        :param nonce: Nonce (see RFC2617).
        :param client_address: Tuple holding host address and TCP port.
        :param username: Username name used for authentication.
        :param realm: Realm for which we authenticate.
        :param nc: Client Nonce (see RFC2617).

        :return: Returns True when authorized, False otherwise.
        """
        try:
            client = self.__auth_clients[nonce]
        except KeyError:
            client = self.__add_client(nonce, client_address,
                                       username, realm)

        if client['next_nonce_counter'] != nc:
            _LOGGER.debug("authorizing client failed: {0} != {1}".format(
                client['nonce_counter'], nc))
            return False

        with self.__auth_clients_lock:
            client['next_nonce_counter'] += 1
            client['expires'] = client['expires'] + timedelta(0, AUTH_EXPIRES)

        return True

    def finish_request(self, request, client_address):
        """Instantiate handler and finish request"""
        self.RequestHandlerClass(request, client_address, self)

class SessionThread(threading.Thread):
    """Session thread which is responsible for handling incoming requests.

    :param name: Thread's name.
    :param server: Reference to server object which knows how to handle
                   requests.
    """
    local_thread = threading.local()

    def __init__(self, name, server):
        """Create a SessionThread object.
        """
        threading.Thread.__init__(self, name=name)
        self.__server = server
        self.__is_shutdown = False
        self.daemon = True

    @staticmethod
    def get_reference():
        """Get a reference to a SessionThread object associated
        to the current thread or None.

        :return: Reference to a SessionThread object associated
                 to the current thread or None.
        """
        try:
            return SessionThread.local_thread.thread
        except AttributeError:
            pass
        return None

    def run(self):
        """Process registered requests.
        """
        _LOGGER.info("Started XML-RPC-Session.")
        try:
            _persistence.init_thread()
        except Exception as error:
            _LOGGER.warning("Error connecting to backing store: (%s).", error)

        SessionThread.local_thread.thread = self

        while True:
            request, client_address = self.__server.dequeue_request()
            _LOGGER.debug(
               "Processing request (%s) from (%s) through thread (%s).",
               request, client_address, self
            )
            # There is no need to catch exceptions here because the method
            # process_request_thread already does so. It is the main entry
            # point in the code which means that any uncaught exception
            # in the code will be reported as xmlrpclib.Fault.
            self.__server.process_request_thread(request, client_address)
            _LOGGER.debug(
                "Finishing request (%s) from (%s) through thread (%s).",
                request, client_address, self
            )
            if self.__is_shutdown:
                self.__server.shutdown_now()

        try:
            _persistence.deinit_thread()
        except Exception as error:
            _LOGGER.warning("Error connecting to backing store: (%s).",
                            error)

    def shutdown(self):
        """Register that this thread is responsible for shutting down
        the server.
        """
        self.__is_shutdown = True

if hasattr(httplib, 'HTTPS'):
    class FabricHTTPSHandler(urllib2.HTTPSHandler):
        def __init__(self, ssl_config):
            urllib2.HTTPSHandler.__init__(self)
            self._ssl_config = ssl_config

        def https_open(self, req):
            return self.do_open(self.get_https_connection, req)

        def get_https_connection(self, host, timeout=300):
            return httplib.HTTPSConnection(
                host,
                key_file=self._ssl_config['ssl_key'],
                cert_file=self._ssl_config['ssl_cert']
            )

class FabricTransport(xmlrpclib.Transport):

    """XMLRPC transport for Fabric supporting Digest Authentication"""

    user_agent = 'mysqlfabric/{0}'.format(FABRIC_VERSION)

    def __init__(self, username, password, verbose=0, realm=None,
                 use_datetime=False, https_handler=None):
        self.__username = username
        self.__password = password
        self._use_datetime = use_datetime
        self.verbose = verbose
        self.__handlers = []
        self.__realm = realm

        self.__passmgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        self.__digest_auth_handler = urllib2.HTTPDigestAuthHandler(
            self.__passmgr)

        if https_handler:
            self.__handlers.append(https_handler)
            self.__scheme = 'https'
        else:
            self.__scheme = 'http'
        self.__handlers.append(self.__digest_auth_handler)

        self._authorization = None

    def request(self, host, handler, request_body, verbose):
        auth_handler = self.__digest_auth_handler
        uri = '{scheme}://{host}{handler}'.format(scheme=self.__scheme,
                                                  host=host, handler=handler)
        if self.verbose:
            _LOGGER.info("FabricTransport: {0}".format(uri))

        self.__passmgr.add_password(None,
                                    uri, self.__username, self.__password)

        headers = {
            'Content-Type': 'text/xml',
            'User-Agent': self.user_agent,
        }

        opener = urllib2.build_opener(*self.__handlers)
        req = urllib2.Request(uri, request_body, headers=headers)

        result = opener.open(req)
        if result.code == 200:
            try:
                self._authorization = req.unredirected_hdrs['Authorization']
            except KeyError:
                self._authorization = None

        return self.parse_response(result)


class MyClient(xmlrpclib.ServerProxy):
    """Simple XML-RPC Client.

    This class defines the client-side interface of the command subsystem.
    The connection to the XML-RPC Server is made when the dispatch method
    is called. This is done because the information on the server is passed
    to the command object after its creation.
    """
    def __init__(self):
        """Create a MyClient object.
        """
        pass

    def dispatch(self, command, *args):
        """Default dispatch method.

        This is the default dispatch method that will just dispatch
        the command with arguments to the server.
        """
        reference = command.group_name + '.' + command.command_name

        address = command.config.get('protocol.xmlrpc', 'address')
        username = command.config.get('protocol.xmlrpc', 'user')
        password = command.config.get('protocol.xmlrpc', 'password')

        ssl_config = {}
        try:
            for option in ('ssl_ca', 'ssl_key', 'ssl_cert'):
                ssl_config[option] = command.config.get('protocol.xmlrpc',
                                                        option)
            if hasattr(httplib, 'HTTPS'):
                https_handler = FabricHTTPSHandler(ssl_config)
                scheme = 'https'
            else:
                _LOGGER.warning("Sorry but support to SSL is not available.")
                raise ConfigParser.NoOptionError
        except ConfigParser.NoOptionError:
            ssl_config = {}
            https_handler = None
            scheme = 'http'

        host, port = address.split(":")
        if not host:
            host = "localhost"

        uri = "{scheme}://{host}:{port}".format(scheme=scheme, host=host,
                                                port=port)

        transport = FabricTransport(
            username, password, verbose=0,
            https_handler=https_handler
        )
        xmlrpclib.ServerProxy.__init__(self, uri, allow_none=True,
                                       transport=transport)

        packet = getattr(self, reference)(*args)
        return _decode(packet)
