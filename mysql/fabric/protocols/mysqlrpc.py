# -*- coding: utf-8 -*-

#
# Copyright (c) 2014 Oracle and/or its affiliates. All rights reserved.
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

"""Server implementing the MySQL client protocol
"""

from binascii import unhexlify
from datetime import datetime, time, timedelta, date
from decimal import Decimal
from hashlib import sha1
import json
import logging
import os
import re
import socket
import SocketServer
import struct
from textwrap import TextWrapper
import threading
from time import struct_time

from mysql.fabric import __version__ as fabric_version
from mysql.fabric import credentials, persistence
from mysql.fabric.command import (
    ResultSet, get_command, get_commands, get_groups
)
from mysql.fabric.utils import dequote

import mysql.connector
from mysql.connector import utils, errorcode
from mysql.connector.constants import (
    ClientFlag, ServerCmd, FieldType, ServerFlag, FieldFlag
)
from mysql.connector.errors import InterfaceError

NEXT_CNX_ID = 0
NEXT_CNX_ID_LOCK = threading.Lock()
CLIENT_SESSION_TIMEOUT = 300  # in seconds

_LOGGER = logging.getLogger(__name__)

# Regular expression getting group, command and the arguments as a string
CHECK_CALL = re.compile(
    r"""^CALL\s*""", re.IGNORECASE
)
PARSE_CALL = re.compile(
    r"""CALL\s+(\w+)\.(\w+)\s*\((.*)\)"""
    r"""(?=(?:[^"']*["'][^"']*["'])*[^"']*$)""", re.IGNORECASE
)
PARSE_CALL_ARGS = re.compile(
    r""",(?=(?:[^"']*["'][^"']*["'])*[^"']*$)"""
)
PARSE_CALL_KWARG = re.compile(
    r"""=(?=(?:[^"']*["'][^"']*["'])*[^"']*$)"""
)

# Regular expression for SHOW CREATE PROCEDURE
CHECK_SHOW_CREATE_PROC = re.compile(
    r"""^SHOW\s+CREATE\s+PROCEDURE\s*""", re.IGNORECASE
)
PARSE_SHOW_CREATE_PROC = re.compile(
    r"""SHOW\s+CREATE\s+PROCEDURE\s+(\w+).(\w+)$""", re.IGNORECASE
)

# Regular expression for INFORMATION_SCHEMA.ROUTINES
PARSE_IS_ROUTINES = re.compile(
    r"""SELECT\s+(.*)\s+FROM\s+INFORMATION_SCHEMA.ROUTINES\s*(.*)""",
    re.IGNORECASE
)

# Regular expression getting SET statements
PARSE_SET = re.compile(
    r"""SET\s+(\w+)\s*=(.*)""", re.IGNORECASE
)

def next_connection_id():
    """Keep track of connection ID similar to MySQL server thread IDs

    :return: Next connection ID.
    :rtype: Integer.
    """
    global NEXT_CNX_ID
    with NEXT_CNX_ID_LOCK:
        NEXT_CNX_ID += 1
        return NEXT_CNX_ID


def lenc_int(int_):
    """Store an integer in a length encoded string.

    :param int_: Integer to encode.
    :return: Length encoded integer.
    :rtype: String.
    """
    if int_ < 251:
        return struct.pack('B', int_)
    elif int_ >= 2**24:
        return struct.pack('<BQ', 254, int_)
    elif int_ >= 2**16:
        packed = packed = struct.pack('<I', int_)
        return struct.pack('<B3s', 253, packed[0:3])
    else:
        return struct.pack('<BH', 252, int_)


class MySQLServerProtocolMixin(object):

    """Class implementing (parts of) the MySQL server protocol"""

    @staticmethod
    def packet_header(nr, length):
        """Create header for a MySQL packet

        :param nr: The number of the packet.
        :param length: Length of the packet.
        :return: The header as string.
        :rtype: string.
        """
        packed_length = struct.pack('<I', length)
        return struct.pack('<3sB', packed_length[0:3], nr)

    @staticmethod
    def handshake(auth_data, connection_id):
        """Create a MySQL handshake packet

        :param auth_data: Authentication data (also known as scramble).
        :return: Packet containing the handshake.
        :rtype: str
        """
        version = '{0}-fabric'.format(fabric_version)
        capabilities = [
            ClientFlag.LONG_PASSWD,
            ClientFlag.LONG_FLAG,
            ClientFlag.PROTOCOL_41,
            ClientFlag.SECURE_CONNECTION,
            ClientFlag.MULTI_RESULTS,
            ClientFlag.INTERACTIVE,
        ]
        flags = 0
        for capability in capabilities:
            flags |= capability

        capability_str = struct.pack("<I", flags)
        auth_plugin = 'mysql_native_password'
        handshake = struct.pack(
            '<B{verlen}sxI8sx2sBH2sx10x12sx'.format(
                verlen=len(version)),
            10,                                     # B
            version,                                # {verlen}s
                                                    # x
            connection_id,             # I
            str(auth_data[0:8]),                    # 8s
                                                    # x
            capability_str[0:2],                    # 2s
            33,                                     # B
            0,                                      # H
            capability_str[2:4],                    # 2s
            str(auth_data[8:20]),
        )
        return handshake

    @staticmethod
    def ok_packet(rows=0, last_insert_id=0, status_flags=0,
                  warnings=0, info=''):
        """Create a MySQL OK-packet

        :param rows: Number of rows incase of result.
        :param last_insert_id: Last Insert ID (see manual).
        :param status_flags: Server status flags.
        :param warnings: Number of warnings generated by last statement.
        :param info: Information to be send to client.
        :return: The OK packet as string.
        :rtype: str
        """
        return (
            '\x00' + lenc_int(rows) + lenc_int(last_insert_id) +
            struct.pack('<HH', status_flags, warnings) +
            info
        )

    @staticmethod
    def error_packet(code, message, sqlstate=None):
        """Create a MySQL Error-packet

        :param code: The Error Code (errno) as integer.
        :param message: Error message as string.
        :param sqlstate: SQLState as string (defaults to 'HY000')
        :return: The error packet as string.
        :rtype: str
        """
        sqlstate = sqlstate or 'HY000'
        return (
            '\xff' + struct.pack('<H', code) + '#' + sqlstate.encode() +
            '[Fabric] ' + message.encode()
        )

    @staticmethod
    def eof_packet(warning_count=0, status_flags=0):
        """Create a MySQL EOF-packet

        :param warning_count: Number of warnings generated by last statement.
        :param status_flags: Server status flags.
        :return: The EOF packet as string.
        :rtype: str
        """
        return '\xfe' + struct.pack('<HH', warning_count, status_flags)

    @staticmethod
    def column_count_packet(count):
        """Create a MySQL packet containing number of columns

        :param count: Number of columns
        :return: The packet with number of columns as length encoded integer.
        :rtype: str
        """
        return lenc_int(count)

    @staticmethod
    def column_packet(name, table, type_, catalog='fabric'):
        """Create a MySQL column-packet

        :param type_: Python type of the column
        :param name: Name of the column.
        :param table: Name of the table.
        :param catalog: Name of the schema or database (default 'fabric')
        :return: The column-packet as string.
        :rtype: str
        """
        len_column = lenc_int(len(name))
        len_table = lenc_int(len(table))
        len_catalog = lenc_int(len(catalog))

        if type_ in (str, unicode, basestring):
            field_type = FieldType.VAR_STRING
        elif type_ in (int, long):
            field_type = FieldType.LONGLONG
        elif type_ in (Decimal, float):
            field_type = FieldType.NEWDECIMAL
        elif type_ in (buffer, bytearray):  # bytes is str in Python 2
            field_type = FieldType.BLOB
        elif type_ in (datetime,):
            field_type = FieldType.DATETIME
        elif type_ in (date,):
            field_type = FieldType.DATE
        elif type_ in (time, struct_time, timedelta):
            field_type = FieldType.TIME
        elif type_ in (bool,):
            field_type = FieldType.TINY
        else:
            raise TypeError("Can not handle {0}".format(type_))


        return (
            len_catalog + catalog +  # catalog
            len_catalog + catalog +  # db
            len_table + table +  # table
            len_table + table +  # org_table
            len_column + name +  # name
            len_column + name +  # org_column
            '\x0c' +
            struct.pack('<HIBHBxx', 33, 1024, field_type,
                        FieldFlag.NOT_NULL, 0)
        )

    @staticmethod
    def row_packet(*values):
        """Create a MySQL row-packet

        :param values: Multiple arguments denoting the row values.
        :return: The row-packet as bytearray
        :rtype: bytearray
        """
        row = bytearray()
        for value in values:
            if value is None or value == u'None':
                row += bytearray(b'\xfb')
                continue

            if isinstance(value, bool):
                if value:
                    value = 1
                else:
                    value = 0

            try:
                value = value.decode('utf-8')
            except AttributeError:
                # Get the string representation of anything else
                value = str(value)
            row += bytearray(lenc_int(len(value)))
            row += bytearray(value, encoding='utf-8')

        return row

    @staticmethod
    def wrap_packet(nr, packet):
        """Add packet MySQL header to the MySQL packet

        :param nr: Number of the packet.
        :param packet: The packet which needs a header.
        :return: The packet with header.
        :rtype: str or bytearray
        """
        return MySQLServerProtocolMixin.packet_header(nr, len(packet)) + packet

    @staticmethod
    def parse_handshake_response(packet):
        """Parse the handshake response packet and returns the information.

        :param packet: Packet received as received from socket without header.
        :return: Dictionary containing the handshake information; empty if
                 there was an error.
        :rtype: dict
        """
        try:
            (capabilities, max_allowed_packet, charset) = struct.unpack_from(
                "<IIB23x", buffer(packet[0:32]))
            buf = packet[32:]
            (buf, username) = utils.read_string(buf, end='\x00')
            (buf, auth_data) = utils.read_lc_string(buf)
            if auth_data and auth_data[-1] == '\x00':
                    auth_data = auth_data[:-1]
            if capabilities & ClientFlag.CONNECT_WITH_DB and buf:
                (buf, database) = utils.read_string(buf, end='\x00')
        except Exception as exc:
            _LOGGER.debug("Failed parsing handshake: %s", str(exc))
            return {}

        return {
            'capabilities': capabilities,
            'max_allowed_packet': max_allowed_packet,
            'charset': charset,
            'username': str(username),
            'auth_data': auth_data,
        }

    @staticmethod
    def get_random_string(size):
        """Get a random string

        :param size: Size of the string to be returned.
        :return: String containing random bytes.
        :rtype: string
        """
        return ''.join([ "%02x" % ord(c) for c in os.urandom(size)])

    @staticmethod
    def get_random_bytes(size):
        """Get random bytes

        :param size: The number of random bytes to be returned.
        :return: A bytearray containing random bytes.
        :rtype : bytearray
        """
        return bytearray(os.urandom(size))


class WrapSocket(object):

    """Wrapper around a socket object

    This wrapper around socket is needed to add an activity member
    which hold the time last something happened. It need to be updated
    using the update_activity() method.
    """

    def __init__(self, sock):
        """Initialize"""
        self.__activity = datetime.utcnow()
        self.__connection_id = None
        self.__socket = sock

    def __getattr__(self, item):
        """Get attribute of wrapped object if not available in self
        :param item: Attribute name
        :return: Attribute
        """
        if item in self.__dict__:
            return getattr(self, item)

        return getattr(self.__socket, item)

    @property
    def activity(self):
        """Returns the last activity on the socket
        :rtype : datetime.datetime
        """
        return self.__activity

    @property
    def connection_id(self):
        """Returns the connection ID

        :return: Connection ID
        :rtype: int
        """
        return self.__connection_id

    @connection_id.setter
    def connection_id(self, value):
        """Set the connection ID

        :param value: The connection ID as integer
        """
        self.__connection_id = int(value)

    def update_activity(self):
        """Update the activity of the socket"""
        self.__activity = datetime.utcnow()


class MySQLRPCRequestHandler(SocketServer.BaseRequestHandler,
                             MySQLServerProtocolMixin):

    """Class implementing the request handler using MySQL Protocol"""

    def __init__(self, request, client_address, server):
        """Initialize"""
        self._handshaked = False
        self._authenticated = False
        self._curr_pktnr = 1
        self._curr_user = None
        self._format = None
        super(MySQLRPCRequestHandler, self).__init__(request,
                                                     client_address, server)

    def setup(self):
        """Setup the MySQLRPC request handler"""
        self._handshaked = False
        self._authenticated = False
        self._curr_pktnr = 1
        persistence.init_thread()
        self._store = persistence.current_persister()

    def read_packet(self):
        """Read a MySQL packet from the socket

        This method reads a MySQL packet form the socket, parses it and
        returns the type and the payload. The type is the value of the
        first byte of the payload.

        :return: Tuple with type and payload of packet.
        :rtype: tuple
        """
        header = bytearray(b'')
        header_len = 0
        while header_len < 4:
            chunk = self.request.recv(4 - header_len)
            if not chunk:
                raise InterfaceError(errno=2013)
            header += chunk
            header_len = len(header)

        length = struct.unpack_from('<I', buffer(header[0:3] + b'\x00'))[0]
        self._curr_pktnr = header[-1]
        data = bytearray(self.request.recv(length))
        return data[0], data[1:]

    def read_handshake_response(self):
        """Read the handshake response from socket

        :return: Dictionary containing the handshake information; empty if
                 there was an error.
        :rtype: dict
        """
        header = self.request.recv(4)
        if not header:
            return
        length = struct.unpack('<I', header[0:3] + '\x00')[0]
        self._curr_pktnr = struct.unpack('B', header[-1])[0]
        payload = bytearray(self.request.recv(length))
        handshake = self.parse_handshake_response(payload)
        if not handshake:
            return {}
        else:
            return handshake

    def send_packet(self, packet):
        """Send the MySQL packet to the client

        :param packet: MySQL packet without header.
        :return:
        """
        self._curr_pktnr += 1
        try:
            self.request.sendall(self.wrap_packet(self._curr_pktnr, packet))
        except socket.error as exc:
            _LOGGER.info("Failed sending to client: %s", str(exc))

    def send_handshake(self, auth_data):
        self._curr_pktnr = 0
        packet = self.handshake(auth_data, self.request.connection_id)
        self.request.sendall(self.wrap_packet(0, packet))

    def send_error(self, error_code, message, sql_state):
        self.send_packet(self.error_packet(error_code, message, sql_state))

    def send_error_unknown_command(self):
        self.send_packet(self.error_packet(errorcode.ER_UNKNOWN_PROCEDURE,
                                           "Unknown Fabric command"))

    def send_syntax_error(self):
        self.send_error(errorcode.ER_SYNTAX_ERROR,
                        "You have an error in your SQL syntax",
                        "42000")

    def send_ok(self, *args, **kwargs):
        self.send_packet(self.ok_packet(*args, **kwargs))

    def authenticate(self, handshake, original_scramble):
        """Authenticate the user using the handshake and original sent scramble.

        If the OpenSSL is not installed, the function will use built-in
        functions from hashlib.

        :returns: MySQL OK or Error packet
        :rtype: str
        """
        user = None
        self._curr_user = None
        username = handshake['username']
        auth_data = buffer(handshake['auth_data'])

        try:
            user_info = credentials.get_user(username, 'mysql')
            password = user_info.password
            hash3 = sha1(buffer(original_scramble)
                         + unhexlify(password)).digest()
            xored = [ord(h1) ^ ord(h3) for (h1, h3) in zip(auth_data, hash3)]
            check = sha1(struct.pack('20B', *xored)).hexdigest()
            if password == check.upper():
                user = credentials.User.fetch_user(username, 'mysql')
        except mysql.connector.Error as exc:
            return self.error_packet(exc.errno, exc.msg, exc.sqlstate)
        except AttributeError:
            # We did not find the user or could not verify password
            user = None

        if not user:
            _LOGGER.info("User '%s' denied access", username)
            return self.error_packet(
                errorcode.ER_DBACCESS_DENIED_ERROR,
                "Access denied for user '{user}'".format(user=username),
                "42000"
            )
        else:
            self._curr_user = user
            _LOGGER.info("User '%s' authenticated", username)
            return self.ok_packet()

    def _prepare_results(self, res):

        result_sets = []

        if isinstance(res[0], bool) and isinstance(res[1], str):
            columns = ['Successful', 'Error']
            data = res[0:2]
            result_sets.append((columns, data))

            try:
                res = res[2]
            except IndexError:
                pass

        if isinstance(res, (list, tuple)):

            columns = []
            values = []
            if isinstance(res[0], dict):
                columns = res[0].keys()
                for result_dict in res:
                    values.append(result_dict.values())
            elif isinstance(res[0], (tuple, list)):
                for i in xrange(0, len(res[0])):
                    columns.append('col{0}'.format(i + 1))
                values = res

            if columns and values:
                result_sets.append([columns, values])

        return result_sets

    def send_resultset(self, resultset, table_name='', server_flags=0):
        self.send_packet(self.column_count_packet(len(resultset.columns)))

        for column in resultset.columns:
            self.send_packet(self.column_packet(name=column.name,
                                                type_=column.type,
                                                table=table_name))

        self.send_packet(self.eof_packet())

        for row in resultset:
            self.send_packet(self.row_packet(*row))

        self.send_packet(self.eof_packet(status_flags=server_flags))

    def get_cmd_info_result(self, cmd_result):
        """Create the information result set from a CommandResult

        :param cmd_result: Instance of CommandResult
        :return: A ResultSet instance
        :rtype: ResultSet
        """
        info_result = ResultSet(('fabric_uuid', 'ttl', 'message'),
                                (str, int, str))
        info_result.append_row((cmd_result.uuid, cmd_result.ttl,
                               cmd_result.error))

        return info_result

    def send_cmd_result(self, cmd_result, group, command):
        """Send the command result to the client as text result.

        The cmd_result is supposed to be a list of tuples. If it is just
        a list, it will be converted.

        The columns are enumerated as 'col1', 'col2', .. The table name in
        the result will be '<group>_<command>'.

        :param cmd_result: Instance of CommandResult
        :param group: Group of the command
        :param command: Command name
        """
        info_result = self.get_cmd_info_result(cmd_result)

        results = [info_result]
        results.extend(cmd_result.results)

        i = 0
        len_results = len(results)
        for result in results:
            i += 1
            if i < len_results:
                server_flags = ServerFlag.MORE_RESULTS_EXISTS
            else:
                # Last result set, don't set ServerFlag MORE_RESULTS_EXISTS
                server_flags = 0

            self.send_resultset(result, '{0}_{1}'.format(group, command),
                                server_flags)

    def send_json(self, cmd_result, group, command):
        """Send the command result as JSON

        :param cmd_result: Isntance of CommandResult
        :param group: Group of the command
        :param command: Command name
        """
        info_result = self.get_cmd_info_result(cmd_result)

        results = [info_result]
        results.extend(cmd_result.results)

        self.send_packet(self.column_count_packet(1))
        self.send_packet(self.column_packet(
            'json',
            type_=buffer,
            table='{0}_{1}'.format(group, command)
        ))
        self.send_packet(self.eof_packet())

        all_results = []
        for result in results:
            dict_result = []
            column_names = [ col.name for col in result.columns]

            for row in result:
                # We need to convert u'None' to None
                row = [ None if v == u'None' else v for v in row]
                dict_result.append(dict(zip(column_names, row)))
            all_results.append(dict_result)

        result_json = json.dumps(all_results)
        self.send_packet(self.row_packet(result_json))
        self.send_packet(self.eof_packet())

    def _handle_call(self, call_match):
        """Handle a CALL statement

        :param call_match: Result of re.match()
        """
        group, command, args_str = call_match.groups()
        group_command = "{0}.{1}".format(group, command)

        if self._curr_user and \
                not self._curr_user.has_permission('core', group, command):
            self.send_error(
                errorcode.ER_PROCACCESS_DENIED_ERROR,
                "Execute command denied to user '{user}' for "
                "command '{cmd_name}'".format(
                    user=self._curr_user.username, cmd_name=group_command),
                "42000")
            return

        args = []
        kwargs = {}
        if args_str:
            split_args = PARSE_CALL_ARGS.split(args_str)
            for arg in split_args:
                arg = arg.strip()
                kwarg = PARSE_CALL_KWARG.split(arg)
                if len(kwarg) == 2:
                    # we have a keyword argument
                    kwargs[kwarg[0].strip()] = dequote(kwarg[1].strip())
                elif arg == 'NULL':
                    args.append(None)
                else:
                    args.append(dequote(arg.strip()))

        _LOGGER.debug("executing: %s.%s(%s, %s)",
                      group, command,
                      str(args), str(kwargs))

        cmd_exec = self.server.get_command_exec(group_command)

        if not cmd_exec:
            self.send_error_unknown_command()
            return

        try:
            result = cmd_exec(*args, **kwargs)
        except Exception as exc:
            self.send_error(
                errorcode.ER_WRONG_PARAMETERS_TO_PROCEDURE,
                "Failed executing command {cmd_name}: {error}".format(
                    error=str(exc), cmd_name=group_command),
                "22000")
        else:
            if self._format == 'json':
                self.send_json(result, group, command)
            else:
                self.send_cmd_result(group=group, command=command,
                                     cmd_result=result)

    def _handle_show_create_procedure(self, showproc_match):
        """Handle SHOW CREATE PROCEDURE

        We send the information from the command as text to the client in a
        one row/one column result.

        :param showproc_match: Result of re.match
        """
        group, command = showproc_match.groups()

        try:
            command_class = get_command(group, command)
        except KeyError:
            # group and/or command does not exists
            self.send_error(
                errorcode.ER_UNKNOWN_PROCEDURE,
                "Fabric command {0}.{1} does not exists".format(group,command),
                "22000"
            )
            return

        self.send_packet(self.column_count_packet(1))
        self.send_packet(self.column_packet(
            'fabric_help',
            type_=str,
            table='fabric_help'
        ))
        self.send_packet(self.eof_packet())

        command_text = command_class.get_signature()
        paragraphs = []
        if command_class.__doc__:
            wrapper = TextWrapper()
            paragraphs = []
            for para in command_class.__doc__.split("\n\n"):
                if para:
                    paragraphs.append(wrapper.fill(para.strip()))
        help_text = command_text + "\n\n" + "\n\n".join(paragraphs)
        _LOGGER.debug(help_text)
        self.send_packet(self.row_packet(help_text))
        self.send_packet(self.eof_packet())

    def _handle_information_schema_routines(self, is_routines_match):
        """Handle a select from INFORMATION_SCHEMA.ROUTINES

        We give a list of all groups and commands. There is no filtering
        going on.

        :param is_routines_match:
        :return:
        """

        groups = get_groups()

        self.send_packet(self.column_count_packet(5))
        self.send_packet(self.column_packet(
            'SPECIFIC_NAME',
            type_=str,
            table='routines',
            catalog='information_schema'
        ))
        self.send_packet(self.column_packet(
            'ROUTINE_CATALOG',
            type_=str,
            table='routines',
            catalog='information_schema'
        ))
        self.send_packet(self.column_packet(
            'ROUTINE_SCHEMA',
            type_=str,
            table='routines',
            catalog='information_schema'
        ))
        self.send_packet(self.column_packet(
            'ROUTINE_NAME',
            type_=str,
            table='routines',
            catalog='information_schema'
        ))
        self.send_packet(self.column_packet(
            'ROUTINE_TYPE',
            type_=str,
            table='routines',
            catalog='information_schema'
        ))
        self.send_packet(self.eof_packet())

        for group in groups:
            commands = get_commands(group)
            for command in commands:
                self.send_packet(self.row_packet(
                    '{0}.{1}'.format(group, command),  # specific_name
                    'fabric',  # catalog
                    group,  # routine_schema
                    command,  # routine_name
                    'FABRIC_COMMAND',  # routine_type
                    ))
        self.send_packet(self.eof_packet())

    def _do_hanshake(self):
        """Send initial handshake and handle response

        :return: Returns True if handshake succeeded, False otherwise.
        :rtype: bool
        """
        self._handshaked = False
        auth_data = self.get_random_bytes(20)
        self.send_handshake(auth_data)
        handshake = self.read_handshake_response()
        if not handshake:
            self.send_error(
                errorcode.ER_HANDSHAKE_ERROR,
                "Bad handshake",
                "42000"
            )
            return False

        if not handshake['capabilities'] & ClientFlag.MULTI_RESULTS:
            self.send_error(errorcode.ER_NO,
                            "Client needs CLIENT_MULTI_RESULTS set",
                            "42000")
            return False

        if not handshake['capabilities'] & ClientFlag.PROTOCOL_41:
            self.send_error(errorcode.ER_NO,
                            "Client needs CLIENT_PROTOCOL_41 set",
                            "42000")
            return False

        if not self.server.auth_disabled:
            response = self.authenticate(handshake, auth_data)
            if not response or response[0] != '\x00':
                self.send_packet(response)
                return False

        self._handshaked = True
        self._authenticated = True
        self.send_ok()

        return True

    def handle(self):
        """Handle incoming requests"""
        response = None
        if not self._handshaked:
            if not self._do_hanshake():
                # Bail out when handshake failed.
                return
        elif not self._authenticated:
            self.send_error(
                errorcode.CR_UNKNOWN_ERROR,
                "Not authenticated after successful handshake",
                "HY000")

        if not self._handshaked:
            # Bail out when handshake failed.
            return

        while True:
            data = None
            try:
                packet_type, data = self.read_packet()
            except Exception as exc:
                break

            self.request.update_activity()

            if packet_type == ServerCmd.PING:
                self.send_ok(info='MySQL Fabric')
                continue

            if not data:
                # Nothing to do, get next packet.
                continue

            data = data.strip().decode('utf8')

            # Handle CALL
            if CHECK_CALL.match(data):
                call_match = PARSE_CALL.match(data)
                if call_match:
                    self._handle_call(call_match)
                else:
                    self.send_syntax_error()
                continue

            # Handle SHOW CREATE PROCEDURE
            if CHECK_SHOW_CREATE_PROC.match(data):
                showproc_match = PARSE_SHOW_CREATE_PROC.match(data)
                if showproc_match:
                    self._handle_show_create_procedure(showproc_match)
                else:
                    self.send_syntax_error()
                continue

            # Handle INFORMATION_SCHEMA.ROUTINES
            is_routines_match = PARSE_IS_ROUTINES.match(data)
            if is_routines_match:
                self._handle_information_schema_routines(is_routines_match)
                continue

            # Handle SET
            set_match = PARSE_SET.match(data)
            if set_match and set_match.group(1).lower() in ('format',):
                format = dequote(set_match.group(2).strip()).strip().lower()
                if format not in ('json',):
                    self.send_error(
                        errorcode.ER_LOCAL_VARIABLE,
                        "Format '{0}' is not supported".format(format),
                        "42000"
                    )
                else:
                    _LOGGER.debug("Format set to %s", format)
                    self._format = format

            # We accept anything else without doing anything
            self.send_packet(self.ok_packet())


class MySQLRPCServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):

    """Class inheriting from SocketServer handling MySQL-RPC"""

    _thread_name_prefix = 'MySQL-RPC-Session-'

    def __init__(self, addr, handler=MySQLRPCRequestHandler,
                 bind_and_activate=True):

        """Initialize the MySQL-RPC server

        :param host: IP or hostname for binding the SocketServer
        :param port: TCP/IP port for the SocketServer
        :param number_threads: Number of threads that will be started (unused).
        :param ssl_config: SSL configuration (unused)
        """
        self.__addr = addr
        self._commands = {}
        self.__auth_disabled = False
        self.__configured = False
        self.__shutdown_request = False
        self.__active_requests_lock = threading.Lock()
        self.__active_requests = []
        self.__requests_purge_timer = None

        self.allow_reuse_address = True
        SocketServer.TCPServer.__init__(self, addr, handler, bind_and_activate)

    @property
    def auth_disabled(self):
        """Returns whether authentication is disabled or not

        :return: True if authentication is disabled, False otherwise
        :rtype: Boolean
        """
        return self.__auth_disabled

    @property
    def addr(self):
        """Returns the IP and port the server is listening on

        :return: Tuple with host address and port.
        :rtype: tuple
        """
        return self.__addr

    def _purge_inactive_requests(self, reschedule=True):
        """Purge inactive requests
        """
        threshold = timedelta(0, CLIENT_SESSION_TIMEOUT, 0)
        with self.__active_requests_lock:
            purged = []
            for request in self.__active_requests:
                delta = datetime.utcnow() - request.activity
                if delta >= threshold:
                    msg = "Force closing connection id {0}: " \
                          "timed out {1} >= {2}".format(
                        request.connection_id, repr(delta), repr(threshold))
                    _LOGGER.info(msg)
                    self.shutdown_request(request)
                    purged.append(request)
            for request in purged:
                self.__active_requests.remove(request)

        if reschedule:
            self.__requests_purge_timer = threading.Timer(
                30, self._purge_inactive_requests)
            self.__requests_purge_timer.setName('MySQL-RPC-Purger')
            self.__requests_purge_timer.start()

    def shutdown(self):
        if self.__requests_purge_timer:
            self.__requests_purge_timer.cancel()
        self._purge_inactive_requests(reschedule=False)
        SocketServer.TCPServer.shutdown(self)

    def process_request(self, request, client_address):
        """Start a new thread to process the request

        We overload this method so we can set the name of the thread.
        We also start a purging thread.

        :param request: The incoming request from client
        :param client_address: Address of the client
        """
        if not self.__requests_purge_timer:
            self.__requests_purge_timer = threading.Timer(
                30, self._purge_inactive_requests)
            self.__requests_purge_timer.setName('MySQL-RPC-Purger')
            self.__requests_purge_timer.start()

        request = WrapSocket(request)
        t = threading.Thread(
            target=self.process_request_thread,
            args=(request, client_address),
            name="{0}{1}".format(self._thread_name_prefix,
                                 next_connection_id()))
        t.daemon = True
        t.start()

    def finish_request(self, request, client_address):
        """Finish one request by instantiating RequestHandlerClass

        :param request: The incoming request from client
        :param client_address: Address of the client
        """
        connection_id = int(threading.current_thread().name.replace(
            self._thread_name_prefix, ''
        ))

        request.connection_id = connection_id
        with self.__active_requests_lock:
            self.__active_requests.append(request)

        self.RequestHandlerClass(request, client_address, self)

        with self.__active_requests_lock:
            try:
                self.__active_requests.remove(request)
            except ValueError:
                # Request must have been cleaned up earlier
                pass

    def get_command_exec(self, command_name):
        """Get the command to be executed

        :param command_name: The name of the command to be retrieved
        :return: Instance or subclass of Command or None if not found.
        :rtype: command.Command or None
        """
        try:
            return self._commands[command_name]
        except KeyError:
            return None

    def _configure(self, config):
        """Configure the server using the loaded configuration

        :param config: Instance of config.Config
        """
        try:
            value = config.get('protocol.mysql', 'disable_authentication')
            if value.lower() == 'yes':
                self.__auth_disabled = True
                _LOGGER.warning(
                    "Authentication disabled for MySQL RPC protocol."
                )
        except:
            self.__auth_disabled = False
        else:
            self.__configured = True

        _LOGGER.info(
            "MySQL-RPC protocol server started, listening on %s:%d",
            self.__addr[0], self.__addr[1]
        )

    def register_command(self, command):
        """Register a command

        :param command: Command to be registered.
        """
        if not self.__configured:
            self._configure(command.config)

        command_name = command.group_name + "." + command.command_name
        self._commands[command_name] = command.execute

    def shutdown_request(self, request):
        """Called to shutdown and close an individual request."""
        _LOGGER.debug("Close request.")
        try:
            request.shutdown(socket.SHUT_WR)
        except socket.error:
            pass
        self.close_request(request)


class FabricMySQLServer(threading.Thread):

    """Class accepting connections from MySQL clients"""

    def __init__(self, host, port, number_threads, ssl_config):
        """Initialize the thread

        :param host: IP or hostname for binding the SocketServer
        :param port: TCP/IP port for the SocketServer
        :param number_threads: Number of threads that will be started (unused).
        :param ssl_config: SSL configuration (unused)
        """
        self.__server = MySQLRPCServer((host, int(port)))

        threading.Thread.__init__(self, name="MySQL-RPC-Server",
                                  target=self.__server.serve_forever)
        self.daemon = True

    def register_command(self, command):
        """Register a command with the server.

        :param command: Command to be registered.
        """
        self.__server.register_command(command)

    def shutdown(self):
        """Shutdown the thread"""
        _LOGGER.debug("Shutting down MySQL-RPC server.")
        self.__server.shutdown()
