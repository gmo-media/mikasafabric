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

"""Define functions that can be used throughout the code.
"""
import logging
import mysql.connector

import mysql.fabric.errors as _errors

_LOGGER = logging.getLogger(__name__)

MYSQL_DEFAULT_PORT = 3306

def split_host_port(address, default_port=MYSQL_DEFAULT_PORT):
    """Return a tuple with host and port.

    If a port is not found in the address, the default port is returned.
    """
    if address.find(":") >= 0:
        host, port = address.split(":")
    else:
        host, port = (address, default_port)
        _LOGGER.warning(
            "Address '%s' does not have a port using '%s'.",
            address,
            default_port
        )
    return host, port

def combine_host_port(host, port, default_port):
    """Return a string with the parameters host and port.

    :return: String host:port.
    """
    if host:
        if host == "127.0.0.1":
            host_info = "localhost"
        else:
            host_info = host
    else:
        host_info = "unknown host"

    if port:
        port_info = port
    else:
        port_info = default_port

    return "%s:%s" % (host_info, port_info)

def exec_mysql_stmt(cnx, stmt_str, options=None):
    """Execute a statement for the client and return a result set or a
    cursor.

    This is the singular method to execute queries. If something goes
    wrong while executing a statement, the exception
    :class:`~mysql.fabric.errors.DatabaseError` is raised.

    :param cnx: Database connection.
    :param stmt_str: The statement (e.g. query, updates, etc) to execute.
    :param options: Options to control behavior:

                    - params - Parameters for statement.
                    - columns - If true, return a rows as named tuples
                      (default is False).
                    - raw - If true, do not convert MySQL's types to
                      Python's types (default is True).
                    - fetch - If true, execute the fetch as part of the
                      operation (default is True).

    :return: Either a result set as list of tuples (either named or unnamed)
             or a cursor.
    """
    if cnx is None:
        raise _errors.DatabaseError("Invalid database connection.")

    options = options or {}
    params = options.get('params', ())
    columns = options.get('columns', False)
    fetch = options.get('fetch', True)
    raw = options.get('raw', False)

    if raw and columns:
        raise _errors.ProgrammingError(
            "No raw cursor available returning named tuple"
        )

    _LOGGER.debug("Statement ({statement}, Params({parameters}).".format(
        statement=stmt_str.replace('\n', '').replace('\r', ''),
        parameters=params)
    )

    cur = None
    try:
        cur = cnx.cursor(raw=raw, named_tuple=columns)
        cur.execute(stmt_str, params)
    except Exception as error:
        if cnx.unread_result:
            cnx.get_rows()
        if cur:
            cur.close()

        errno = getattr(error, 'errno', None)
        raise _errors.DatabaseError(
            "Command (%s, %s) failed accessing (%s). %s." %
            (stmt_str, params, mysql_address_from_cnx(cnx), error),
            errno
        )

    assert(cur is not None)
    if fetch:
        results = None
        try:
            if cnx.unread_result:
                results = cur.fetchall()
        except mysql.connector.errors.InterfaceError as error:
            raise _errors.DatabaseError(
                "Command (%s, %s) failed fetching data from (%s). %s." %
                (stmt_str, params, mysql_address_from_cnx(cnx), error)
            )
        finally:
            cur.close()
        return results

    return cur

def create_mysql_connection():
    """Create a MySQLConnection object.
    """
    return mysql.connector.MySQLConnection()

def connect_to_mysql(cnx=None, **kwargs):
    """Create a connection.
    """
    try:
        if cnx is None:
            return mysql.connector.Connect(**kwargs)
        else:
            return cnx.connect(**kwargs)
    except mysql.connector.Error as error:
        raise _errors.DatabaseError(error)

def disconnect_mysql_connection(cnx):
    """Close the connection in a friendly way. Send a QUIT command.
    """
    try:
        if cnx:
            cnx.disconnect()
    except Exception as error:
        raise _errors.DatabaseError(
            "Error trying to disconnect friendly from (%s). %s." %
            (mysql_address_from_cnx(cnx), error)
        )

def destroy_mysql_connection(cnx):
    """Close the connection abruptly. Do not interact with the server.
    """
    try:
        if cnx:
            cnx.shutdown()
    except Exception as error:
        raise _errors.DatabaseError(
            "Error trying to disconnect abruptly from (%s). %s." %
            (mysql_address_from_cnx(cnx), error)
        )

def is_valid_mysql_connection(cnx):
    """Check if it is a valid MySQL connection.
    """
    if cnx is not None and cnx.is_connected():
        return True
    return False

def reestablish_mysql_connection(cnx, attempt, delay):
    """Try to reconnect if it is not already connected.
    """
    try:
        cnx.reconnect(attempt, delay)
    except (AttributeError, mysql.connector.errors.InterfaceError):
        raise _errors.DatabaseError("Invalid database connection.")

def mysql_address_from_cnx(cnx):
    """Return address associated to a connection.

    :param cnx: Connection.
    """
    if cnx is None:
        return "<Connection is None>"

    return ":".join([cnx.server_host, str(cnx.server_port)])
