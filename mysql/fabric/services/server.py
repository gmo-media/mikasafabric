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
"""This module provides the necessary interfaces for performing administrative
tasks on groups and servers, specifically MySQL Servers.

It is possible to add, update and remove a group. One cannot, however, remove
a group if there are associated servers. It is possible to add a server to a
group and remove a server from a group. Search functions are also provided so
that one may look up groups and servers. Given a server's address, one may
also find out the server's uuid if the server is alive and kicking.

When a group is created though, it is inactive which means that the failure
detector will not check if its servers are alive. To start up the failure
detector, one needs to explicitly activate it per group. A server may have
one of the following statuses:

- PRIMARY - This is set when the server may accept both reads and writes
  operations.
- SECONDARY - This is set when the server may accept only read operations.
- SPARE - This is set when users want to have server that is kept in sync
  but does not accept neither reads or writes operations.
- FAULTY - This is set by the failure detector and indicates that a server
  is not reachable.

Find in what follows the possible state transitions:

.. graphviz::

   digraph state_transition {
    rankdir=LR;
    size="8,5"

    node [shape = circle]; Primary;
    node [shape = circle]; Secondary;
    node [shape = circle]; Spare;
    node [shape = circle]; Faulty;

    Primary   -> Secondary [ label = "demote" ];
    Primary   -> Faulty [ label = "failure" ];
    Secondary -> Primary [ label = "promote" ];
    Secondary -> Spare [ label = "set_status" ];
    Secondary -> Faulty [ label = "failure" ];
    Spare     -> Primary [ label = "promote" ];
    Spare     -> Secondary [ label = "set_status" ];
    Spare     -> Faulty [ label = "failure" ];
    Faulty    -> Spare [ label = "set_status" ];
  }

It is worth noticing that this module only provides functions for performing
basic administrative tasks, provisioning and high-availability functions are
provided elsewhere.
"""
import logging
import uuid as _uuid

import mysql.fabric.services.utils as _services_utils

from mysql.connector.errorcode import (
    ER_ROW_IS_REFERENCED,
    ER_ROW_IS_REFERENCED_2,
)

from mysql.fabric import (
    backup as _backup,
    events as _events,
    server as _server,
    errors as _errors,
    failure_detector as _detector,
    sharding as _sharding,
    config as _config,
)

from mysql.fabric.command import (
    ProcedureGroup,
    Command,
    ResultSet,
    CommandResult,
)

from mysql.fabric.server_utils import (
    split_host_port
)

_LOGGER = logging.getLogger(__name__)

SERVER_NOT_FOUND = "Server with UUID %s not found."
MYSQLDUMP_NOT_FOUND = "Unable to find mysqldump in location %s"
MYSQLCLIENT_NOT_FOUND = "Unable to find MySQL Client in location %s"

MIN_UNREACHABLE_TIMEOUT = 1
MAX_UNREACHABLE_TIMEOUT = 60
DEFAULT_UNREACHABLE_TIMEOUT = 5

class GroupLookups(Command):
    """Return information on existing group(s).
    """
    group_name = "group"
    command_name = "lookup_groups"

    def execute(self, group_id=None):
        """Return information on existing group(s).

        :param group_id: None if one wants to list the existing groups or
                         group's id if one wants information on a group.
        :return: List with {"group_id" : group_id, "failure_detector": ON/OFF,
                 "description" : description}.
        """

        if group_id is None:
            gids = _server.Group.groups()
        else:
            gids = [group_id]

        _LOGGER.debug("Group IDs: %s", gids)

        # Fetch all the groups before building the result set since an
        # exception can be thrown and there is little point in trying
        # to build a result set before all groups can be fetched.
        groups = [ _retrieve_group(gid) for gid in gids ]

        rset = ResultSet(
            names=('group_id', 'description', 'failure_detector', 'master_uuid'),
            types=(str, str, bool, str)
        )

        for group in groups:
            rset.append_row([
                group.group_id,          # group_id
                group.description,       # description
                group.status,            # failure_detector
                group.master,            # master_uuid
            ])

        return CommandResult(None, results=rset)

CREATE_GROUP = _events.Event()
class GroupCreate(ProcedureGroup):
    """Create a group.
    """
    group_name = "group"
    command_name = "create"

    def execute(self, group_id, description=None, synchronous=True):
        """Create a group.

        :param group_id: Group's id.
        :param description: Group's description.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            CREATE_GROUP, self.get_lockable_objects(), group_id, description
        )
        return self.wait_for_procedures(procedures, synchronous)

UPDATE_GROUP = _events.Event()
class GroupDescription(ProcedureGroup):
    """Update group's description.
    """
    group_name = "group"
    command_name = "description"

    def execute(self, group_id, description=None, synchronous=True):
        """Update group's description.

        :param group_id: Group's id.
        :param description: Group's description.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            UPDATE_GROUP, self.get_lockable_objects(), group_id, description
        )
        return self.wait_for_procedures(procedures, synchronous)

DESTROY_GROUP = _events.Event()
class DestroyGroup(ProcedureGroup):
    """Remove a group.
    """
    group_name = "group"
    command_name = "destroy"

    def execute(self, group_id, synchronous=True):
        """Remove a group.

        :param group_id: Group's id.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            DESTROY_GROUP, self.get_lockable_objects(), group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

class ServerLookups(Command):
    """Return information on existing server(s) in a group.
    """
    group_name = "group"
    command_name = "lookup_servers"

    def execute(self, group_id, server_id=None, status=None, mode=None):
        """Return information on existing server(s) in a group.

        :param group_id: Group's id.
        :param uuid: None if one wants to list the existing servers
                     in a group or server's id if one wants information
                     on a server in a group.
        :server_id type: Servers's UUID or HOST:PORT.
        :param status: Server's status one is searching for.
        :param mode: Server's mode one is searching for.
        :return: Information on servers.
        :rtype: List with [uuid, address, status, mode, weight]
        """
        # Determine the set of servers to iterate through.
        group = _retrieve_group(group_id)
        if server_id is None:
            servers = [server for server in group.servers()]
        else:
            servers = [_retrieve_server(server_id, group_id)]

        # Determine the set of status to check upon.
        if status is None:
            status = _server.MySQLServer.SERVER_STATUS
        else:
            status = [_retrieve_server_status(status)]

        # Determine the set of modes to check upon.
        if mode is None:
            mode = _server.MySQLServer.SERVER_MODE
        else:
            mode = [_retrieve_server_mode(mode)]

        # Create result set.
        rset = ResultSet(
            names=('server_uuid', 'address', 'status', 'mode', 'weight'),
            types=(str, str, str, str, float),
        )
        for server in servers:
            if server.status in status and server.mode in mode:
                rset.append_row([
                    str(server.uuid),
                    server.address,
                    server.status,
                    server.mode,
                    server.weight
                ])

        return CommandResult(None, results=rset)

class ServerUuid(Command):
    """Return server's uuid.
    """
    group_name = "server"
    command_name = "lookup_uuid"

    def execute(self, address, timeout=None):
        """Return server's UUID.

        :param address: Server's address.
        :param timeout: Time in seconds after which an error is reported
                        if the UUID is not retrieved.
        :return: UUID.
        """
        rset = ResultSet(names=['uuid'], types=[str])
        rset.append_row([_lookup_uuid(address, timeout)])
        return CommandResult(None, results=rset)

ADD_SERVER = _events.Event()
class ServerAdd(ProcedureGroup):
    """Add a server into group.

    If users just want to update the state store and skip provisioning steps
    such as configuring replication, the update_only parameter must be set to
    true.

    Note that the current implementation has a simple provisioning step that
    makes the server point to the master if there is any.
    """
    group_name = "group"
    command_name = "add"

    def execute(self, group_id, address, timeout=None, update_only=False,
                synchronous=True):
        """Add a server into a group.

        :param group_id: Group's id.
        :param address: Server's address.
        :param timeout: Time in seconds after which an error is reported
                        if one cannot access the server.
        :update_only: Only update the state store and skip provisioning.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        :return: Tuple with job's uuid and status.
        """
        _LOGGER.debug("executing group add.")
        procedures = _events.trigger(ADD_SERVER, self.get_lockable_objects(),
            group_id, address, timeout, update_only
        )
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SERVER = _events.Event()
class ServerRemove(ProcedureGroup):
    """Remove a server from a group.
    """
    group_name = "group"
    command_name = "remove"

    def execute(self, group_id, server_id, synchronous=True):
        """Remove a server from a group.

        :param group_id: Group's id.
        :param server_id: Servers's UUID or HOST:PORT.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(REMOVE_SERVER, self.get_lockable_objects(),
            group_id, server_id
        )
        return self.wait_for_procedures(procedures, synchronous)

ACTIVATE_GROUP = _events.Event()
class ActivateGroup(ProcedureGroup):
    """Activate failure detector for server(s) in a group.

    By default the failure detector is disabled.
    """
    group_name = "group"
    command_name = "activate"

    def execute(self, group_id, synchronous=True):
        """Activate a group.

        :param group_id: Group's id.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            ACTIVATE_GROUP, self.get_lockable_objects(), group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

DEACTIVATE_GROUP = _events.Event()
class DeactivateGroup(ProcedureGroup):
    """Deactivate failure detector for server(s) in a group.

    By default the failure detector is disabled.
    """
    group_name = "group"
    command_name = "deactivate"

    def execute(self, group_id, synchronous=True):
        """Deactivate group.

        :param group_id: Group's id.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            DEACTIVATE_GROUP, self.get_lockable_objects(), group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

class DumpServers(Command):
    """Return information about servers.

    The servers might belong to any group that matches any of the provided
    patterns, or all servers if no patterns are provided.
    """
    group_name = "dump"
    command_name = "servers"

    def execute(self, connector_version=None, patterns=""):
        """Return information about all servers.

        :param connector_version: The connectors version of the data.
        :param patterns: group pattern.
        """
        rset = ResultSet(
            names=('server_uuid', 'group_id', 'host', 'port', 'mode', 'status', 'weight'),
            types=(str, str, str, int, int, int, float)
        )

        for row in _server.MySQLServer.dump_servers(connector_version, patterns):
            rset.append_row(row)

        return CommandResult(None, results=rset)


SET_SERVER_STATUS = _events.Event()
class SetServerStatus(ProcedureGroup):
    """Set a server's status.

    Any server added into a group has to be alive and kicking and its status
    is automatically set to SECONDARY. If the failure detector is activate
    and the server is not reachable, it is automatically set to FAULTY.

    Users can also manually change the server's status. Usually, a user may
    change a slave's mode to SPARE to avoid write and read access and
    guarantee that it is not choosen when a failover or swithover routine is
    executed.

    By default replication is automatically configured when a server has its
    status changed. In order to skip this, users must set the update_only
    parameter to true. If done so, only the state store will be updated with
    information on the new status.
    """
    group_name = "server"
    command_name = "set_status"

    def execute(self, server_id, status, update_only=False, synchronous=True):
        """Set a server's status.

        :param server_id: Servers's UUID or HOST:PORT.
        :param status: Server's status.
        :update_only: Only update the state store and skip provisioning.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        """
        procedures = _events.trigger(
            SET_SERVER_STATUS, self.get_lockable_objects(), server_id, status,
            update_only
        )
        return self.wait_for_procedures(procedures, synchronous)

SET_SERVER_WEIGHT = _events.Event()
class SetServerWeight(ProcedureGroup):
    """Set a server's weight.

    The weight determines the likelihood of a server being choseen by a
    connector to process transactions. For example, a server whose weight
    is 2.0 may receive 2 times more requests than a server whose weight is
    1.0.
    """
    group_name = "server"
    command_name = "set_weight"

    def execute(self, server_id, weight, synchronous=True):
        """Set a server's weight.

        :param server_id: Servers's UUID or HOST:PORT.
        :param weight: Server's weight.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        """
        procedures = _events.trigger(
            SET_SERVER_WEIGHT, self.get_lockable_objects(), server_id, weight
        )
        return self.wait_for_procedures(procedures, synchronous)

SET_SERVER_MODE = _events.Event()
class SetServerMode(ProcedureGroup):
    """Set a server's mode.

    The mode determines whether a server can process read-only, read-write
    or both transaction types.
    """
    group_name = "server"
    command_name = "set_mode"

    def execute(self, server_id, mode, synchronous=True):
        """Set a server's mode.

        :param server_id: Servers's UUID or HOST:PORT.
        :param weight: Server's weight.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        """
        procedures = _events.trigger(
            SET_SERVER_MODE, self.get_lockable_objects(), server_id, mode
        )
        return self.wait_for_procedures(procedures, synchronous)

BACKUP_SERVER = _events.Event("BACKUP_SERVER")
RESTORE_SERVER = _events.Event("RESTORE_SERVER")
class CloneServer(ProcedureGroup):
    """Clone the objects of a given server into a destination server.
    """
    group_name = "server"
    command_name = "clone"

    def execute(self, group_id, destn_address, source_id=None, timeout=None,
                synchronous=True):
        """Clone the objects of a given server into a destination server.

        :param group_id: The ID of the source group.
        :param destn_address: The address of the destination MySQL Server.
        :param source_id: The address or UUID of the source MySQL Server.
        :param timeout: Time in seconds after which an error is reported
                        if the destination server is unreachable.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        """
        # If the destination server is already part of a Fabric Group, raise
        # an error
        destn_server_uuid = _lookup_uuid(destn_address, timeout)
        _check_server_not_in_any_group(destn_server_uuid)
        host, port = split_host_port(destn_address)

        # Fetch config information

        backup_user = _services_utils.read_config_value(
                                self.config,
                                'servers',
                                'backup_user'
                            )
        backup_passwd = _services_utils.read_config_value(
                                self.config,
                                'servers',
                                'backup_password'
                            )
        restore_user = _services_utils.read_config_value(
                                self.config,
                                'servers',
                                'restore_user'
                            )
        restore_passwd = _services_utils.read_config_value(
                                self.config,
                                'servers',
                                'restore_password'
                            )

        mysqldump_binary = _services_utils.read_config_value(
                                self.config,
                                'sharding',
                                'mysqldump_program'
                            )
        mysqlclient_binary = _services_utils.read_config_value(
                                self.config,
                                'sharding',
                                'mysqlclient_program'
                            )

        if not _services_utils.is_valid_binary(mysqldump_binary):
            raise _errors.ServerError(MYSQLDUMP_NOT_FOUND % mysqldump_binary)

        if not _services_utils.is_valid_binary(mysqlclient_binary):
            raise _errors.ServerError(MYSQLCLIENT_NOT_FOUND % mysqlclient_binary)

        # Check if the destination server has restore privileges.
        server = _server.MySQLServer(_uuid.UUID(destn_server_uuid), destn_address,
                                     restore_user, restore_passwd)
        _backup.MySQLDump.check_restore_privileges(server)

        # Fetch a reference to source server.
        if source_id:
            server = _retrieve_server(source_id, group_id)
        else:
            group = _retrieve_group(group_id)
            server = _services_utils.fetch_backup_server(group)

        # Check if the source server has backup privileges.
        server.user = backup_user
        server.passwd = backup_passwd
        _backup.MySQLDump.check_backup_privileges(server)

        # Schedule the clone operation through the executor.
        procedures = _events.trigger(
            BACKUP_SERVER,
            self.get_lockable_objects(),
            str(server.uuid),
            host,
            port
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(CREATE_GROUP)
def _create_group(group_id, description):
    """Create a group.
    """
    _check_group_exists(group_id)

    group = _server.Group(group_id=group_id, description=description,
                          status=_server.Group.INACTIVE)
    _server.Group.add(group)
    _LOGGER.debug("Added group (%s).", group)

@_events.on_event(ACTIVATE_GROUP)
def _activate_group(group_id):
    """Activate a group.
    """
    group = _retrieve_group(group_id)
    group.status = _server.Group.ACTIVE

    _detector.FailureDetector.register_group(group_id)
    _LOGGER.debug("Group (%s) is active.", group)

@_events.on_event(DEACTIVATE_GROUP)
def _deactivate_group(group_id):
    """Deactivate a group.
    """
    group = _retrieve_group(group_id)
    group.status = _server.Group.INACTIVE
    _detector.FailureDetector.unregister_group(group_id)
    _LOGGER.debug("Group (%s) is active.", str(group))

@_events.on_event(UPDATE_GROUP)
def _update_group_description(group_id, description):
    """Update a group description.
    """
    group = _retrieve_group(group_id)
    group.description = description
    _LOGGER.debug("Updated group (%s).", group)

@_events.on_event(DESTROY_GROUP)
def _destroy_group(group_id):
    """Destroy a group.
    """
    group = _retrieve_group(group_id)
    _check_group_dependencies(group)
    _detector.FailureDetector.unregister_group(group_id)
    try:
        _server.Group.remove(group)
    except _errors.DatabaseError as error:
        foreign_key_errors = (ER_ROW_IS_REFERENCED, ER_ROW_IS_REFERENCED_2)
        if error.errno in foreign_key_errors:
            raise _errors.GroupError(
                "Cannot destroy group (%s): %s." % (group_id, error, )
            )
        raise
    _LOGGER.debug("Destroyed group (%s).", group)

@_destroy_group.undo
def _undo_destroy_group(group_id):
    """Register a group if the destroy operation has failed.
    """
    _detector.FailureDetector.register_group(group_id)

def _lookup_uuid(address, timeout):
    """Return server's uuid.
    """
    try:
        timeout = float(timeout)
    except (TypeError, ValueError):
        pass

    timeout = timeout or DEFAULT_UNREACHABLE_TIMEOUT
    try:
        return _server.MySQLServer.discover_uuid(
            address=address, connection_timeout=timeout
        )
    except _errors.DatabaseError as error:
        raise _errors.ServerError(
            "Error accessing server (%s): %s." % (address, error)
        )

@_events.on_event(ADD_SERVER)
def _add_server(group_id, address, timeout, update_only):
    """Add a server into a group.
    """
    group = _retrieve_group(group_id)
    uuid = _lookup_uuid(address, timeout)
    _check_server_exists(uuid)
    server = _server.MySQLServer(uuid=_uuid.UUID(uuid), address=address)

    # Check if the server fulfils the necessary requirements to become
    # a member.
    _check_requirements(server)

    # Add server to the state store.
    _server.MySQLServer.add(server)

    # Add server as a member in the group.
    server.group_id = group_id

    if not update_only:
        # Configure the server as a slave if there is a master.
        _configure_as_slave(group, server)

    _LOGGER.debug("Added server (%s) to group (%s).", server, group)

@_events.on_event(REMOVE_SERVER)
def _remove_server(group_id, server_id):
    """Remove a server from a group.
    """
    group = _retrieve_group(group_id)
    server = _retrieve_server(server_id, group_id)

    if group.master == server.uuid:
        raise _errors.ServerError(
            "Cannot remove server (%s), which is master in group (%s). "
            "Please, demote it first." % (server.uuid, group_id)
        )

    _server.MySQLServer.remove(server)
    server.disconnect()
    _server.ConnectionManager().purge_connections(server)

@_events.on_event(SET_SERVER_STATUS)
def _set_server_status(server_id, status, update_only):
    """Set a server's status.
    """
    status = _retrieve_server_status(status)
    server = _retrieve_server(server_id)

    if status == _server.MySQLServer.PRIMARY:
        _set_server_status_primary(server, update_only)
    elif status == _server.MySQLServer.FAULTY:
        _set_server_status_faulty(server, update_only)
    elif status == _server.MySQLServer.SECONDARY:
        _set_server_status_secondary(server, update_only)
    elif status == _server.MySQLServer.SPARE:
        _set_server_status_spare(server, update_only)

def _retrieve_server_status(status):
    """Check whether the server's status is valid or not and
    if an integer was provided retrieve the correspondent
    string.
    """
    valid = False
    try:
        idx = int(status)
        try:
            status = _server.MySQLServer.get_status(idx)
            valid = True
        except IndexError:
            pass
    except ValueError:
        try:
            status = str(status).upper()
            _server.MySQLServer.get_status_idx(status)
            valid = True
        except ValueError:
            pass

    if not valid:
        values = [ str((_server.MySQLServer.get_status_idx(value), value))
                   for value in _server.MySQLServer.SERVER_STATUS ]
        raise _errors.ServerError("Trying to use an invalid status (%s). "
            "Possible values are %s." % (status, ", ".join(values))
        )

    return status

def _set_server_status_primary(server, update_only):
    """Set server's status to primary.
    """
    raise _errors.ServerError(
        "If you want to make a server (%s) primary, please, use the "
        "group.promote function." % (server.uuid, )
    )

def _set_server_status_faulty(server, update_only):
    raise _errors.ServerError(
        "If you want to set a server (%s) to faulty, please, use the "
        "threat.report_faulty interface." % (server.uuid, )
    )

def _set_server_status_secondary(server, update_only):
    """Set server's status to secondary.
    """
    allowed_status = [_server.MySQLServer.SPARE]
    status = _server.MySQLServer.SECONDARY
    mode = _server.MySQLServer.READ_ONLY
    _do_set_status(server, allowed_status, status, mode, update_only)

def _set_server_status_spare(server, update_only):
    """Set server's status to spare.
    """
    allowed_status = [
        _server.MySQLServer.SECONDARY, _server.MySQLServer.FAULTY
    ]
    status = _server.MySQLServer.SPARE
    mode = _server.MySQLServer.OFFLINE
    previous_status = server.status
    _do_set_status(server, allowed_status, status, mode, update_only)

    if previous_status == _server.MySQLServer.FAULTY:
        # Check whether the server is really alive or not.
        _check_requirements(server)

        # Configure replication
        if not update_only:
            group = _server.Group.fetch(server.group_id)
            _configure_as_slave(group, server)

def _do_set_status(server, allowed_status, status, mode, update_only):
    """Set server's status.
    """
    allowed_transition = server.status in allowed_status
    previous_status = server.status
    if allowed_transition:
        server.status = status
        server.mode = mode
    else:
        raise _errors.ServerError(
            "Cannot change server's (%s) status from (%s) to (%s)." %
            (str(server.uuid), server.status, status)
        )

    _LOGGER.debug(
        "Changed server's status (%s) from (%s) to (%s). Update-only "
        "state store parameter is (%s).", str(server.uuid), previous_status,
        server.status, update_only
    )

@_events.on_event(SET_SERVER_WEIGHT)
def _set_server_weight(server_id, weight):
    """Set server's weight.
    """
    server = _retrieve_server(server_id)

    try:
        weight = float(weight)
    except ValueError:
        raise _errors.ServerError("Value (%s) must be a float." % (weight, ))

    if weight <= 0.0:
        raise _errors.ServerError(
            "Cannot set the server's weight (%s) to a value lower "
            "than or equal to 0.0" % (weight, )
        )
    server.weight = weight

@_events.on_event(SET_SERVER_MODE)
def _set_server_mode(server_id, mode):
    """Set server's mode.
    """
    mode = _retrieve_server_mode(mode)
    server = _retrieve_server(server_id)

    if server.status == _server.MySQLServer.PRIMARY:
        _set_server_mode_primary(server, mode)
    elif server.status == _server.MySQLServer.SECONDARY:
        _set_server_mode_secondary(server, mode)
    elif server.status == _server.MySQLServer.SPARE:
        _set_server_mode_spare(server, mode)
    elif server.status == _server.MySQLServer.FAULTY:
        _set_server_mode_faulty(server, mode)

@_events.on_event(BACKUP_SERVER)
def _backup_server(source_uuid, host, port):
    """Backup the source server, given by the source_uuid.

    :param source_uuid: The UUID of the source server.
    :param host: The hostname of the destination server.
    :param port: The port number of the destination server.
    """
    backup_user = _services_utils.read_config_value(
                            _config.global_config,
                            'servers',
                            'backup_user'
                        )
    backup_passwd = _services_utils.read_config_value(
                            _config.global_config,
                            'servers',
                            'backup_password'
                        )
    mysqldump_binary = _services_utils.read_config_value(
                            _config.global_config,
                            'sharding',
                            'mysqldump_program'
                        )

    source_server = _server.MySQLServer.fetch(source_uuid)
    #Do the backup of the group hosting the source shard.
    backup_image = _backup.MySQLDump.backup(
                        source_server,
                        backup_user, backup_passwd,
                        mysqldump_binary
                    )
    _LOGGER.debug("Done with backup of server with uuid = %s.", source_uuid)
    _events.trigger_within_procedure(
        RESTORE_SERVER,
        source_uuid,
        host,
        port,
        backup_image.path
    )

@_events.on_event(RESTORE_SERVER)
def _restore_server(source_uuid, host, port, backup_image):
    """Restore the backup on the destination Server.

    :param source_uuid: The UUID of the source server.
    :param host: The hostname of the destination server.
    :param port: The port number of the destination server.
    :param backup_image: The backup image path.
    """
    restore_user = _services_utils.read_config_value(
                            _config.global_config,
                            'servers',
                            'restore_user'
                        )
    restore_passwd = _services_utils.read_config_value(
                            _config.global_config,
                            'servers',
                            'restore_password'
                        )

    mysqlclient_binary = _services_utils.read_config_value(
                            _config.global_config,
                            'sharding',
                            'mysqlclient_program'
                        )

    source_server = _server.MySQLServer.fetch(source_uuid)
    if not source_server:
        raise _errors.ServerError(SERVER_NOT_FOUND % source_uuid)
    #Build a backup image that will be used for restoring
    bk_img = _backup.BackupImage(backup_image)
    _backup.MySQLDump.restore_server(
        host,
        port,
        restore_user, restore_passwd,
        bk_img,
        mysqlclient_binary
    )
    _LOGGER.debug("Done with restore of server with host = %s, port = %s",
                  host, port)

def _retrieve_server_mode(mode):
    """Check whether the server's mode is valid or not and if an integer was
    provided retrieve the correspondent string.
    """
    valid = False
    try:
        idx = int(mode)
        try:
            mode = _server.MySQLServer.get_mode(idx)
            valid = True
        except IndexError:
            pass
    except ValueError:
        try:
            mode = str(mode).upper()
            _server.MySQLServer.get_mode_idx(mode)
            valid = True
        except ValueError:
            pass

    if not valid:
        values = [ str((_server.MySQLServer.get_mode_idx(value), value))
                   for value in _server.MySQLServer.SERVER_MODE ]
        raise _errors.ServerError("Trying to use an invalid mode (%s). "
            "Possible values are: %s." % (mode, ", ".join(values))
        )

    return mode

def _set_server_mode_primary(server, mode):
    """Set server's mode when it is a primary.
    """
    allowed_mode = \
        (_server.MySQLServer.WRITE_ONLY, _server.MySQLServer.READ_WRITE)
    _do_set_server_mode(server, mode, allowed_mode)

def _set_server_mode_secondary(server, mode):
    """Set server's mode when it is a secondary.
    """
    allowed_mode = \
        (_server.MySQLServer.OFFLINE, _server.MySQLServer.READ_ONLY)
    _do_set_server_mode(server, mode, allowed_mode)

def _set_server_mode_spare(server, mode):
    """Set server's mode when it is a spare.
    """
    allowed_mode = \
        (_server.MySQLServer.OFFLINE, _server.MySQLServer.READ_ONLY)
    _do_set_server_mode(server, mode, allowed_mode)

def _set_server_mode_faulty(server, mode):
    """Set server's mode when it is a faulty.
    """
    allowed_mode = ()
    _do_set_server_mode(server, mode, allowed_mode)

def _do_set_server_mode(server, mode, allowed_mode):
    """Set server's mode.
    """
    if mode not in allowed_mode:
        raise _errors.ServerError(
            "Cannot set mode to (%s) when the server's (%s) status is (%s)."
            % (mode, server.uuid, server.status)
        )
    server.mode = mode

def _retrieve_server(server_id, group_id=None):
    """Return a MySQLServer object from a UUID or a HOST:PORT.
    """
    server = _server.MySQLServer.fetch(server_id)

    if not server:
        raise _errors.ServerError(
            "Server (%s) does not exist." % (server_id, )
            )

    if not server.group_id:
        raise _errors.GroupError(
            "Server (%s) does not belong to a group." % (server_id, )
            )

    if group_id is not None and group_id != server.group_id:
        raise _errors.GroupError(
            "Group (%s) does not contain server (%s)." % (group_id, server_id)
        )

    return server

def _check_server_exists(server_id):
    """Check whether a MySQLServer instance exists or not.

    :param server_id: The UUID or the host:port for the server.
    """
    server = _server.MySQLServer.fetch(server_id)

    if server:
        raise _errors.ServerError("Server (%s) already exists." % (server_id, ))

def _check_server_not_in_any_group(server_id):
    """Check for both the presence of the server object and its associated
       group. If the server belongs to a group an error is raised.

    :param server_id: The UUID or the host:port for the server.
    """
    server = _server.MySQLServer.fetch(server_id)
    if server and server.group_id:
        raise _errors.ServerError(
            "Destination server (%s) is already part of group (%s)" %
            (server.address, server.group_id)
        )

def _retrieve_group(group_id):
    """Return a Group object from an identifier.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))
    return group

def _check_group_exists(group_id):
    """Check whether a group exists or not.
    """
    group = _server.Group.fetch(group_id)
    if group:
        raise _errors.GroupError("Group (%s) already exists." % (group_id, ))

def _check_group_dependencies(group):
    """Check whether there is a shard associated with the group.
    """
    group_id = group.group_id

    shard_id = _sharding.Shards.lookup_shard_id(group_id)
    if shard_id:
        raise _errors.GroupError(
            "Cannot destroy a group (%s) which is associated to a shard (%s)." %
            (group_id, shard_id)
        )

    shard_mapping_id = _sharding.ShardMapping.lookup_shard_mapping_id(group_id)
    if shard_mapping_id:
        raise _errors.GroupError(
            "Cannot destroy a group (%s) which is used as a global group in a "
            "shard definition (%s)." % (group_id, shard_mapping_id)
        )

    if group.servers():
        raise _errors.GroupError(
            "Cannot destroy a group (%s) which has associated servers." %
            (group_id, )
        )

def _check_requirements(server):
    """Check if the server fulfils some requirements.
    """
    # Being able to connect to the server is the first requirment.
    server.connect()

    if not server.check_version_compat((5, 6, 8)):
        raise _errors.ServerError(
            "Server (%s) has an outdated version (%s). 5.6.8 or greater "
            "is required." % (server.uuid, server.version)
        )

    server.check_server_privileges()

    if not server.gtid_enabled or not server.binlog_enabled:
        raise _errors.ServerError(
            "Server (%s) does not have the binary log or gtid enabled."
            % (server.uuid, )
        )

def _configure_as_slave(group, server):
    """Configure the server as a slave.
    """
    try:
        if group.master:
            master = _server.MySQLServer.fetch(group.master)
            master.connect()
            _services_utils.switch_master(server, master)
    except _errors.DatabaseError as error:
        msg = "Error trying to configure server ({0}) as slave: {1}.".format(
            server.uuid, error)
        _LOGGER.debug(msg)
        raise _errors.ServerError(msg)

def configure(config):
    """Set configuration values.
    """
    try:
        timeout = int(config.get("servers", "unreachable_timeout"))
        if timeout < MIN_UNREACHABLE_TIMEOUT:
            _LOGGER.warning(
                "Unreachable timeout cannot be lower than %s.",
                MIN_UNREACHABLE_TIMEOUT
            )
            timeout = MIN_UNREACHABLE_TIMEOUT
        if timeout > MAX_UNREACHABLE_TIMEOUT:
            _LOGGER.warning(
                "Unreachable timeout cannot be greater than %s.",
                MAX_UNREACHABLE_TIMEOUT
            )
            timeout = MAX_UNREACHABLE_TIMEOUT
        global DEFAULT_UNREACHABLE_TIMEOUT
        DEFAULT_UNREACHABLE_TIMEOUT = timeout
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass
