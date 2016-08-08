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

"""This module provides the necessary interfaces for performing administrative
tasks on replication.
"""
import re
import logging
import uuid as _uuid

import mysql.fabric.services.utils as _utils

from  mysql.fabric import (
    events as _events,
    group_replication as _group_replication,
    server as _server,
    replication as _replication,
    errors as _errors,
)

from mysql.fabric.command import (
    ProcedureGroup,
)

from mysql.fabric.services.server import (
    _retrieve_server
)

_LOGGER = logging.getLogger(__name__)

# Find out which operation should be executed.
DEFINE_HA_OPERATION = _events.Event()
# Find a slave that was not failing to keep with the master's pace.
FIND_CANDIDATE_FAIL = _events.Event("FAIL_OVER")
# Check if the candidate is properly configured to become a master.
CHECK_CANDIDATE_FAIL = _events.Event()
# Wait until all slaves or a candidate process the relay log.
WAIT_SLAVE_FAIL = _events.Event()
# Find a slave that is not failing to keep with the master's pace.
FIND_CANDIDATE_SWITCH = _events.Event()
# Check if the candidate is properly configured to become a master.
CHECK_CANDIDATE_SWITCH = _events.Event()
# Block any write access to the master.
BLOCK_WRITE_SWITCH = _events.Event()
# Wait until all slaves synchronize with the master.
WAIT_SLAVES_SWITCH = _events.Event()
# Enable the new master by making slaves point to it.
CHANGE_TO_CANDIDATE = _events.Event()
class PromoteMaster(ProcedureGroup):
    """Promote a server into master.

    If users just want to update the state store and skip provisioning steps
    such as configuring replication, the update_only parameter must be set to
    true. Otherwise, the following happens.

    If the master within a group fails, a new master is either automatically
    or manually selected among the slaves in the group. The process of
    selecting and setting up a new master after detecting that the current
    master failed is known as failover.

    It is also possible to switch to a new master when the current one is still
    alive and kicking. The process is known as switchover and may be used, for
    example, when one wants to take the current master off-line for
    maintenance.

    If a slave is not provided, the best candidate to become the new master is
    found. Any candidate must have the binary log enabled, should
    have logged the updates executed through the SQL Thread and both
    candidate and master must belong to the same group. The smaller the lag
    between slave and the master the better. So the candidate which satisfies
    the requirements and has the smaller lag is chosen to become the new
    master.

    In the failover operation, after choosing a candidate, one makes the slaves
    point to the new master and updates the state store setting the new master.

    In the switchover operation, after choosing a candidate, any write access
    to the current master is disabled and the slaves are synchronized with it.
    Failures during the synchronization that do not involve the candidate slave
    are ignored. Then slaves are stopped and configured to point to the new
    master and the state store is updated setting the new master.
    """
    group_name = "group"
    command_name = "promote"

    def execute(self, group_id, slave_id=None, update_only=False,
                synchronous=True):
        """Promote a new master.

        :param uuid: Group's id.
        :param slave_id: Candidate's UUID or HOST:PORT.
        :param update_only: Only update the state store and skip provisioning.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.

        In what follows, one will find a figure that depicts the sequence of
        event that happen during a promote operation. The figure is split in
        two pieces and names are abbreviated in order to ease presentation:

        .. seqdiag::

          diagram {
            activation = none;
            === Schedule "find_candidate" ===
            fail_over --> executor [ label = "schedule(find_candidate)" ];
            fail_over <-- executor;
            === Execute "find_candidate" and schedule "check_candidate" ===
            executor -> find_candidate [ label = "execute(find_candidate)" ];
            find_candidate --> executor [ label = "schedule(check_candidate)" ];
            find_candidate <-- executor;
            executor <- find_candidate;
            === Execute "check_candidate" and schedule "wait_slave" ===
            executor -> check_candidate [ label = "execute(check_candidate)" ];
            check_candidate --> executor [ label = "schedule(wait_slave)" ];
            check_candidate <-- executor;
            executor <- check_candidate;
            === Continue in the next diagram ===
          }

        .. seqdiag::

          diagram {
            activation = none;
            edge_length = 300;
            === Continuation from previous diagram ===
            === Execute "wait_slaves" and schedule "change_to_candidate" ===
            executor -> wait_slave [ label = "execute(wait_slave)" ];
            wait_slave --> executor [ label = "schedule(change_to_candidate)" ];
            wait_slave <-- executor;
            executor <- wait_slave;
            === Execute "change_to_candidate" ===
            executor -> change_to_candidate [ label = "execute(change_to_candidate)" ];
            change_to_candidate <- executor;
          }

        In what follows, one will find a figure that depicts the sequence of
        events that happen during the switchover operation. The figure is split
        in two pieces and names are abreviated in order to ease presentation:

        .. seqdiag::

          diagram {
            activation = none;
            === Schedule "find_candidate" ===
            switch_over --> executor [ label = "schedule(find_candidate)" ];
            switch_over <-- executor;
            === Execute "find_candidate" and schedule "check_candidate" ===
            executor -> find_candidate [ label = "execute(find_candidate)" ];
            find_candidate --> executor [ label = "schedule(check_candidate)" ];
            find_candidate <-- executor;
            executor <- find_candidate;
            === Execute "check_candidate" and schedule "block_write" ===
            executor -> check_candidate [ label = "execute(check_candidate)" ];
            check_candidate --> executor [ label = "schedule(block_write)" ];
            check_candidate <-- executor;
            executor <- check_candidate;
            === Execute "block_write" and schedule "wait_slaves" ===
            executor -> block_write [ label = "execute(block_write)" ];
            block_write --> executor [ label = "schedule(wait_slaves)" ];
            block_write <-- executor;
            executor <- block_write;
            === Continue in the next diagram ===
          }

        .. seqdiag::

          diagram {
            activation = none;
            edge_length = 350;
            === Continuation from previous diagram ===
            === Execute "wait_slaves_catch" and schedule "change_to_candidate" ===
            executor -> wait_slaves [ label = "execute(wait_slaves)" ];
            wait_slaves --> executor [ label = "schedule(change_to_candidate)" ];
            wait_slaves <-- executor;
            executor <- wait_slaves;
            === Execute "change_to_candidate" ===
            executor -> change_to_candidate [ label = "execute(change_to_candidate)" ];
            executor <- change_to_candidate;
          }
        """
        procedures = _events.trigger(
            DEFINE_HA_OPERATION, self.get_lockable_objects(), group_id,
            slave_id, update_only
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(DEFINE_HA_OPERATION)
def _define_ha_operation(group_id, slave_id, update_only):
    """Define which operation must be called based on the master's status
    and whether the candidate slave is provided or not.
    """
    fail_over = True

    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    if update_only and not slave_id:
        raise _errors.ServerError(
            "The new master must be specified through --slave-uuid if "
            "--update-only is set."
        )

    if group.master:
        master = _server.MySQLServer.fetch(group.master)
        if master.status != _server.MySQLServer.FAULTY:
            if update_only:
                _do_block_write_master(group_id, str(group.master), update_only)
            fail_over = False

    if update_only:
        # Check whether the server is registered or not.
        _retrieve_server(slave_id, group_id)
        _change_to_candidate(group_id, slave_id, update_only)
        return

    if fail_over:
        if not slave_id:
            _events.trigger_within_procedure(FIND_CANDIDATE_FAIL, group_id)
        else:
            _events.trigger_within_procedure(CHECK_CANDIDATE_FAIL, group_id,
                                             slave_id
            )
    else:
        if not slave_id:
            _events.trigger_within_procedure(FIND_CANDIDATE_SWITCH, group_id)
        else:
            _events.trigger_within_procedure(CHECK_CANDIDATE_SWITCH, group_id,
                                             slave_id
            )

# Block any write access to the master.
BLOCK_WRITE_DEMOTE = _events.Event()
# Wait until all slaves synchronize with the master.
WAIT_SLAVES_DEMOTE = _events.Event()
class DemoteMaster(ProcedureGroup):
    """Demote the current master if there is one.

    If users just want to update the state store and skip provisioning steps
    such as configuring replication, the update_only parameter must be set to
    true. Otherwise any write access to the master is blocked, slaves are
    synchronized with the master, stopped and their replication configuration
    reset. Note that no slave is promoted as master.
    """
    group_name = "group"
    command_name = "demote"

    def execute(self, group_id, update_only=False, synchronous=True):
        """Demote the current master if there is one.

        :param uuid: Group's id.
        :param update_only: Only update the state store and skip provisioning.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.

        In what follows, one will find a figure that depicts the sequence of
        event that happen during the demote operation. To ease the presentation
        some names are abbreivated:

        .. seqdiag::

          diagram {
            activation = none;
            === Schedule "block_write" ===
            demote --> executor [ label = "schedule(block_write)" ];
            demote <-- executor;
            === Execute "block_write" and schedule "wait_slaves" ===
            executor -> block_write [ label = "execute(block_write)" ];
            block_write --> executor [ label = "schedule(wait_slaves)" ];
            block_write <-- executor;
            executor <- block_write;
            === Execute "wait_slaves" ===
            executor -> wait_slaves [ label = "execute(wait_slaves)" ];
            wait_slaves --> executor;
            wait_slaves <-- executor;
            executor <- wait_slaves;
          }
        """
        procedures = _events.trigger(
            BLOCK_WRITE_DEMOTE, self.get_lockable_objects(), group_id,
            update_only
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(FIND_CANDIDATE_SWITCH)
def _find_candidate_switch(group_id):
    """Find the best slave to replace the current master.
    """
    slave_uuid = _do_find_candidate(group_id, FIND_CANDIDATE_SWITCH)
    _events.trigger_within_procedure(CHECK_CANDIDATE_SWITCH, group_id,
                                     slave_uuid)

def _do_find_candidate(group_id, event):
    """Find the best candidate in a group that may be used to replace the
    current master if there is any.

    It chooses the slave that has processed more transactions and may become a
    master, e.g. has the binary log enabled. This function does not consider
    purged transactions and delays in the slave while picking up a slave.

    :param group_id: Group's id from where a candidate will be chosen.
    :return: Return the uuid of the best candidate to become a master in the
             group.
    """
    forbidden_status = (_server.MySQLServer.FAULTY, _server.MySQLServer.SPARE)
    group = _server.Group.fetch(group_id)

    master_uuid = None
    if group.master:
        master_uuid = str(group.master)

    chosen_uuid = None
    chosen_gtid_status = None
    for candidate in group.servers():
        if master_uuid != str(candidate.uuid) and \
            candidate.status not in forbidden_status:
            try:
                candidate.connect()
                gtid_status = candidate.get_gtid_status()
                master_issues, why_master_issues = \
                    _replication.check_master_issues(candidate)
                slave_issues = False
                why_slave_issues = {}
                if event == FIND_CANDIDATE_SWITCH:
                    slave_issues, why_slave_issues = \
                        _replication.check_slave_issues(candidate)
                has_valid_master = (master_uuid is None or \
                    _replication.slave_has_master(candidate) == master_uuid)
                can_become_master = False
                if chosen_gtid_status:
                    n_trans = 0
                    try:
                        n_trans = _replication.get_slave_num_gtid_behind(
                            candidate, chosen_gtid_status
                            )
                    except _errors.InvalidGtidError:
                        pass
                    if n_trans == 0 and not master_issues and \
                        has_valid_master and not slave_issues:
                        chosen_gtid_status = gtid_status
                        chosen_uuid = str(candidate.uuid)
                        can_become_master = True
                elif not master_issues and has_valid_master and \
                    not slave_issues:
                    chosen_gtid_status = gtid_status
                    chosen_uuid = str(candidate.uuid)
                    can_become_master = True
                if not can_become_master:
                    _LOGGER.warning(
                        "Candidate (%s) cannot become a master due to the "
                        "following reasons: issues to become a "
                        "master (%s), prerequistes as a slave (%s), valid "
                        "master (%s).", candidate.uuid, why_master_issues,
                        why_slave_issues, has_valid_master
                        )
            except _errors.DatabaseError as error:
                _LOGGER.warning(
                    "Error accessing candidate (%s): %s.", candidate.uuid,
                    error
                )

    if not chosen_uuid:
        raise _errors.GroupError(
            "There is no valid candidate that can be automatically "
            "chosen in group (%s). Please, choose one manually." %
            (group_id, )
        )
    return chosen_uuid

@_events.on_event(CHECK_CANDIDATE_SWITCH)
def _check_candidate_switch(group_id, slave_id):
    """Check if the candidate has all the features to become the new
    master.
    """
    allowed_status = (_server.MySQLServer.SECONDARY, _server.MySQLServer.SPARE)
    group = _server.Group.fetch(group_id)

    if not group.master:
        raise _errors.GroupError(
            "Group (%s) does not contain a valid "
            "master. Please, run a promote or failover." % (group_id, )
        )

    slave = _retrieve_server(slave_id, group_id)
    slave.connect()

    if group.master == slave.uuid:
        raise _errors.ServerError(
            "Candidate slave (%s) is already master." % (slave_id, )
            )

    master_issues, why_master_issues = _replication.check_master_issues(slave)
    if master_issues:
        raise _errors.ServerError(
            "Server (%s) is not a valid candidate slave "
            "due to the following reason(s): (%s)." %
            (slave.uuid, why_master_issues)
            )

    slave_issues, why_slave_issues = _replication.check_slave_issues(slave)
    if slave_issues:
        raise _errors.ServerError(
            "Server (%s) is not a valid candidate slave "
            "due to the following reason: (%s)." %
            (slave.uuid, why_slave_issues)
            )

    master_uuid = _replication.slave_has_master(slave)
    if master_uuid is None or group.master != _uuid.UUID(master_uuid):
        raise _errors.GroupError(
            "The group's master (%s) is different from the candidate's "
            "master (%s)." % (group.master, master_uuid)
            )

    if slave.status not in allowed_status:
        raise _errors.ServerError("Server (%s) is faulty." % (slave_id, ))

    _events.trigger_within_procedure(
        BLOCK_WRITE_SWITCH, group_id, master_uuid, str(slave.uuid)
        )

@_events.on_event(BLOCK_WRITE_SWITCH)
def _block_write_switch(group_id, master_uuid, slave_uuid):
    """Block and disable write access to the current master.
    """
    _do_block_write_master(group_id, master_uuid)
    _events.trigger_within_procedure(WAIT_SLAVES_SWITCH, group_id,
        master_uuid, slave_uuid
        )

def _do_block_write_master(group_id, master_uuid, update_only=False):
    """Block and disable write access to the current master.

    Note that connections are not killed and blocking the master
    may take some time.
    """
    master = _server.MySQLServer.fetch(_uuid.UUID(master_uuid))
    assert(master.status == _server.MySQLServer.PRIMARY)
    master.mode = _server.MySQLServer.READ_ONLY
    master.status = _server.MySQLServer.SECONDARY

    if not update_only:
        master.connect()
        _utils.set_read_only(master, True)

    # Temporarily unset the master in this group.
    group = _server.Group.fetch(group_id)
    _set_group_master_replication(group, None)

    # At the end, we notify that a server was demoted.
    # Any function that implements this event should not
    # run any action that updates Fabric. The event was
    # designed to trigger external actions such as:
    #
    # . Updating an external entity.
    #
    # . Fencing off a server.
    _events.trigger("SERVER_DEMOTED", set([group_id]),
        group_id, str(master.uuid)
    )

@_events.on_event(WAIT_SLAVES_SWITCH)
def _wait_slaves_switch(group_id, master_uuid, slave_uuid):
    """Synchronize candidate with master and also all the other slaves.

    Note that this can be optimized as one may determine the set of
    slaves that must be synchronized with the master.
    """
    master = _server.MySQLServer.fetch(_uuid.UUID(master_uuid))
    master.connect()
    slave = _server.MySQLServer.fetch(_uuid.UUID(slave_uuid))
    slave.connect()

    _utils.synchronize(slave, master)
    _do_wait_slaves_catch(group_id, master, [slave_uuid])

    _events.trigger_within_procedure(CHANGE_TO_CANDIDATE, group_id, slave_uuid)

def _do_wait_slaves_catch(group_id, master, skip_servers=None):
    """Synchronize slaves with master.
    """
    skip_servers = skip_servers or []
    skip_servers.append(str(master.uuid))

    group = _server.Group.fetch(group_id)
    for server in group.servers():
        if str(server.uuid) not in skip_servers:
            try:
                server.connect()
                used_master_uuid = _replication.slave_has_master(server)
                if  str(master.uuid) == used_master_uuid:
                    _utils.synchronize(server, master)
                else:
                    _LOGGER.debug("Slave (%s) has a different master "
                        "from group (%s).", server.uuid, group_id)
            except _errors.DatabaseError as error:
                _LOGGER.debug(
                    "Error synchronizing slave (%s): %s.", server.uuid,
                    error
                )

@_events.on_event(CHANGE_TO_CANDIDATE)
def _change_to_candidate(group_id, master_uuid, update_only=False):
    """Switch to candidate slave.
    """
    forbidden_status = (_server.MySQLServer.FAULTY, )
    master = _server.MySQLServer.fetch(_uuid.UUID(master_uuid))
    master.mode = _server.MySQLServer.READ_WRITE
    master.status = _server.MySQLServer.PRIMARY

    if not update_only:
        # Prepare the server to be the master
        master.connect()
        _utils.reset_slave(master)
        _utils.set_read_only(master, False)

    group = _server.Group.fetch(group_id)
    _set_group_master_replication(group, master.uuid, update_only)

    if not update_only:
        # Make slaves point to the master.
        for server in group.servers():
            if server.uuid != _uuid.UUID(master_uuid) and \
                server.status not in forbidden_status:
                try:
                    server.connect()
                    _utils.switch_master(server, master)
                except _errors.DatabaseError as error:
                    _LOGGER.debug(
                        "Error configuring slave (%s): %s.", server.uuid, error
                    )

    # At the end, we notify that a server was promoted.
    # Any function that implements this event should not
    # run any action that updates Fabric. The event was
    # designed to trigger external actions such as:
    #
    # . Updating an external entity.
    _events.trigger("SERVER_PROMOTED", set([group_id]),
        group_id, master_uuid
    )

@_events.on_event(FIND_CANDIDATE_FAIL)
def _find_candidate_fail(group_id):
    """Find the best candidate to replace the failed master.
    """
    slave_uuid = _do_find_candidate(group_id, FIND_CANDIDATE_FAIL)
    _events.trigger_within_procedure(CHECK_CANDIDATE_FAIL, group_id,
                                     slave_uuid)

@_events.on_event(CHECK_CANDIDATE_FAIL)
def _check_candidate_fail(group_id, slave_id):
    """Check if the candidate has all the prerequisites to become the new
    master.
    """
    allowed_status = (_server.MySQLServer.SECONDARY, _server.MySQLServer.SPARE)
    group = _server.Group.fetch(group_id)

    slave = _retrieve_server(slave_id, group_id)
    slave.connect()

    if group.master == slave.uuid:
        raise _errors.ServerError(
            "Candidate slave (%s) is already master." % (slave_id, )
            )

    master_issues, why_master_issues = _replication.check_master_issues(slave)
    if master_issues:
        raise _errors.ServerError(
            "Server (%s) is not a valid candidate slave "
            "due to the following reason(s): (%s)." %
            (slave.uuid, why_master_issues)
            )

    if slave.status not in allowed_status:
        raise _errors.ServerError("Server (%s) is faulty." % (slave_id, ))

    _events.trigger_within_procedure(WAIT_SLAVE_FAIL, group_id, str(slave.uuid))

@_events.on_event(WAIT_SLAVE_FAIL)
def _wait_slave_fail(group_id, slave_uuid):
    """Wait until a slave processes its backlog.
    """
    slave = _server.MySQLServer.fetch(_uuid.UUID(slave_uuid))
    slave.connect()

    try:
        _utils.process_slave_backlog(slave)
    except _errors.DatabaseError as error:
        _LOGGER.warning(
            "Error trying to process transactions in the relay log "
            "for candidate (%s): %s.", slave, error
        )

    _events.trigger_within_procedure(CHANGE_TO_CANDIDATE, group_id, slave_uuid)

@_events.on_event(BLOCK_WRITE_DEMOTE)
def _block_write_demote(group_id, update_only):
    """Block and disable write access to the current master.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    if not group.master:
        raise _errors.GroupError("Group (%s) does not have a master." %
                                 (group_id, ))

    master = _server.MySQLServer.fetch(group.master)
    assert(master.status in \
        (_server.MySQLServer.PRIMARY, _server.MySQLServer.FAULTY)
    )

    if master.status == _server.MySQLServer.PRIMARY:
        master.connect()
        master.mode = _server.MySQLServer.READ_ONLY
        master.status = _server.MySQLServer.SECONDARY
        _utils.set_read_only(master, True)

        if not update_only:
            _events.trigger_within_procedure(
                WAIT_SLAVES_DEMOTE, group_id, str(master.uuid)
            )

    _set_group_master_replication(group, None, update_only)

@_events.on_event(WAIT_SLAVES_DEMOTE)
def _wait_slaves_demote(group_id, master_uuid):
    """Synchronize slaves with master.
    """
    master = _server.MySQLServer.fetch(_uuid.UUID(master_uuid))
    master.connect()

    # Synchronize slaves.
    _do_wait_slaves_catch(group_id, master)

    # Stop replication threads at all slaves.
    group = _server.Group.fetch(group_id)
    for server in group.servers():
        try:
            server.connect()
            _utils.stop_slave(server)
        except _errors.DatabaseError as error:
            _LOGGER.debug(
                "Error waiting for slave (%s) to stop: %s.", server.uuid,
                error
            )

def _set_group_master_replication(group, server_id, update_only=False):
    """Set the master for the given group and also reset the
    replication with the other group masters. Any change of master
    for a group will be initiated through this method. The method also
    takes care of resetting the master and the slaves that are registered
    with this group to connect with the new master.

    The idea is that operations like switchover, failover, promote all are
    finally master changing operations. Hence the right place to handle
    these operations is at the place where the master is being changed.

    The following operations need to be done

    - Stop the slave on the old master
    - Stop the slaves replicating from the old master
    - Start the slave on the new master
    - Start the slaves with the new master

    :param group: The group whose master needs to be changed.
    :param server_id: The server id of the server that is becoming the master.
    :param update_only: Only update the state store and skip provisioning.
    """
    # Set the new master if update-only is true.
    if update_only:
        group.master = server_id
        return

    try:
        # Otherwise, stop the slave running on the current master
        if group.master_group_id is not None and group.master is not None:
            _group_replication.stop_group_slave(
                group.master_group_id, group.group_id, False
            )
        # Stop the Groups replicating from the current group.
        _group_replication.stop_group_slaves(group.group_id)
    except (_errors.GroupError, _errors.DatabaseError) as error:
        _LOGGER.error(
            "Error accessing groups related to (%s): %s.", group.group_id,
            error
        )

    # Set the new master
    group.master = server_id

    try:
        # If the master is not None setup the master and the slaves.
        if group.master is not None:
            # Start the slave groups for this group.
            _group_replication.start_group_slaves(group.group_id)
            if group.master_group_id is not None:
                # Start the slave on this group
                _group_replication.setup_group_replication(
                    group.master_group_id, group.group_id
                )
    except (_errors.GroupError, _errors.DatabaseError) as error:
        _LOGGER.error(
            "Error accessing groups related to (%s): %s.", group.group_id,
            error
        )
