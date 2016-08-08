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

"""This module contains the functionality used to setup relication between
the masters of Groups. The module contains utility methods for doing the
following

- Setting up replication between two groups - setup_group_replication
- Starting the slaves of a particular group - start_group_slaves
- Stopping the slaves of a particular group - stop_group_slaves
- Stopping a given group that is a slave - stop_group_slave

Note that each group is aware of the master group it replicates from and also
of the slave groups that replicate from it. This information helps in performing
clean up during the event of a "master changing operation" in the group.

As a design alternative these methods could have have been moved into the
mysql.fabric.server.py module or into the mysql.py.replication.py module. But
this would have resulted in tight coupling between the HA and the Server layers
and also would have resulted in a circular dependency, since the replication
module already has a reference to the server module.
"""
import logging

from mysql.fabric.server import Group,  MySQLServer
import mysql.fabric.replication as _replication
import mysql.fabric.errors as _errors

_LOGGER = logging.getLogger(__name__)

GROUP_REPLICATION_GROUP_NOT_FOUND_ERROR = "Group not found %s"
GROUP_REPLICATION_GROUP_MASTER_NOT_FOUND_ERROR = "Group master not found %s"
GROUP_MASTER_NOT_RUNNING = "Group master not running %s"
GROUP_REPLICATION_SERVER_ERROR = \
    "Error accessing server (%s) while configuring group replication. %s."

def start_group_slaves(master_group_id):
    """Start the slave groups for the given master group. The
    method will be used in the events that requires, a group, that
    has registered slaves to start them. An example would be
    enable shard, enable shard requires that a group start all
    the slaves that are registered with it.

    :param master_group_id: The master group ID. The ID belongs to the master
                            whose slaves need to be started.
    """
    # Fetch the master group corresponding to the master group
    # ID.
    master_group = Group.fetch(master_group_id)
    if master_group is None:
        raise _errors.GroupError(GROUP_REPLICATION_GROUP_NOT_FOUND_ERROR % \
                                                (master_group_id, ))

    # Setup replication with masters of the groups registered as master
    # groups. master_group.slave_group_ids contains the list of the group
    # IDs that are slaves to this master. Iterate through this list and start
    # replication with the registered slaves.
    for slave_group_id in master_group.slave_group_ids:
        slave_group = Group.fetch(slave_group_id)
        # Setup replication with the slave group.
        try:
            setup_group_replication(master_group_id, slave_group.group_id)
        except (_errors.GroupError, _errors.DatabaseError) as error:
            _LOGGER.warning(
                "Error while configuring group replication between "
                "(%s) and (%s): (%s).", master_group_id, slave_group.group_id,
                error
            )

def stop_group_slaves(master_group_id):
    """Stop the group slaves for the given master group. This will be used
    for use cases that required all the slaves replicating from this group to
    be stopped. An example use case would be disabling a shard.

    :param master_group_id: The master group ID.
    """
    master_group = Group.fetch(master_group_id)
    if master_group is None:
        raise _errors.GroupError \
        (GROUP_REPLICATION_GROUP_NOT_FOUND_ERROR % \
        (master_group_id, ))

    # Stop the replication on all of the registered slaves for the group.
    for slave_group_id in master_group.slave_group_ids:

        slave_group = Group.fetch(slave_group_id)
        # Fetch the Slave Group and the master of the Slave Group
        slave_group_master = MySQLServer.fetch(slave_group.master)
        if slave_group_master is None:
            _LOGGER.warning(GROUP_REPLICATION_GROUP_MASTER_NOT_FOUND_ERROR,
                            slave_group.master)
            continue

        if not server_running(slave_group_master):
            # The server is already down. we cannot connect to it to stop
            # replication.
            continue

        try:
            slave_group_master.connect()
            _replication.stop_slave(slave_group_master, wait=True)
            # Reset the slave to remove the reference of the master so
            # that when the server is used as a slave next it does not
            # complaint about having a different master.
            _replication.reset_slave(slave_group_master, clean=True)
        except _errors.DatabaseError as error:
            # Server is not accessible, unable to connect to the server.
            _LOGGER.warning(
                "Error while unconfiguring group replication between "
                "(%s) and (%s): (%s).", master_group_id, slave_group.group_id,
                error
            )
            continue

def stop_group_slave(group_master_id,  group_slave_id,  clear_ref):
    """Stop the slave on the slave group. This utility method is the
    completement of the setup_group_replication method and is
    used to stop the replication on the slave group. Given a master group ID
    and the slave group ID the method stops the slave on the slave
    group and updates the references on both the master and the
    slave group.

    :param group_master_id: The id of the master group.
    :param group_slave_id: The id of the slave group.
    :param clear_ref: The parameter indicates if the stop_group_slave
                                needs to clear the references to the group's
                                slaves. For example when you do a disable
                                shard the shard group still retains the
                                references to its slaves, since when enabled
                                it needs to enable the replication.
    """
    master_group = Group.fetch(group_master_id)
    slave_group = Group.fetch(group_slave_id)

    if master_group is None:
        raise _errors.GroupError \
        (GROUP_REPLICATION_GROUP_NOT_FOUND_ERROR % (group_master_id, ))

    if slave_group is None:
        raise _errors.GroupError \
        (GROUP_REPLICATION_GROUP_NOT_FOUND_ERROR % (group_slave_id, ))

    slave_group_master = MySQLServer.fetch(slave_group.master)
    if slave_group_master is None:
        raise _errors.GroupError \
        (GROUP_REPLICATION_GROUP_MASTER_NOT_FOUND_ERROR %
          (slave_group.master, ))

    if not server_running(slave_group_master):
        #The server is already down. We cannot connect to it to stop
        #replication.
        return
    try:
        slave_group_master.connect()
    except _errors.DatabaseError:
        #Server is not accessible, unable to connect to the server.
        return

    #Stop replication on the master of the group and clear the references,
    #if clear_ref has been set.
    _replication.stop_slave(slave_group_master, wait=True)
    _replication.reset_slave(slave_group_master,  clean=True)
    if clear_ref:
        slave_group.remove_master_group_id()
        master_group.remove_slave_group_id(group_slave_id)

def setup_group_replication(group_master_id,  group_slave_id):
    """Sets up replication between the masters of the two groups and
    updates the references to the groups in each other.

    :param group_master_id: The group whose master will act as the master
                                             in the replication setup.
    :param group_slave_id: The group whose master will act as the slave in the
                                      replication setup.
    """
    group_master = Group.fetch(group_master_id)
    group_slave = Group.fetch(group_slave_id)

    if group_master is None:
        raise _errors.GroupError \
        (GROUP_REPLICATION_GROUP_NOT_FOUND_ERROR % (group_master_id, ))

    if group_slave is None:
        raise _errors.GroupError \
        (GROUP_REPLICATION_GROUP_NOT_FOUND_ERROR % (group_slave_id, ))

    if group_master.master is None:
        raise _errors.GroupError \
        (GROUP_REPLICATION_GROUP_MASTER_NOT_FOUND_ERROR % "")

    if group_slave.master is None:
        raise _errors.GroupError \
        (GROUP_REPLICATION_GROUP_MASTER_NOT_FOUND_ERROR % "")

    #Master is the master of the Global Group. We replicate from here to
    #the masters of all the shard Groups.
    master = MySQLServer.fetch(group_master.master)
    if master is None:
        raise _errors.GroupError \
        (GROUP_REPLICATION_GROUP_MASTER_NOT_FOUND_ERROR % \
        (group_master.master, ))

    #Get the master of the shard Group.
    slave = MySQLServer.fetch(group_slave.master)
    if slave is None:
        raise _errors.GroupError \
        (GROUP_REPLICATION_GROUP_MASTER_NOT_FOUND_ERROR % \
        (group_slave.master, ))

    if not server_running(master):
        #The server is already down. We cannot connect to it to setup
        #replication.
        raise _errors.GroupError \
        (GROUP_MASTER_NOT_RUNNING % (group_master.group_id, ))

    try:
        master.connect()
    except _errors.DatabaseError as error: 
        #Server is not accessible, unable to connect to the server.
        raise _errors.GroupError(
            GROUP_REPLICATION_SERVER_ERROR %  (group_slave.master, error)
        )

    if not server_running(slave):
        #The server is already down. We cannot connect to it to setup
        #replication.
        raise _errors.GroupError \
            (GROUP_MASTER_NOT_RUNNING % (group_slave.group_id, ))

    try:
        slave.connect()
    except _errors.DatabaseError as error:
        raise _errors.GroupError(
            GROUP_REPLICATION_SERVER_ERROR %  (group_master.master, error)
        )

    _replication.stop_slave(slave, wait=True)

    #clear references to old masters in the slave
    _replication.reset_slave(slave,  clean=True)

    _replication.switch_master(slave, master, master.user, master.passwd)

    _replication.start_slave(slave, wait=True)

    try:
        group_master.add_slave_group_id(group_slave_id)
        group_slave.add_master_group_id(group_master_id)
    except _errors.DatabaseError:
        #If there is an error while adding a reference to
        #the slave group or a master group, it means that
        #the slave group was already added and the error
        #is happening because the group was already registered.
        #Ignore this error.
        pass

def server_running(server):
    """Check if the server is in the running state.

    :param server: The MySQLServer object whose mode needs to be checked.

    :return: True if server is in the running state.
             False otherwise.
    """
    if server.status in [MySQLServer.FAULTY]:
        return False
    return True
