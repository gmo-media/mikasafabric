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

""" This module contains functions that are called from the services interface
and change MySQL state. Notice though that after a failure the system does no
undo the changes made through the execution of these functions.
"""
from mysql.fabric import (
    errors as _errors,
    replication as _replication,
    server as _server,
)

CONFIG_NOT_FOUND = "Configuration option not found %s . %s"
GROUP_MASTER_NOT_FOUND = "Group master not found"

def switch_master(slave, master):
    """Make slave point to master.

    :param slave: Slave.
    :param master: Master.
    """
    _replication.stop_slave(slave, wait=True)
    _replication.switch_master(slave, master, master.user, master.passwd)
    slave.read_only = True
    _replication.start_slave(slave, wait=True)


def set_read_only(server, read_only):
    """Set server to read only mode.

    :param read_only: Either read-only or not.
    """
    server.read_only = read_only


def reset_slave(slave):
    """Stop slave and reset it.

    :param slave: slave.
    """
    _replication.stop_slave(slave, wait=True)
    _replication.reset_slave(slave, clean=True)


def process_slave_backlog(slave):
    """Wait until slave processes its backlog.

    :param slave: slave.
    """
    _replication.stop_slave(slave, wait=True)
    _replication.start_slave(slave, threads=("SQL_THREAD", ), wait=True)
    slave_status = _replication.get_slave_status(slave)[0]
    _replication.wait_for_slave(
        slave, slave_status.Master_Log_File, slave_status.Read_Master_Log_Pos
     )


def synchronize(slave, master):
    """Synchronize a slave with a master and after that stop the slave.

    :param slave: Slave.
    :param master: Master.
    """
    _replication.sync_slave_with_master(slave, master, timeout=0)


def stop_slave(slave):
    """Stop slave.

    :param slave: Slave.
    """
    _replication.stop_slave(slave, wait=True)

def read_config_value(config, config_group, config_name):
    """Read the value of the configuration option from the config files.

    :param config: The config class that encapsulates the config parsing
                    logic.
    :param config_group: The configuration group to which the configuration
                        belongs
    :param config_name: The name of the configuration that needs to be read,
    """
    config_value = None

    try:
        config_value = config.get(config_group, config_name)
    except AttributeError:
        pass

    if config_value is None:
        raise _errors.ConfigurationError(CONFIG_NOT_FOUND %
                                        (config_group, config_name))

    return config_value

def is_valid_binary(binary):
    """Prints if the binary was found in the given path.

    :param binary: The full path to the binary that needs to be verified.

    :return True: If the binary was found
        False: If the binary was not found.
    """
    import os
    return os.path.isfile(binary) and os.access(binary, os.X_OK)

def fetch_backup_server(source_group):
    """Fetch a spare, slave or master from a group in that order of
    availability. Find any spare (no criteria), if there is no spare, find
    a secondary(no criteria) and if there is no secondary the master.

    :param source_group: The group from which the server needs to
                         be fetched.
    """
    #Get a slave server whose status is spare.
    backup_server = None
    for server in source_group.servers():
        if server.status == "SPARE":
            backup_server = server

    #If there is no spare check if a running slave is available
    if backup_server is None:
        for server in source_group.servers():
            if source_group.master != server.uuid and \
                server.status == "SECONDARY":
                backup_server = server

    #If there is no running slave just use the master
    if backup_server is None:
        backup_server = _server.MySQLServer.fetch(source_group.master)

    #If there is no master throw an exception
    if backup_server is None:
        raise _errors.ShardingError(GROUP_MASTER_NOT_FOUND)

    return backup_server
