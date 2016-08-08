#
# Copyright (c) 2014,2015, Oracle and/or its affiliates. All rights reserved.
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

"""This module provides the necessary interfaces to perform re-sharding in
fabric. The module takes care of the shard move, shard split and the shard
prune operations.
"""

import logging
import time

from mysql.connector.errorcode import (
    ER_NO_SUCH_TABLE,
)

from mysql.fabric import (
    errors as _errors,
    events as _events,
    group_replication as _group_replication,
    replication as _replication,
    backup as _backup,
    utils as _utils,
    config as _config,
)

from mysql.fabric.server import (
    Group,
    MySQLServer,
)

from mysql.fabric.sharding import (
    ShardMapping,
    RangeShardingSpecification,
    HashShardingSpecification,
    Shards,
    SHARDING_DATATYPE_HANDLER,
    SHARDING_SPECIFICATION_HANDLER,
)

from mysql.fabric.command import (
    ProcedureShard,
)

from mysql.fabric.services import (
    sharding as _services_sharding,
    utils as _services_utils,
)

_LOGGER = logging.getLogger(__name__)

PRUNE_SHARD_TABLES = _events.Event("PRUNE_SHARD_TABLES")
class PruneShardTables(ProcedureShard):
    """Given the table name prune the tables according to the defined
    sharding specification for the table.
    """
    group_name = "sharding"
    command_name = "prune_shard"
    def execute(self, table_name, synchronous=True):
        """Given the table name prune the tables according to the defined
        sharding specification for the table. The command prunes all the
        tables that are part of this shard. There might be multiple tables that
        are part of the same shard, these tables will be related together by
        the same sharding key.

        :param table_name: The table that needs to be sharded.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """
        prune_limit =   _services_utils.read_config_value(
                                self.config,
                                'sharding',
                                'prune_limit'
                            )
        procedures = _events.trigger(
            PRUNE_SHARD_TABLES,
            self.get_lockable_objects(),
            table_name,
            prune_limit
        )
        return self.wait_for_procedures(procedures, synchronous)

CHECK_SHARD_INFORMATION = _events.Event("CHECK_SHARD_INFORMATION")
BACKUP_SOURCE_SHARD = _events.Event("BACKUP_SOURCE_SHARD")
RESTORE_SHARD_BACKUP = _events.Event("RESTORE_SHARD_BACKUP")
SETUP_REPLICATION = _events.Event("SETUP_REPLICATION")
SETUP_SYNC = _events.Event("SETUP_SYNC")
SETUP_RESHARDING_SWITCH = _events.Event("SETUP_RESHARDING_SWITCH")
PRUNE_SHARDS = _events.Event("PRUNE_SHARDS")
class MoveShardServer(ProcedureShard):
    """Move the shard represented by the shard_id to the destination group.

    By default this operation takes a backup, restores it on the destination
    group and guarantees that source and destination groups are synchronized
    before pointing the shard to the new group. If users just want to update
    the state store and skip these provisioning steps, the update_only
    parameter must be set to true.
    """
    group_name = "sharding"
    command_name = "move_shard"
    def execute(self, shard_id, group_id, update_only=False,
                synchronous=True):
        """Move the shard represented by the shard_id to the destination group.

        :param shard_id: The ID of the shard that needs to be moved.
        :param group_id: The ID of the group to which the shard needs to
                         be moved.
        :update_only: Only update the state store and skip provisioning.
        :param synchronous: Whether one should wait until the execution finishes
                        or not.
        """

        procedures = _events.trigger(
            CHECK_SHARD_INFORMATION, self.get_lockable_objects(), shard_id,
            group_id, None, "", "MOVE", update_only
        )
        return self.wait_for_procedures(procedures, synchronous)

class SplitShardServer(ProcedureShard):
    """Split the shard represented by the shard_id into the destination group.

    By default this operation takes a backup, restores it on the destination
    group and guarantees that source and destination groups are synchronized
    before pointing the shard to the new group. If users just want to update
    the state store and skip these provisioning steps, the update_only
    parameter must be set to true.

    """
    group_name = "sharding"
    command_name = "split_shard"
    def execute(self, shard_id, group_id, split_value=None,
                update_only=False, synchronous=True):
        """Split the shard represented by the shard_id into the destination
        group.

        :param shard_id: The shard_id of the shard that needs to be split.
        :param group_id: The ID of the group into which the split data needs
                         to be moved.
        :param split_value: The value at which the range needs to be split.
        :update_only: Only update the state store and skip provisioning.
        :param synchronous: Whether one should wait until the execution
                            finishes
        """
        prune_limit =   _services_utils.read_config_value(
                                self.config,
                                'sharding',
                                'prune_limit'
                            )
        procedures = _events.trigger(
            CHECK_SHARD_INFORMATION, self.get_lockable_objects(), shard_id,
            group_id, split_value, prune_limit, "SPLIT", update_only
        )
        return self.wait_for_procedures(procedures, synchronous)


@_events.on_event(PRUNE_SHARD_TABLES)
def _prune_shard_tables(table_name, prune_limit):
    """Delete the data from the copied data directories based on the
    sharding configuration uploaded in the sharding tables of the state
    store. The basic logic consists of

    a) Querying the sharding scheme name corresponding to the sharding table
    b) Querying the sharding key range using the sharding scheme name.
    c) Deleting the sharding keys that fall outside the range for a given
        server.

    :param table_name: The table_name who's shards need to be pruned.
    :param prune_limit: The number of DELETEs that should be
                    done in one batch.
    """
    shard_mapping = ShardMapping.fetch(table_name)
    try:
        SHARDING_SPECIFICATION_HANDLER[shard_mapping.type_name].\
            delete_from_shard_db(table_name, shard_mapping.type_name, prune_limit)
    except _errors.DatabaseError as error:
        if error.errno == ER_NO_SUCH_TABLE:
            #Error happens because the actual tables are not present in the
            #server. We will ignore this.
            pass
        else:
            raise error

@_events.on_event(CHECK_SHARD_INFORMATION)
def _check_shard_information(shard_id, destn_group_id,
                             split_value, prune_limit, cmd, update_only):
    """Verify the sharding information before starting a re-sharding operation.

    :param shard_id: The destination shard ID.
    :param destn_group_id: The Destination group ID.
    :param split_value: The point at which the sharding definition
                        should be split.
    :param prune_limit: The number of DELETEs that should be
                        done in one batch.
    :param cmd: Indicates if it is a split or a move being executed.
    :param update_only: If the operation is a update only operation.
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
    mysqldump_binary = _services_utils.read_config_value(
                            _config.global_config,
                            'sharding',
                            'mysqldump_program'
                        )
    mysqlclient_binary = _services_utils.read_config_value(
                            _config.global_config,
                            'sharding',
                            'mysqlclient_program'
                        )

    if not _services_utils.is_valid_binary(mysqldump_binary):
        raise _errors.ShardingError(
                _services_sharding.MYSQLDUMP_NOT_FOUND % mysqldump_binary)

    if not _services_utils.is_valid_binary(mysqlclient_binary):
        raise _errors.ShardingError(
                _services_sharding.MYSQLCLIENT_NOT_FOUND % mysqlclient_binary)

    if cmd == "SPLIT":
        range_sharding_spec, _, shard_mappings, _ = \
            _services_sharding.verify_and_fetch_shard(shard_id)
        upper_bound = \
            SHARDING_SPECIFICATION_HANDLER[shard_mappings[0].type_name].\
                        get_upper_bound(
                            range_sharding_spec.lower_bound,
                            range_sharding_spec.shard_mapping_id,
                            shard_mappings[0].type_name
                          )
        #If the underlying sharding scheme is a HASH. When a shard is split,
        #all the tables that are part of the shard, have the same sharding
        #scheme. All the shard mappings associated with this shard_id will be
        #of the same sharding type. Hence it is safe to use one of the shard
        #mappings.
        if shard_mappings[0].type_name == "HASH":
            if split_value is not None:
                raise _errors.ShardingError(
                    _services_sharding.NO_LOWER_BOUND_FOR_HASH_SHARDING
                )
            if  upper_bound is None:
                #While splitting a range, retrieve the next upper bound and
                #find the mid-point, in the case where the next upper_bound
                #is unavailable pick the maximum value in the set of values in
                #the shard.
                upper_bound = HashShardingSpecification.fetch_max_key(shard_id)

            #Calculate the split value.
            split_value = \
                SHARDING_DATATYPE_HANDLER[shard_mappings[0].type_name].\
                split_value(
                    range_sharding_spec.lower_bound,
                    upper_bound
                )
        elif split_value is not None:
            if not (SHARDING_DATATYPE_HANDLER[shard_mappings[0].type_name].\
                    is_valid_split_value(
                        split_value, range_sharding_spec.lower_bound,
                        upper_bound
                    )
                ):
                raise _errors.ShardingError(
                    _services_sharding.INVALID_LOWER_BOUND_VALUE %
                    (split_value, )
                )
        elif split_value is None:
            raise _errors.ShardingError(
                _services_sharding.SPLIT_VALUE_NOT_DEFINED
            )

    #Ensure that the group does not already contain a shard.
    if Shards.lookup_shard_id(destn_group_id) is not None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_MOVE_DESTINATION_NOT_EMPTY %
            (destn_group_id, )
        )

    #Fetch the group information for the source shard that
    #needs to be moved.
    source_shard = Shards.fetch(shard_id)
    if source_shard is None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_NOT_FOUND % (shard_id, ))

    #Fetch the group_id and the group that hosts the source shard.
    source_group_id = source_shard.group_id

    destn_group = Group.fetch(destn_group_id)
    if destn_group is None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_GROUP_NOT_FOUND %
            (destn_group_id, ))

    if not update_only:
        # Check if the source server has backup privileges.
        source_group = Group.fetch(source_group_id)
        server = _services_utils.fetch_backup_server(source_group)
        server.user = backup_user
        server.passwd = backup_passwd
        _backup.MySQLDump.check_backup_privileges(server)

        # Check if the destination server has restore privileges.
        destination_group = Group.fetch(destn_group_id)
        server = MySQLServer.fetch(destination_group.master)
        server.user = restore_user
        server.passwd = restore_passwd
        _backup.MySQLDump.check_restore_privileges(server)

        _events.trigger_within_procedure(
            BACKUP_SOURCE_SHARD, shard_id, source_group_id, destn_group_id,
            split_value, prune_limit, cmd, update_only
        )
    else:
        _events.trigger_within_procedure(
            SETUP_RESHARDING_SWITCH, shard_id, source_group_id, destn_group_id,
            split_value, prune_limit, cmd, update_only
        )

@_events.on_event(BACKUP_SOURCE_SHARD)
def _backup_source_shard(shard_id, source_group_id, destn_group_id,
                         split_value, prune_limit, cmd, update_only):
    """Backup the source shard.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                           be moved.
    :param split_value: Indicates the value at which the range for the
                        particular shard will be split. Will be set only
                        for shard split operations.
    :param prune_limit: The number of DELETEs that should be
                        done in one batch.
    :param cmd: Indicates the type of re-sharding operation (move, split)
    :update_only: Only update the state store and skip provisioning.
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

    source_group = Group.fetch(source_group_id)
    move_source_server = _services_utils.fetch_backup_server(source_group)

    #Do the backup of the group hosting the source shard.
    backup_image = _backup.MySQLDump.backup(
                        move_source_server,
                        backup_user, backup_passwd,
                        mysqldump_binary
                    )

    #Change the master for the server that is master of the group which hosts
    #the destination shard.
    _events.trigger_within_procedure(
                                     RESTORE_SHARD_BACKUP,
                                     shard_id,
                                     source_group_id,
                                     destn_group_id,
                                     backup_image.path,
                                     split_value,
                                     prune_limit,
                                     cmd
                                     )

@_events.on_event(RESTORE_SHARD_BACKUP)
def _restore_shard_backup(shard_id, source_group_id, destn_group_id,
                          backup_image, split_value, prune_limit, cmd):
    """Restore the backup on the destination Group.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                           be moved.
    :param backup_image: The destination file that contains the backup
                         of the source shard.
    :param split_value: Indicates the value at which the range for the
                        particular shard will be split. Will be set only
                        for shard split operations.
    :param prune_limit: The number of DELETEs that should be
                        done in one batch.
    :param cmd: Indicates the type of re-sharding operation
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

    destn_group = Group.fetch(destn_group_id)
    if destn_group is None:
        raise _errors.ShardingError(_services_sharding.SHARD_GROUP_NOT_FOUND %
                                    (destn_group_id, ))

    #Build a backup image that will be used for restoring
    bk_img = _backup.BackupImage(backup_image)

    for destn_group_server in destn_group.servers():
        destn_group_server.connect()
        _backup.MySQLDump.restore_fabric_server(
            destn_group_server,
            restore_user, restore_passwd,
            bk_img,
            mysqlclient_binary
        )

    #Setup sync between the source and the destination groups.
    _events.trigger_within_procedure(
                                     SETUP_REPLICATION,
                                     shard_id,
                                     source_group_id,
                                     destn_group_id,
                                     split_value,
                                     prune_limit,
                                     cmd
                                     )

@_events.on_event(SETUP_REPLICATION)
def _setup_replication(shard_id, source_group_id, destn_group_id, split_value,
                                        prune_limit, cmd):
    """Setup replication between the source and the destination groups and
    ensure that they are in sync.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                           be moved.
    :param split_value: Indicates the value at which the range for the
                        particular shard will be split. Will be set only
                        for shard split operations.
    :param prune_limit: The number of DELETEs that should be
                        done in one batch.
    :param cmd: Indicates the type of re-sharding operation
    """
    source_group = Group.fetch(source_group_id)
    if source_group is None:
        raise _errors.ShardingError(_services_sharding.SHARD_GROUP_NOT_FOUND %
                                    (source_group_id, ))

    destination_group = Group.fetch(destn_group_id)
    if destination_group is None:
        raise _errors.ShardingError(_services_sharding.SHARD_GROUP_NOT_FOUND %
                                    (destn_group_id, ))

    master = MySQLServer.fetch(source_group.master)
    if master is None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_GROUP_MASTER_NOT_FOUND)
    master.connect()

    slave = MySQLServer.fetch(destination_group.master)
    if slave is None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_GROUP_MASTER_NOT_FOUND)
    slave.connect()

    #Stop and reset any slave that  might be running on the slave server.
    _replication.stop_slave(slave, wait=True)
    _replication.reset_slave(slave, clean=True)

    #Change the master to the shard group master.
    _replication.switch_master(slave, master, master.user, master.passwd)

    #Start the slave so that syncing of the data begins
    _replication.start_slave(slave, wait=True)

    #Setup sync between the source and the destination groups.
    _events.trigger_within_procedure(
                                     SETUP_SYNC,
                                     shard_id,
                                     source_group_id,
                                     destn_group_id,
                                     split_value,
                                     prune_limit,
                                     cmd
                                     )

@_events.on_event(SETUP_SYNC)
def _setup_sync(shard_id, source_group_id, destn_group_id, split_value,
                                        prune_limit, cmd):

    """sync the source and the destination groups.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                           be moved.
    :param split_value: Indicates the value at which the range for the
                        particular shard will be split. Will be set only
                        for shard split operations.
    :param prune_limit: The number of DELETEs that should be
                        done in one batch.
    :param cmd: Indicates the type of re-sharding operation
    """
    source_group = Group.fetch(source_group_id)
    if source_group is None:
        raise _errors.ShardingError(_services_sharding.SHARD_GROUP_NOT_FOUND %
                                    (source_group_id, ))

    destination_group = Group.fetch(destn_group_id)
    if destination_group is None:
        raise _errors.ShardingError(_services_sharding.SHARD_GROUP_NOT_FOUND %
                                    (destn_group_id, ))

    master = MySQLServer.fetch(source_group.master)
    if master is None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_GROUP_MASTER_NOT_FOUND)
    master.connect()

    slave = MySQLServer.fetch(destination_group.master)
    if slave is None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_GROUP_MASTER_NOT_FOUND)
    slave.connect()

    #Synchronize until the slave catches up with the master.
    _replication.synchronize_with_read_only(slave, master)

    #Reset replication once the syncing is done.
    _replication.stop_slave(slave, wait=True)
    _replication.reset_slave(slave, clean=True)

    #Trigger changing the mappings for the shard that was copied
    _events.trigger_within_procedure(
                                     SETUP_RESHARDING_SWITCH,
                                     shard_id,
                                     source_group_id,
                                     destn_group_id,
                                     split_value,
                                     prune_limit,
                                     cmd
                                     )

@_events.on_event(SETUP_RESHARDING_SWITCH)
def _setup_resharding_switch(shard_id, source_group_id, destination_group_id,
                             split_value, prune_limit, cmd, update_only=False):
    """Setup the shard move or shard split workflow based on the command
    argument.

    :param shard_id: The ID of the shard that needs to be re-sharded.
    :param source_group_id: The ID of the source group.
    :param destination_group_id: The ID of the destination group.
    :param split_value: The value at which the shard needs to be split
                        (in the case of a shard split operation).
    :param prune_limit: The number of DELETEs that should be
                        done in one batch.
    :param cmd: whether the operation that needs to be split is a
                MOVE or a SPLIT operation.
    :update_only: Only update the state store and skip provisioning.
    """
    if cmd == "MOVE":
        _setup_shard_switch_move(
            shard_id, source_group_id, destination_group_id,
            update_only
        )
    elif cmd == "SPLIT":
        _setup_shard_switch_split(
            shard_id, source_group_id, destination_group_id, split_value,
            prune_limit, cmd, update_only
        )

def _setup_shard_switch_split(shard_id, source_group_id, destination_group_id,
                              split_value, prune_limit, cmd, update_only):
    """Setup the moved shard to map to the new group.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                           be moved.
    :param split_value: Indicates the value at which the range for the
                        particular shard will be split. Will be set only
                        for shard split operations.
    :param prune_limit: The number of DELETEs that should be
                        done in one batch.
    :param cmd: Indicates the type of re-sharding operation.
    :update_only: Only update the state store and skip provisioning.
    """
    #Fetch the Range sharding specification.
    range_sharding_spec, source_shard, shard_mappings, shard_mapping_defn = \
            _services_sharding.verify_and_fetch_shard(shard_id)

    #Disable the old shard
    source_shard.disable()

    #Remove the old shard.
    range_sharding_spec.remove()
    source_shard.remove()

    destination_group = Group.fetch(destination_group_id)
    if destination_group is None:
        raise _errors.ShardingError(_services_sharding.SHARD_GROUP_NOT_FOUND %
                                    (destination_group_id, ))
    destn_group_master = MySQLServer.fetch(destination_group.master)
    if destn_group_master is None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_GROUP_MASTER_NOT_FOUND)
    destn_group_master.connect()

    #Make the destination group as read only to disable updates until the
    #connectors update their caches, thus avoiding inconsistency.
    destn_group_master.read_only = True

    #Add the new shards. Generate new shard IDs for the shard being
    #split and also for the shard that is created as a result of the split.
    new_shard_1 = Shards.add(source_shard.group_id, "DISABLED")
    new_shard_2 = Shards.add(destination_group_id, "DISABLED")

    #Both of the shard mappings associated with this shard_id should
    #be of the same sharding type. Hence it is safe to use one of the
    #shard mappings.
    if shard_mappings[0].type_name == "HASH":
        #In the case of a split involving a HASH sharding scheme,
        #the shard that is split gets a new shard_id, while the split
        #gets the new computed lower_bound and also a new shard id.
        #NOTE: How the shard that is split retains its lower_bound.
        HashShardingSpecification.add_hash_split(
            range_sharding_spec.shard_mapping_id,
            new_shard_1.shard_id,
            range_sharding_spec.lower_bound
        )
        HashShardingSpecification.add_hash_split(
            range_sharding_spec.shard_mapping_id,
            new_shard_2.shard_id,
            split_value
        )
    else:
        #Add the new ranges. Note that the shard being split retains
        #its lower_bound, while the new shard gets the computed,
        #lower_bound.
        RangeShardingSpecification.add(
            range_sharding_spec.shard_mapping_id,
            range_sharding_spec.lower_bound,
            new_shard_1.shard_id
        )
        RangeShardingSpecification.add(
            range_sharding_spec.shard_mapping_id,
            split_value,
            new_shard_2.shard_id
        )

    #The sleep ensures that the connector have refreshed their caches with the
    #new shards that have been added as a result of the split.
    time.sleep(_utils.TTL)

    #The source shard group master would have been marked as read only
    #during the sync. Remove the read_only flag.
    source_group = Group.fetch(source_group_id)
    if source_group is None:
        raise _errors.ShardingError(_services_sharding.SHARD_GROUP_NOT_FOUND %
                                    (source_group_id, ))

    source_group_master = MySQLServer.fetch(source_group.master)
    if source_group_master is None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_GROUP_MASTER_NOT_FOUND)
    source_group_master.connect()

    #Kill all the existing connections on the servers
    source_group.kill_connections_on_servers()

    #Allow connections on the source group master
    source_group_master.read_only = False

    #Allow connections on the destination group master
    destn_group_master.read_only = False

    #Setup replication for the new group from the global server
    _group_replication.setup_group_replication \
            (shard_mapping_defn[2], destination_group_id)

    #Enable the split shards
    new_shard_1.enable()
    new_shard_2.enable()

    #Trigger changing the mappings for the shard that was copied
    if not update_only:
        _events.trigger_within_procedure(
            PRUNE_SHARDS, new_shard_1.shard_id, new_shard_2.shard_id, prune_limit
        )

@_events.on_event(PRUNE_SHARDS)
def _prune_shard_tables_after_split(shard_id_1, shard_id_2, prune_limit):
    """Prune the two shards generated after a split.

    :param shard_id_1: The first shard id after the split.
    :param shard_id_2: The second shard id after the split.
    :param prune_limit: The number of DELETEs that should be
                        done in one batch.
    """
    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    _, _, shard_mappings,  _ = _services_sharding.\
        verify_and_fetch_shard(shard_id_1)

    #All the shard mappings associated with this shard_id should be
    #of the same type. Hence it is safe to use one of them.
    try:
        SHARDING_SPECIFICATION_HANDLER[shard_mappings[0].type_name].\
        prune_shard_id(shard_id_1, shard_mappings[0].type_name, prune_limit)
    except _errors.DatabaseError as error:
        if error.errno == ER_NO_SUCH_TABLE:
            #Error happens because the actual tables are not present in the
            #server. We will ignore this.
            pass
        else:
            raise error

    try:
        SHARDING_SPECIFICATION_HANDLER[shard_mappings[0].type_name].\
        prune_shard_id(shard_id_2, shard_mappings[0].type_name, prune_limit)
    except _errors.DatabaseError as error:
        if error.errno == ER_NO_SUCH_TABLE:
            #Error happens because the actual tables are not present in the
            #server. We will ignore this.
            pass
        else:
            raise error

def _setup_shard_switch_move(shard_id, source_group_id, destination_group_id,
                             update_only):
    """Setup the moved shard to map to the new group.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destination_group_id: The ID of the group to which the shard
                                needs to be moved.
    :update_only: Only update the state store and skip provisioning.
    """
    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    _, source_shard, _, shard_mapping_defn = \
        _services_sharding.verify_and_fetch_shard(shard_id)

    destination_group = Group.fetch(destination_group_id)
    if destination_group is None:
        raise _errors.ShardingError(_services_sharding.SHARD_GROUP_NOT_FOUND %
                                    (destination_group_id, ))
    destn_group_master = MySQLServer.fetch(destination_group.master)
    if destn_group_master is None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_GROUP_MASTER_NOT_FOUND)
    destn_group_master.connect()
    #Set the destination group master to read_only
    destn_group_master.read_only = True

    #Setup replication between the shard group and the global group.
    _group_replication.setup_group_replication \
            (shard_mapping_defn[2], destination_group_id)
    #set the shard to point to the new group.
    source_shard.group_id = destination_group_id
    #Stop the replication between the global server and the original
    #group associated with the shard.
    _group_replication.stop_group_slave\
            (shard_mapping_defn[2], source_group_id,  True)

    #The sleep ensures that the connector have refreshed their caches with the
    #new shards that have been added as a result of the split.
    time.sleep(_utils.TTL)

    #Reset the read only flag on the source server.
    source_group = Group.fetch(source_group_id)
    if source_group is None:
        raise _errors.ShardingError(_services_sharding.SHARD_GROUP_NOT_FOUND %
                                    (source_group_id, ))

    master = MySQLServer.fetch(source_group.master)
    if master is None:
        raise _errors.ShardingError(
            _services_sharding.SHARD_GROUP_MASTER_NOT_FOUND)

    if not update_only:
        master.connect()
        master.read_only = False
        #Kill all the existing connections on the servers
        source_group.kill_connections_on_servers()
        #allow updates in the destination group master
        destn_group_master.read_only = False
