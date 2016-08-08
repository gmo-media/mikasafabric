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

"""This module provides the necessary interfaces for working with the shards
in FABRIC.
"""

import logging

from mysql.fabric import (
    errors as _errors,
    events as _events,
    group_replication as _group_replication,
    utils as _utils,
)

from mysql.fabric.server import (
    Group,
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
    Command,
    ResultSet,
    CommandResult,
)

from mysql.fabric.services.server import (
    ServerLookups,
)

_LOGGER = logging.getLogger(__name__)

#Error messages
INVALID_SHARDING_TYPE = "Invalid Sharding Type %s"
TABLE_NAME_NOT_FOUND = "Table name %s not found"
CANNOT_REMOVE_SHARD_MAPPING = "Cannot remove mapping, while, " \
                                                "shards still exist"
INVALID_SHARD_STATE = "Invalid Shard State %s"
INVALID_SHARDING_RANGE = "Invalid sharding range"
SHARD_MAPPING_NOT_FOUND = "Shard Mapping with shard_mapping_id %s not found"
SHARD_MAPPING_DEFN_NOT_FOUND = "Shard Mapping Definition with "\
    "shard_mapping_id %s not found"
SHARD_NOT_DISABLED = "Shard not disabled"
SHARD_NOT_ENABLED = "Shard not enabled"
INVALID_SHARDING_KEY = "Invalid Key %s"
SHARD_NOT_FOUND = "Shard %s not found"
SHARD_LOCATION_NOT_FOUND = "Shard location not found"
INVALID_SHARDING_HINT = "Unknown lookup hint"
SHARD_GROUP_NOT_FOUND = "Shard group %s not found"
SHARD_GROUP_MASTER_NOT_FOUND = "Shard group master not found"
SHARD_MOVE_DESTINATION_NOT_EMPTY = "Shard move destination %s already "\
    "hosts a shard"
INVALID_SHARD_SPLIT_VALUE = "The chosen split value must be between the " \
                            "lower bound and upper bound of the shard"
INVALID_LOWER_BOUND_VALUE = "Invalid lower_bound value for RANGE sharding " \
    "specification %s"
SHARDS_ALREADY_EXIST = "Shards are already present in the definition, "\
        "use split_shard to create further shards."
LOWER_BOUND_GROUP_ID_COUNT_MISMATCH = "Lower Bound, Group ID pair mismatch "\
                                    "format should be group-id/lower_bound, "\
                                    "group-id/lower_bound...."
LOWER_BOUND_AUTO_GENERATED = "Lower Bounds are auto-generated in hash "\
                            "based sharding"
SPLIT_VALUE_NOT_DEFINED = "Splitting a RANGE shard definition requires a split"\
            " value to be defined"
INVALID_SPLIT_VALUE = "Invalid value given for shard splitting"
NO_LOWER_BOUND_FOR_HASH_SHARDING = "Lower bound should not be specified "\
                                "for hash based sharding"
MYSQLDUMP_NOT_FOUND = "Unable to find MySQLDump in location %s"
MYSQLCLIENT_NOT_FOUND = "Unable to find MySQL Client in location %s"

DEFINE_SHARD_MAPPING = _events.Event("DEFINE_SHARD_MAPPING")
class DefineShardMapping(ProcedureShard):
    """Define a shard mapping.
    """
    group_name = "sharding"
    command_name = "create_definition"
    def execute(self, type_name, group_id, synchronous=True):
        """Define a shard mapping.

        :param type_name: The type of sharding scheme - RANGE, HASH, LIST etc
        :param group_id: Every shard mapping is associated with a global group
                         that stores the global updates and the schema changes
                         for this shard mapping and dissipates these to the
                         shards.
        """
        procedures = _events.trigger(
            DEFINE_SHARD_MAPPING, self.get_lockable_objects(),
            type_name, group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

ADD_SHARD_MAPPING = _events.Event("ADD_SHARD_MAPPING")
class AddShardMapping(ProcedureShard):
    """Add a table to a shard mapping.
    """
    group_name = "sharding"
    command_name = "add_table"
    def execute(self, shard_mapping_id, table_name, column_name,
                synchronous=True):
        """Add a table to a shard mapping.

        :param shard_mapping_id: The shard mapping id to which the input
                                    table is attached.
        :param table_name: The table being sharded.
        :param column_name: The column whose value is used in the sharding
                            scheme being applied
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """

        procedures = _events.trigger(
            ADD_SHARD_MAPPING, self.get_lockable_objects(),
            shard_mapping_id, table_name, column_name
        )
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SHARD_MAPPING = _events.Event("REMOVE_SHARD_MAPPING")
class RemoveShardMapping(ProcedureShard):
    """Remove the shard mapping represented by the Shard Mapping object.
    """
    group_name = "sharding"
    command_name = "remove_table"
    def execute(self, table_name, synchronous=True):
        """Remove the shard mapping corresponding to the table passed as input.
        This method is exposed through the XML-RPC framework and creates a job
        and enqueues it in the executor.

        :param table_name: The name of the table whose sharding specification is
                            being removed.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """
        procedures = _events.trigger(
            REMOVE_SHARD_MAPPING, self.get_lockable_objects(), table_name
        )
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SHARD_MAPPING_DEFN = _events.Event("REMOVE_SHARD_MAPPING_DEFN")
class RemoveShardMappingDefn(ProcedureShard):
    """Remove the shard mapping definition represented by the Shard Mapping
    ID.
    """
    group_name = "sharding"
    command_name = "remove_definition"
    def execute(self, shard_mapping_id, synchronous=True):
        """Remove the shard mapping definition represented by the Shard Mapping
        ID. This method is exposed through the XML-RPC framework and creates a
        job and enqueues it in the executor.

        :param shard_mapping_id: The shard mapping ID of the shard mapping
                                definition that needs to be removed.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """
        procedures = _events.trigger(
            REMOVE_SHARD_MAPPING_DEFN,
            self.get_lockable_objects(),
            shard_mapping_id
        )
        return self.wait_for_procedures(procedures, synchronous)

class LookupShardMapping(Command):
    """Fetch the shard specification mapping for the given table
    """
    group_name = "sharding"
    command_name = "lookup_table"
    def execute(self, table_name):
        """Fetch the shard specification mapping for the given table

        :param table_name: The name of the table for which the sharding
                           specification is being queried.

        :return: The a dictionary that contains the shard mapping information
                 for the given table.
        """
        return Command.generate_output_pattern(_lookup_shard_mapping,
                                               table_name)

class ListShardMappings(Command):
    """Returns all the shard mappings of a particular
    sharding_type.
    """
    group_name = "sharding"
    command_name = "list_tables"
    def execute(self, sharding_type):
        """The method returns all the shard mappings (names) of a
        particular sharding_type. For example if the method is called
        with 'range' it returns all the sharding specifications that exist
        of type range.

        :param sharding_type: The sharding type for which the sharding
                              specification needs to be returned.

        :return: A list of dictionaries of shard mappings that are of
                     the sharding type
                     An empty list of the sharding type is valid but no
                     shard mapping definition is found
                     An error if the sharding type is invalid.
        """
        return Command.generate_output_pattern(_list, sharding_type)

class ListShardMappingDefinitions(Command):
    """Lists all the shard mapping definitions.
    """
    group_name = "sharding"
    command_name = "list_definitions"
    def execute(self):
        """The method returns all the shard mapping definitions.

        :return: A list of shard mapping definitions
                    An Empty List if no shard mapping definition is found.
        """
        rset = ResultSet(
            names=('mapping_id', 'type_name', 'global_group_id'),
            types=(int, str, str),
        )
        for row in ShardMapping.list_shard_mapping_defn():
            rset.append_row(row)
        return CommandResult(None, results=rset)

ADD_SHARD = _events.Event("ADD_SHARD")
class AddShard(ProcedureShard):
    """Add a shard.
    """
    group_name = "sharding"
    command_name = "add_shard"
    def execute(self, shard_mapping_id, groupid_lb_list, state="DISABLED",
                synchronous=True):
        """Add the RANGE shard specification. This represents a single instance
        of a shard specification that maps a key RANGE to a server.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param state: Indicates whether a given shard is ENABLED or DISABLED
        :param groupid_lb_list: The list of group_id, lower_bounds pairs in
                                the format, group_id/lower_bound,
                                group_id/lower_bound...
        :param synchronous: Whether one should wait until the execution finishes
                            or not.

        :return: A dictionary representing the current Range specification.
        """
        procedures = _events.trigger(ADD_SHARD, self.get_lockable_objects(),
            shard_mapping_id, groupid_lb_list, state
        )
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SHARD = \
        _events.Event("REMOVE_SHARD")
class RemoveShard(ProcedureShard):
    """Remove a Shard.
    """
    group_name = "sharding"
    command_name = "remove_shard"
    def execute(self, shard_id, synchronous=True):
        """Remove the RANGE specification mapping represented by the current
        RANGE shard specification object.

        :param shard_id: The shard ID of the shard that needs to be removed.
        :param synchronous: Whether one should wait until the execution finishes
                        or not.
        """

        procedures = _events.trigger(
            REMOVE_SHARD, self.get_lockable_objects(), shard_id
        )
        return self.wait_for_procedures(procedures, synchronous)

SHARD_ENABLE = \
        _events.Event("SHARD_ENABLE")
class EnableShard(ProcedureShard):
    """Enable a shard.
    """
    group_name = "sharding"
    command_name = "enable_shard"
    def execute(self, shard_id, synchronous=True):
        """Enable the Shard represented by the shard_id.

        :param shard_id: The shard ID of the shard that needs to be removed.
        :param synchronous: Whether one should wait until the execution finishes
                        or not.
        """
        procedures = _events.trigger(
            SHARD_ENABLE, self.get_lockable_objects(), shard_id
        )
        return self.wait_for_procedures(procedures, synchronous)

SHARD_DISABLE = \
        _events.Event("SHARD_DISABLE")
class DisableShard(ProcedureShard):
    """Disable a shard.
    """
    group_name = "sharding"
    command_name = "disable_shard"
    def execute(self, shard_id, synchronous=True):
        """Disable the Shard represented by the shard_id.

        :param shard_id: The shard ID of the shard that needs to be removed.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """

        procedures = _events.trigger(
            SHARD_DISABLE, self.get_lockable_objects(), shard_id
        )
        return self.wait_for_procedures(procedures, synchronous)

class LookupShardServers(Command):
    """Lookup a shard based on the give sharding key.
    """
    group_name = "sharding"
    command_name = "lookup_servers"
    def execute(self, table_name, key, hint="LOCAL"):
        """Given a table name and a key return the server where the shard of
        this table can be found.

        :param table_name: The table whose sharding specification needs to be
                            looked up.
        :param key: The key value that needs to be looked up
        :param hint: A hint indicates if the query is LOCAL or GLOBAL

        :return: The Group UUID that contains the range in which the key
                 belongs.
        """
        return _lookup(table_name, key, hint)

class DumpShardTables(Command):
    """Return information about all tables belonging to mappings
    matching any of the provided patterns. If no patterns are provided,
    dump information about all tables.
    """
    group_name = "dump"
    command_name = "shard_tables"

    def execute(self, connector_version=None, patterns=""):
        """Return information about all tables belonging to mappings
        matching any of the provided patterns.

        :param connector_version: The connectors version of the data.
        :param patterns: shard mapping pattern.
        """

        rset = ResultSet(
            names=('schema_name', 'table_name', 'column_name', 'mapping_id'),
            types=(str, str, str, int),
        )

        for row in ShardMapping.dump_shard_tables(connector_version, patterns):
            rset.append_row(row)

        return CommandResult(None, results=rset)

class DumpShardingInformation(Command):
    """Return all the sharding information about the tables passed as patterns.
    If no patterns are provided, dump sharding information about all tables.
    """
    group_name = "dump"
    command_name = "sharding_information"

    def execute(self, connector_version=None, patterns=""):
        """Return all the sharding information about the tables passed as
        patterns. If no patterns are provided, dump sharding information
        about all tables.

        :param connector_version: The connectors version of the data.
        :param patterns: shard table pattern.
        """

        rset = ResultSet(
            names=('schema_name', 'table_name', 'column_name', 'lower_bound',
                   'shard_id', 'type_name', 'group_id', 'global_group'),
            types=(str, str, str, str, int, str, str, str),
        )

        for row in ShardMapping.dump_sharding_info(connector_version, patterns):
            rset.append_row(row)

        return CommandResult(None, results=rset)

class DumpShardMappings(Command):
    """Return information about all shard mappings matching any of the
    provided patterns. If no patterns are provided, dump information about
    all shard mappings.
    """
    group_name = "dump"
    command_name = "shard_maps"

    def execute(self, connector_version=None, patterns=""):
        """Return information about all shard mappings matching any of the
        provided patterns.

        :param connector_version: The connectors version of the data.
        :param patterns: shard mapping pattern.
        """

        rset = ResultSet(
            names=('mapping_id', 'type_name', 'global_group_id'),
            types=(int, str, str),
        )

        for row in ShardMapping.dump_shard_maps(connector_version, patterns):
            rset.append_row(row)

        return CommandResult(None, results=rset)

class DumpShardIndex(Command):
    """Return information about the index for all mappings matching
    any of the patterns provided. If no pattern is provided, dump the
    entire index. The lower_bound that is returned is a string that is
    a md-5 hash of the group-id in which the data is stored.
    """
    group_name = "dump"
    command_name = "shard_index"

    def execute(self, connector_version=None, patterns=""):
        """Return information about the index for all mappings matching
        any of the patterns provided.

        :param connector_version: The connectors version of the data.
        :param patterns: group pattern.
        """

        rset = ResultSet(
            names=('lower_bound', 'mapping_id', 'shard_id', 'group_id'),
            types=(str, int, int, str),
        )

        for row in Shards.dump_shard_indexes(connector_version, patterns):
            rset.append_row(row)

        return CommandResult(None, results=rset)

@_events.on_event(DEFINE_SHARD_MAPPING)
def _define_shard_mapping(type_name, global_group_id):
    """Define a shard mapping.

    :param type_name: The type of sharding scheme - RANGE, HASH, LIST etc
    :param global_group: Every shard mapping is associated with a
                        Global Group that stores the global updates
                        and the schema changes for this shard mapping
                        and dissipates these to the shards.
    :return: The shard_mapping_id generated for the shard mapping.
    :raises: ShardingError if the sharding type is invalid.
    """
    type_name = type_name.upper()
    if type_name not in Shards.VALID_SHARDING_TYPES:
        raise _errors.ShardingError(INVALID_SHARDING_TYPE % (type_name, ))
    shard_mapping_id = ShardMapping.define(type_name, global_group_id)
    return shard_mapping_id

@_events.on_event(ADD_SHARD_MAPPING)
def _add_shard_mapping(shard_mapping_id, table_name, column_name):
    """Add a table to a shard mapping.

    :param shard_mapping_id: The shard mapping id to which the input
                                table is attached.
    :param table_name: The table being sharded.
    :param column_name: The column whose value is used in the sharding
                        scheme being applied

    :return: True if the the table was successfully added.
                False otherwise.
    """
    ShardMapping.add(shard_mapping_id, table_name, column_name)

@_events.on_event(REMOVE_SHARD_MAPPING)
def _remove_shard_mapping(table_name):
    """Remove the shard mapping for the given table.

    :param table_name: The name of the table for which the shard mapping
                        needs to be removed.

    :return: True if the remove succeeded
            False if the query failed
    :raises: ShardingError if the table name is not found.
    """
    shard_mapping = ShardMapping.fetch(table_name)
    if shard_mapping is None:
        raise _errors.ShardingError(TABLE_NAME_NOT_FOUND % (table_name, ))
    shard_mapping.remove()

@_events.on_event(REMOVE_SHARD_MAPPING_DEFN)
def _remove_shard_mapping_defn(shard_mapping_id):
    """Remove the shard mapping definition of the given table.

    :param shard_mapping_id: The shard mapping ID of the shard mapping
                            definition that needs to be removed.
    """
    ShardMapping.remove_sharding_definition(shard_mapping_id)

def _lookup_shard_mapping(table_name):
    """Fetch the shard specification mapping for the given table

    :param table_name: The name of the table for which the sharding
                        specification is being queried.

    :return: A dictionary that contains the shard mapping information for
                the given table.
    """
    shard_mapping = ShardMapping.fetch(table_name)
    if shard_mapping is not None:
        return [{"mapping_id":shard_mapping.shard_mapping_id,
                 "table_name":shard_mapping.table_name,
                 "column_name":shard_mapping.column_name,
                 "type_name":shard_mapping.type_name,
                 "global_group":shard_mapping.global_group}]
    else:
        #We return an empty shard mapping because if an Error is thrown
        #it would cause the executor to rollback which is an unnecessary
        #action. It is enough if we inform the user that the lookup returned
        #nothing.
        return [{"mapping_id":"",
                 "table_name":"",
                 "column_name":"",
                 "type_name":"",
                 "global_group":""}]

def _list(sharding_type):
    """The method returns all the shard mappings (names) of a
    particular sharding_type. For example if the method is called
    with 'range' it returns all the sharding specifications that exist
    of type range.

    :param sharding_type: The sharding type for which the sharding
                          specification needs to be returned.

    :return: A list of dictionaries of shard mappings that are of
                 the sharding type
                 An empty list of the sharding type is valid but no
                 shard mapping definition is found
                 An error if the sharding type is invalid.

    :raises: Sharding Error if Sharding type is not found.
    """
    if sharding_type not in Shards.VALID_SHARDING_TYPES:
        raise _errors.ShardingError(INVALID_SHARDING_TYPE % (sharding_type,))

    ret_shard_mappings = []
    shard_mappings = ShardMapping.list(sharding_type)
    for shard_mapping in shard_mappings:
        ret_shard_mappings.append({
                    "mapping_id":shard_mapping.shard_mapping_id,
                    "table_name":shard_mapping.table_name,
                    "column_name":shard_mapping.column_name,
                    "type_name":shard_mapping.type_name,
                    "global_group":shard_mapping.global_group})
    return ret_shard_mappings

@_events.on_event(ADD_SHARD)
def _add_shard(shard_mapping_id, groupid_lb_list, state):
    """Add the RANGE shard specification. This represents a single instance
    of a shard specification that maps a key RANGE to a server.

    :param shard_mapping_id: The unique identification for a shard mapping.
    :param groupid_lb_list: The list of group_id, lower_bounds pairs in the
                        format, group_id/lower_bound, group_id/lower_bound... .
    :param state: Indicates whether a given shard is ENABLED or DISABLED

    :return: True if the add succeeded.
                False otherwise.
    :raises: ShardingError If the group on which the shard is being
                           created does not exist,
                           If the shard_mapping_id is not found,
                           If adding the shard definition fails,
                           If the state of the shard is an invalid
                           value,
                           If the range definition is invalid.
    """
    shard_mapping = ShardMapping.fetch_shard_mapping_defn(shard_mapping_id)
    if shard_mapping is None:
        raise _errors.ShardingError(SHARD_MAPPING_NOT_FOUND % \
                                                    (shard_mapping_id,  ))

    schema_type = shard_mapping[1]

    if len(RangeShardingSpecification.list(shard_mapping_id)) != 0:
        raise _errors.ShardingError(SHARDS_ALREADY_EXIST)

    group_id_list, lower_bound_list = \
        _utils.get_group_lower_bound_list(groupid_lb_list)

    if (len(group_id_list) != len(lower_bound_list)) and\
        schema_type == "RANGE":
        raise _errors.ShardingError(LOWER_BOUND_GROUP_ID_COUNT_MISMATCH)

    if len(lower_bound_list) != 0 and schema_type == "HASH":
        raise _errors.ShardingError(LOWER_BOUND_AUTO_GENERATED)

    if schema_type in Shards.VALID_RANGE_SHARDING_TYPES:
        for lower_bound in lower_bound_list:
            if not SHARDING_DATATYPE_HANDLER[schema_type].\
                        is_valid_lower_bound(lower_bound):
                raise _errors.ShardingError(
                                INVALID_LOWER_BOUND_VALUE % (lower_bound, ))

    state = state.upper()
    if state not in Shards.VALID_SHARD_STATES:
        raise _errors.ShardingError(INVALID_SHARD_STATE % (state,  ))

    for index, group_id in enumerate(group_id_list):
        shard = Shards.add(group_id, state)

        shard_id = shard.shard_id

        if schema_type == "HASH":
            HashShardingSpecification.add(
                shard_mapping_id,
                shard_id
            )
            _LOGGER.debug(
                "Added Shard (map id = %s, id = %s).",
                shard_mapping_id,
                shard_id
            )
        else:
            range_sharding_specification = \
                SHARDING_SPECIFICATION_HANDLER[schema_type].add(
                                                shard_mapping_id,
                                                lower_bound_list[index],
                                                shard_id
                                            )
            _LOGGER.debug(
                "Added Shard (map id = %s, lower bound = %s, id = %s).",
                range_sharding_specification.shard_mapping_id,
                range_sharding_specification.lower_bound,
                range_sharding_specification.shard_id
            )

        #If the shard is added in a DISABLED state  do not setup replication
        #with the primary of the global group. Basically setup replication only
        #if the shard is ENABLED.
        if state == "ENABLED":
            _setup_shard_group_replication(shard_id)

@_events.on_event(REMOVE_SHARD)
def _remove_shard(shard_id):
    """Remove the RANGE specification mapping represented by the current
    RANGE shard specification object.

    :param shard_id: The shard ID of the shard that needs to be removed.

    :return: True if the remove succeeded
            False if the query failed
    :raises: ShardingError if the shard id is not found,
        :       ShardingError if the shard is not disabled.
    """
    range_sharding_specification, shard, _, _ = \
        verify_and_fetch_shard(shard_id)
    if shard.state == "ENABLED":
        raise _errors.ShardingError(SHARD_NOT_DISABLED)
    #Stop the replication of the shard group with the global
    #group. Also clear the references of the master and the
    #slave group from the current group.
    #NOTE: When we do the stopping of the shard group
    #replication in shard remove we are actually just clearing
    #the references, since a shard cannot  be removed unless
    #it is disabled and when it is disabled the replication is
    #stopped but the references are not cleared.
    _stop_shard_group_replication(shard_id,  True)
    range_sharding_specification.remove()
    shard.remove()
    _LOGGER.debug("Removed Shard (%s).", shard_id)

def _lookup(lookup_arg, key,  hint):
    """Given a table name and a key return the servers of the Group where the
    shard of this table can be found

    :param lookup_arg: table name for "LOCAL" lookups
                Shard Mapping ID for "GLOBAL" lookups.
    :param key: The key value that needs to be looked up
    :param hint: A hint indicates if the query is LOCAL or GLOBAL

    :return: The servers of the Group that contains the range in which the
            key belongs.
    """
    VALID_HINTS = ('LOCAL',  'GLOBAL')
    hint = hint.upper()
    if hint not in VALID_HINTS:
        raise _errors.ShardingError(INVALID_SHARDING_HINT)

    group = None

    #Perform the lookup for the group contaning the lookup data.
    if hint == "GLOBAL":
        #Fetch the shard mapping object. In the case of GLOBAL lookups
        #the shard mapping ID is passed directly. In the case of "LOCAL"
        #lookups it is the table name that is passed.
        shard_mapping = ShardMapping.fetch_by_id(lookup_arg)
        if shard_mapping is None:
            raise _errors.ShardingError(
                SHARD_MAPPING_NOT_FOUND % (lookup_arg,  )
                )
        #GLOBAL lookups. There can be only one global group, hence using
        #shard_mapping[0] is safe.
        group_id = shard_mapping[0].global_group
    else:
        shard_mapping = ShardMapping.fetch(lookup_arg)
        if shard_mapping is None:
            raise _errors.ShardingError(TABLE_NAME_NOT_FOUND % (lookup_arg,  ))
        sharding_specification = \
            SHARDING_SPECIFICATION_HANDLER[shard_mapping.type_name].\
            lookup(key, shard_mapping.shard_mapping_id, shard_mapping.type_name)
        if sharding_specification is None:
            raise _errors.ShardingError(INVALID_SHARDING_KEY % (key,  ))
        shard = Shards.fetch(str(sharding_specification.shard_id))
        if shard.state == "DISABLED":
            raise _errors.ShardingError(SHARD_NOT_ENABLED)
        #group cannot be None since there is a foreign key on the group_id.
        #An exception will be thrown nevertheless.
        group_id = shard.group_id

    return ServerLookups().execute(group_id=group_id)
    
@_events.on_event(SHARD_ENABLE)
def _enable_shard(shard_id):
    """Enable the RANGE specification mapping represented by the current
    RANGE shard specification object.

    :param shard_id: The shard ID of the shard that needs to be removed.

    :return: True Placeholder return value
    :raises: ShardingError if the shard_id is not found.
    """
    _, shard, _, _ = verify_and_fetch_shard(shard_id)
    #When you enable a shard, setup replication with the global server
    #of the shard mapping associated with this shard.
    _setup_shard_group_replication(shard_id)
    shard.enable()

@_events.on_event(SHARD_DISABLE)
def _disable_shard(shard_id):
    """Disable the RANGE specification mapping represented by the current
    RANGE shard specification object.

    :param shard_id: The shard ID of the shard that needs to be removed.

    :return: True Placeholder return value
    :raises: ShardingError if the shard_id is not found.
    """
    _, shard, _, _ = verify_and_fetch_shard(shard_id)
    #When you disable a shard, disable replication with the global server
    #of the shard mapping associated with the shard.
    _stop_shard_group_replication(shard_id,  False)
    shard.disable()

def verify_and_fetch_shard(shard_id):
    """Find out if the shard_id exists and return the sharding specification for
    it. If it does not exist throw an exception.

    :param shard_id: The ID for the shard whose specification needs to be
                     fetched.

    :return: The sharding specification class representing the shard ID.

    :raises: ShardingError if the shard ID is not found.
    """
    #Here the underlying sharding specification might be a RANGE
    #or a HASH. The type of sharding specification is obtained from the
    #shard mapping.
    range_sharding_spec = RangeShardingSpecification.fetch(shard_id)
    if range_sharding_spec is None:
        raise _errors.ShardingError(SHARD_NOT_FOUND % (shard_id,  ))

    #Fetch the shard mappings and use them to find the type of sharding
    #scheme.
    shard_mappings = ShardMapping.fetch_by_id(
                        range_sharding_spec.shard_mapping_id
                    )
    if shard_mappings is None:
        raise _errors.ShardingError(
                    SHARD_MAPPING_NOT_FOUND % (
                        range_sharding_spec.shard_mapping_id,
                    )
                )

    #Fetch the shard mapping definition. There is only one shard mapping
    #definition associated with all of the shard mappings.
    shard_mapping_defn =  ShardMapping.fetch_shard_mapping_defn(
                        range_sharding_spec.shard_mapping_id
                    )
    if shard_mapping_defn is None:
        raise _errors.ShardingError(
                    SHARD_MAPPING_DEFN_NOT_FOUND % (
                        range_sharding_spec.shard_mapping_id,
                    )
                )

    shard = Shards.fetch(shard_id)
    if shard is None:
        raise _errors.ShardingError(SHARD_NOT_FOUND % (shard_id,  ))

    #Both of the shard_mappings retrieved will be of the same sharding
    #type. Hence it is safe to use one of them to retireve the sharding type.
    if shard_mappings[0].type_name == "HASH":
        return HashShardingSpecification.fetch(shard_id), \
            shard,  shard_mappings, shard_mapping_defn
    else:
        return range_sharding_spec, shard, shard_mappings, shard_mapping_defn

def _setup_shard_group_replication(shard_id):
    """Setup the replication between the master group and the
    shard group. This is a utility method that given a shard id
    will lookup the group associated with this shard, and setup
    replication between the group and the global group.

    :param shard_id: The ID of the shard, whose group needs to
                     be setup as a slave.
    """
    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    _, shard, _, shard_mapping_defn  = \
        verify_and_fetch_shard(shard_id)

    #Setup replication between the shard group and the global group.
    _group_replication.setup_group_replication \
            (shard_mapping_defn[2],  shard.group_id)

def _stop_shard_group_replication(shard_id,  clear_ref):
    """Stop the replication between the master group and the shard group.

    :param shard_id: The ID of the shard, whose group needs to
                     be atopped as a slave.
    :param clear_ref: Indicates whether removing the shard should result
                      in the shard group losing all its slave group references.
    """
    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    _, shard, _, shard_mapping_defn  = \
        verify_and_fetch_shard(shard_id)

    #Stop the replication between the shard group and the global group. Also
    #based on the clear_ref flag decide if you want to clear the references
    #associated with the group.
    _group_replication.stop_group_slave(shard_mapping_defn[2],  shard.group_id,
                                                                clear_ref)
