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

"""This module contains the plumbing necessary for creating, modifying and
querying the sharding information from the state stores.

"""

import mysql.fabric.errors as _errors
import mysql.fabric.persistence as _persistence
import mysql.fabric.utils as _utils

from mysql.fabric.server import MySQLServer, Group
from mysql.fabric.sharding_datatype import (
    HashShardingHandler,
    RangeShardingIntegerHandler,
    RangeShardingStringHandler,
    RangeShardingDateTimeHandler
)

class ShardMapping(_persistence.Persistable):
    """Represents the mapping between the sharding scheme and the table
    being sharded. The class encapsulates the operations required to
    manage the information in the state store related to this mapping.

    NOTE: A sharding scheme contains the details of the type name (i.e.) RANGE,
    HASH, List used for sharding and the parameters for the same. For example,
    a range sharding would contain the Lower Bound and the Upper Bound for
    the sharding definition.

    The information in the state store, for a sharding scheme, in the case
    where the state store is a relational store takes the following form:

        +--------------+--------------------------+---------------------+
        | shard_map_id |      table_name          |    column_name      |
        +==============+==========================+=====================+
        |1             |Employee                  |ID                   |
        +--------------+--------------------------+---------------------+
        |2             |Salary                    |EmpID                |
        +--------------+--------------------------+---------------------+

    The columns are explained as follows,
    * shard_mapping_id - The unique identification for a shard mapping.
    * table_name - The tables associated with this shard mapping.
    * column_name - The column name in the table that is used to shard this
    table.

        +--------------+--------------------------+---------------------+
        | shard_map_id |         type_name        |     global          |
        +==============+==========================+=====================+
        |1             |RANGE                     |GROUPIDX             |
        +--------------+--------------------------+---------------------+

    The columns are explained as follows

    * shard_mapping_id - The unique identification for a shard mapping.
    * type_name - The type name of the sharding scheme- RANGE, HASH, LIST etc
    * global_group - Every shard mapping is associated with a Global Group
                        that stores the global updates and the schema changes
                        for this shard mapping and dissipates these to the
                        shards.
    """

    #Create the schema for the tables used to store the shard specification
    #mapping information
    CREATE_SHARD_MAPPING = ("CREATE TABLE shard_tables"
                            "(shard_mapping_id INT NOT NULL, "
                            "table_name VARCHAR(64) NOT NULL, "
                            "column_name VARCHAR(64) NOT NULL, "
                            "PRIMARY KEY (table_name, column_name), "
                            "INDEX(shard_mapping_id)) "
                            "DEFAULT CHARSET=utf8"
    )

    DUMP_SHARD_TABLES = (
                            "SELECT "
                            "table_name, column_name, shard_mapping_id "
                            "FROM "
                            "shard_tables "
                            "WHERE "
                            "shard_mapping_id LIKE %s "
                            "ORDER BY shard_mapping_id, table_name, column_name"
                         )

    DUMP_SHARDING_INFORMATION = (
        "SELECT t.table_name, t.column_name, "
        "IF( m.type_name = 'HASH', HEX(r.lower_bound), r.lower_bound) "
        "AS lower_bound, "
        "r.shard_id, "
        "m.type_name, s.group_id, m.global_group "
        "FROM "
        "shard_maps AS m RIGHT JOIN shard_tables AS t USING (shard_mapping_id) "
        "LEFT JOIN shard_ranges AS r USING (shard_mapping_id) "
        "LEFT JOIN shards AS s USING (shard_id) "
        "WHERE table_name LIKE %s AND "
        "s.state = 'ENABLED' "
        "ORDER BY r.shard_id, t.table_name, t.column_name, r.lower_bound"
    )

    CREATE_SHARD_MAPPING_DEFN = (
        "CREATE TABLE shard_maps"
         "("
         "shard_mapping_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY, "
         "type_name ENUM "
         "("
         "'RANGE','RANGE_INTEGER','RANGE_STRING','RANGE_DATETIME','HASH'"
         ") NOT NULL, "
         "global_group VARCHAR(64)"
         ") DEFAULT CHARSET=utf8"
    )

    DUMP_SHARD_MAPS = (
                            "SELECT "
                            "shard_mapping_id, type_name, global_group "
                            "FROM "
                            "shard_maps "
                            "WHERE "
                            "shard_mapping_id LIKE %s "
                            "ORDER BY "
                            "shard_mapping_id, type_name, global_group"
                       )

    #Create the referential integrity constraint with the shard_maps
    #table.
    ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = \
                                ("ALTER TABLE shard_tables "
                                  "ADD CONSTRAINT fk_shard_mapping_id "
                                  "FOREIGN KEY(shard_mapping_id) REFERENCES "
                                  "shard_maps(shard_mapping_id)")

    #Create the referential integrity constraint with the groups table.
    ADD_FOREIGN_KEY_CONSTRAINT_GLOBAL_GROUP = \
                            ("ALTER TABLE shard_maps "
                            "ADD CONSTRAINT fk_shard_mapping_global_group "
                            "FOREIGN KEY(global_group) REFERENCES "
                            "groups(group_id)")

    #Define a shard mapping.
    DEFINE_SHARD_MAPPING = ("INSERT INTO "
                            "shard_maps(type_name, global_group) "
                            "VALUES(%s, %s)")
    ADD_SHARD_MAPPING = ("INSERT INTO shard_tables"
                         "(shard_mapping_id, table_name, column_name) "
                         "VALUES(%s, %s, %s)")

    #select the shard specification mapping information for a table
    SELECT_SHARD_MAPPING = ("SELECT sm.shard_mapping_id, table_name, "
                            "column_name, type_name, "
                            "global_group "
                            "FROM shard_tables as sm, "
                            "shard_maps as smd "
                            "WHERE sm.shard_mapping_id = smd.shard_mapping_id "
                            "AND table_name = %s")

    #Select the shard mapping for a given shard mapping ID.
    SELECT_SHARD_MAPPING_BY_ID = ("SELECT sm.shard_mapping_id, table_name, "
                            "column_name, type_name, "
                            "global_group "
                            "FROM shard_tables as sm, "
                            "shard_maps as smd "
                            "WHERE sm.shard_mapping_id = smd.shard_mapping_id "
                            "AND sm.shard_mapping_id = %s")

    #Select all the shard specifications of a particular sharding type name.
    LIST_SHARD_MAPPINGS = ("SELECT sm.shard_mapping_id, table_name, "
                            "column_name, type_name, "
                            "global_group "
                            "FROM shard_tables as sm, "
                            "shard_maps as smd "
                            "WHERE sm.shard_mapping_id = smd.shard_mapping_id "
                            "AND type_name = %s")

    #Select the shard mapping for a given shard mapping ID.
    SELECT_SHARD_MAPPING_DEFN = ("SELECT shard_mapping_id, type_name, "
                                 "global_group FROM shard_maps "
                                 "WHERE shard_mapping_id = %s")

    #Select all the shard mapping definitions
    LIST_SHARD_MAPPING_DEFN = ("SELECT shard_mapping_id, type_name, "
                               "global_group FROM shard_maps")

    #Delete the sharding specification to table mapping for a given table.
    DELETE_SHARD_MAPPING = ("DELETE FROM shard_tables "
                            "WHERE table_name = %s")

    #Delete the shard mapping definition
    DELETE_SHARD_MAPPING_DEFN = ("DELETE FROM shard_maps "
                                 "WHERE shard_mapping_id = %s")

    QUERY_SHARD_MAPPING_PER_GROUP = ("SELECT shard_mapping_id "
        "FROM shard_maps WHERE global_group = %s"
    )

    def __init__(self, shard_mapping_id, table_name, column_name, type_name,
                 global_group):
        """Initialize the Shard Specification Mapping for a given table.

        :param shard_mapping_id: The unique identifier for this shard mapping.
        :param table_name: The table for which the mapping is being created.
        :param column_name: The column name on which the mapping is being
                            defined. The column is useful when resharding
                            comes into the picture.
        :param type_name: The type of sharding defined on the given table.
                            E.g. RANGE, HASH etc.
        :param global_group: Every shard mapping is associated with a Global
                                Group that stores the global updates and the
                                schema changes for this shard mapping and
                                dissipates these to the shards.
        """
        super(ShardMapping, self).__init__()
        self.__shard_mapping_id = shard_mapping_id
        self.__table_name = table_name
        self.__column_name = column_name
        self.__type_name = type_name
        self.__global_group = global_group

    @property
    def shard_mapping_id(self):
        """Return the shard mapping id for this shard mapping.
        """
        return self.__shard_mapping_id

    @property
    def table_name(self):
        """Return the table on which the shard mapping is defined.
        """
        return self.__table_name

    @property
    def column_name(self):
        """Return the column in the table on which the sharding is defined.
        """
        return self.__column_name

    @property
    def type_name(self):
        """Return the type of the sharding specification defined on the table.
        """
        return self.__type_name

    @property
    def global_group(self):
        """Return the global group for the sharding specification.
        """
        return self.__global_group

    @staticmethod
    def list(sharding_type, persister=None):
        """The method returns all the shard mappings (names) of a
        particular sharding_type. For example if the method is called
        with 'range' it returns all the sharding specifications that exist
        of type range.

        :param sharding_type: The sharding type for which the sharding
                              specification needs to be returned.
        :param persister: A valid handle to the state store.

        :return: A list of sharding specification names that are of the
                  sharding type.
        """

        sharding_type = sharding_type.upper()
        cur = persister.exec_stmt(ShardMapping.LIST_SHARD_MAPPINGS,
                                  {"fetch" : False,
                                  "params" : (sharding_type,)})
        rows = cur.fetchall()

        return [ ShardMapping(*row[0:5]) for row in rows ]

    def remove(self, persister=None):
        """Remove the shard mapping represented by the Shard Mapping object.
        The method basically results in removing the association between a
        table and the sharding specification used to shard this table.

        :param persister: Represents a valid handle to the state
                          store.
        """
        #Remove the shard mapping
        persister.exec_stmt(
            ShardMapping.DELETE_SHARD_MAPPING,
            {"params":(self.__table_name,)})

    @staticmethod
    def remove_sharding_definition(shard_mapping_id, persister=None):
        """Remove the shard mapping definition identified uniquely by the given
        shard_mapping_id.

        :param shard_mapping_id: The shard mapping ID of the sharding defn.
        :param persister: Represents a valid handle to the state
                          store.
        """
        #Remove the shard mapping definition.
        persister.exec_stmt(
            ShardMapping.DELETE_SHARD_MAPPING_DEFN,
            {"params":(shard_mapping_id,)})

    @staticmethod
    def create(persister=None):
        """Create the schema to store the mapping between the table and
        the sharding specification.

        :param persister: A valid handle to the state store.
        """

        persister.exec_stmt(ShardMapping.CREATE_SHARD_MAPPING)
        persister.exec_stmt(ShardMapping.CREATE_SHARD_MAPPING_DEFN)

    @staticmethod
    def fetch(table_name, persister=None):
        """Fetch the shard specification mapping for the given table

        :param table_name: The name of the table for which the sharding
                            specification is being queried.
        :param persister: A valid handle to the state store.
        :return: The ShardMapping object that encapsulates the shard mapping
                    information for the given table.
        """
        cur = persister.exec_stmt(
                                  ShardMapping.SELECT_SHARD_MAPPING,
                                  {"fetch" : False,
                                  "params" : (table_name,)})
        row = cur.fetchone()
        if row:
            return ShardMapping(row[0], row[1], row[2], row[3], row[4])

        return None

    @staticmethod
    def fetch_by_id(shard_mapping_id, persister=None):
        """Fetch the shard specification mapping for the given shard mapping
        ID.

        :param shard_mapping_id: The shard mapping id for which the sharding
                            specification is being queried.
        :param persister: A valid handle to the state store.
        :return: The ShardMapping objects that encapsulates the shard mapping
                    information for the given shard mapping ID.
        """
        #When there are several tables related (sharded) by the same key defn,
        #we will have multiple shard mappings for the same shard mapping key.
        shard_mapping_list = []

        cur = persister.exec_stmt(
                                  ShardMapping.SELECT_SHARD_MAPPING_BY_ID,
                                  {"fetch" : False,
                                  "params" : (shard_mapping_id,)})
        rows = cur.fetchall()

        for row in rows:
            shard_mapping_list.append(
                ShardMapping(row[0], row[1], row[2], row[3], row[4])
            )

        return shard_mapping_list

    @staticmethod
    def fetch_shard_mapping_defn(shard_mapping_id, persister=None):
        """Fetch the shard mapping definition corresponding to the
        shard mapping id.

        :param shard_mapping_id: The id of the shard mapping definition that
                                    needs to be fetched.
        :param persister: A valid handle to the state store.
        :return: A list containing the shard mapping definition parameters.
        """
        row = persister.exec_stmt(ShardMapping.SELECT_SHARD_MAPPING_DEFN,
                                  {"params" : (shard_mapping_id,)})
        if row:
            #There is no abstraction for a shard mapping definition. A
            #shard mapping definition is just a triplet of
            #(shard_id, sharding_type, global_group)
            return row[0]

        return None

    @staticmethod
    def list_shard_mapping_defn(persister=None):
        """Fetch all the shard mapping definitions.

        :param persister: A valid handle to the state store.
        :return: A list containing the shard mapping definitions.
        """
        rows = persister.exec_stmt(ShardMapping.LIST_SHARD_MAPPING_DEFN)
        if rows is not None:
            return rows

        return []

    @staticmethod
    def lookup_shard_mapping_id(group_id, persister=None):
        """Return the shard mapping associated with a group.

        :group_id: Group identification.
        :param persister: A valid handle to the state store.
        :return: Shard mapping associated to a group..
        """
        rows = persister.exec_stmt(ShardMapping.QUERY_SHARD_MAPPING_PER_GROUP,
          {"params": (group_id, )}
        )
        if rows:
            return rows[0][0]

    @staticmethod
    def define(type_name, global_group_id, persister=None):
        """Define a shard mapping.

        :param type_name: The type of sharding scheme - RANGE, HASH, LIST etc
        :param global_group: Every shard mapping is associated with a
                            Global Group that stores the global updates
                            and the schema changes for this shard mapping
                            and dissipates these to the shards.
        :param persister: A valid handle to the state store.
        :return: The shard_mapping_id generated for the shard mapping.
        """
        persister.exec_stmt(
            ShardMapping.DEFINE_SHARD_MAPPING,
            {"params":(type_name, global_group_id)})
        row = persister.exec_stmt("SELECT LAST_INSERT_ID()")
        return int(row[0][0])

    @staticmethod
    def add(shard_mapping_id, table_name, column_name, persister=None):
        """Add a table to a shard mapping.

        :param shard_mapping_id: The shard mapping id to which the input
                                    table is attached.
        :param table_name: The table being sharded.
        :param column_name: The column whose value is used in the sharding
                            scheme being applied
        :param persister: A valid handle to the state store.

        :return: The ShardMapping object for the mapping created.
                 None if the insert failed.
        """
        persister.exec_stmt(
            ShardMapping.ADD_SHARD_MAPPING,
            {"params":(shard_mapping_id, table_name, column_name)})
        return ShardMapping.fetch(table_name)

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints on the tables that stores the list of registered
        sharded tables.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(
            ShardMapping.ADD_FOREIGN_KEY_CONSTRAINT_GLOBAL_GROUP
        )
        persister.exec_stmt(
            ShardMapping.ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID
        )

    @staticmethod
    def dump_shard_tables(version=None, patterns="", persister=None):
        """Return the list of shard tables that belong to the shard mappings
        listed in the patterns strings separated by comma.

        :param version: The connectors version of the data.
        :param patterns: shard mapping pattern.
        :param persister: Persister to persist the object to.
        :return: A list of registered tables using the provided shard mapping
                pattern.
        """

        #This stores the pattern that will be passed to the LIKE MySQL
        #command.
        like_pattern = None

        if patterns is None:
            patterns = ''

        #Split the patterns string into a list of patterns of groups.
        pattern_list = _utils.split_dump_pattern(patterns)

        #Iterate through the pattern list and fire a query for
        #each pattern.
        for find in pattern_list:
            if find == '':
                like_pattern = '%%'
            else:
                like_pattern = '%' + find + '%'
            cur = persister.exec_stmt(ShardMapping.DUMP_SHARD_TABLES,
                                  {"fetch" : False, "params":(like_pattern,)})
            rows = cur.fetchall()
            #For each row fetched, split the fully qualified table name into
            #a database and table name.
            for row in rows:
                database, table = _utils.split_database_table(row[0])
                yield (database, table, row[1], row[2] )

    @staticmethod
    def dump_sharding_info(version=None, patterns="", persister=None):
        """Return all the sharding information about the table in the
        patterns string.

        :param version: The connectors version of the data.
        :param patterns: shard table pattern.
        :param persister: Persister to persist the object to.
        :return: The sharding information for all the tables passed in patterns.
        """

        #This stores the pattern that will be passed to the LIKE MySQL
        #command.
        like_pattern = None

        if patterns is None:
            patterns = ''

        #Split the patterns string into a list of patterns of groups.
        pattern_list = _utils.split_dump_pattern(patterns)

        #Iterate through the pattern list and fire a query for
        #each pattern.
        for find in pattern_list:
            if not find:
                like_pattern = '%%'
            else:
                like_pattern = find
            cur = persister.exec_stmt(ShardMapping.DUMP_SHARDING_INFORMATION,
                                  {"fetch" : False, "params":(like_pattern,)})
            rows = cur.fetchall()
            #For each row fetched, split the fully qualified table name into
            #a database and table name.
            for row in rows:
                database, table = _utils.split_database_table(row[0])
                yield (
                    database,
                    table,
                    row[1],     # column_name
                    row[2],     # lower_bound
                    row[3],     # shard_id
                    row[4],     # type_name
                    row[5],     # group_id
                    row[6],     # global_group
                )

    @staticmethod
    def dump_shard_maps(version=None, patterns="", persister=None):
        """Return the list of shard mappings identified by the shard mapping
        IDs listed in the patterns strings separated by comma.

        :param version: The connectors version of the data.
        :param patterns: shard mapping pattern.
        :param persister: Persister to persist the object to.
        :return: A list of shard mappings matching the provided pattern.
        """

        #This stores the pattern that will be passed to the LIKE MySQL
        #command.
        like_pattern = None

        if patterns is None:
            patterns = ''

        #Split the patterns string into a list of patterns of groups.
        pattern_list = _utils.split_dump_pattern(patterns)

        #Iterate through the pattern list and fire a query for
        #each pattern.
        for find in pattern_list:
            if find == '':
                like_pattern = '%%'
            else:
                like_pattern = '%' + find + '%'
            cur = persister.exec_stmt(ShardMapping.DUMP_SHARD_MAPS,
                                  {"fetch" : False, "params":(like_pattern,)})
            rows = cur.fetchall()
            for row in rows:
                yield (row[0], row[1], row[2])

class Shards(_persistence.Persistable):
    """Contains the mapping between the Shard ID and the Group ID

    A typical mapping between the shards and their location looks like
    the following

+--------------------+--------------------+--------------------+
|     shard_id       |      group_id      |     state          |
+====================+====================+====================+
|1                   |GroupID1            |ENABLED             |
+--------------------+--------------------+--------------------+

    The columns are explained as follows,

    shard_id - Unique identifier for the shard of a particular table.
    group_id - The Server location for the given partition of the table.
    state - Indicates whether a given shard is ENABLED or DISABLED.
    """

    #Tuple stores the list of valid Range Sharding Types.
    VALID_RANGE_SHARDING_TYPES = (
        'RANGE', 'RANGE_INTEGER','RANGE_STRING', 'RANGE_DATETIME',
    )

    #Tuple stores the list of valid Hash Sharding Types.
    VALID_HASH_SHARDING_TYPES = ("HASH",)

    #Tuple stores the list of valid shard mapping types.
    VALID_SHARDING_TYPES = \
        VALID_RANGE_SHARDING_TYPES + VALID_HASH_SHARDING_TYPES

    #Valid states of the shards
    VALID_SHARD_STATES = ("ENABLED", "DISABLED")

    #Create the schema for storing the shard to groups mapping
    CREATE_SHARDS = ("CREATE TABLE shards ("
                    "shard_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY, "
                    "group_id VARCHAR(64) NOT NULL, "
                    "state ENUM('DISABLED', 'ENABLED') NOT NULL) "
                    "DEFAULT CHARSET=utf8"
    )

    #Create the referential integrity constraint with the groups table.
    ADD_FOREIGN_KEY_CONSTRAINT_GROUP_ID = \
                                ("ALTER TABLE shards "
                                  "ADD CONSTRAINT fk_shards_group_id "
                                  "FOREIGN KEY(group_id) REFERENCES "
                                  "groups(group_id)")

    #Insert the Range to Shard mapping into the table.
    INSERT_SHARD = ("INSERT INTO shards(group_id, state) VALUES(%s, %s)")

    #Update the group_id for a shard.
    UPDATE_SHARD = ("UPDATE shards SET group_id=%s WHERE shard_id=%s")

    #Delete a given shard to group mapping.
    DELETE_SHARD = ("DELETE FROM shards WHERE shard_id = %s")

    #Select the group to which a shard ID maps to.
    SELECT_SHARD = ("SELECT shard_id, group_id, state "
                                    "FROM shards WHERE shard_id = %s")

    #Dump all the shard indexes that belong to a shard mapping ID.
    DUMP_SHARD_INDEXES = (
                            "SELECT "
                            "IF( m.type_name = 'HASH', HEX(sr.lower_bound), "
                            "sr.lower_bound), sr.shard_mapping_id, "
                            "s.shard_id, s.group_id "
                            "FROM "
                            "shard_maps AS m JOIN shard_ranges AS sr "
                            "USING (shard_mapping_id) "
                            "JOIN shards AS s USING (shard_id) "
                            "WHERE sr.shard_mapping_id LIKE %s AND "
                            "s.state = 'ENABLED' "
                            "ORDER BY s.shard_id, sr.shard_mapping_id, "
                            "sr.lower_bound, s.group_id"
                            )

    #Select the shard that belongs to a given group.
    SELECT_GROUP_FOR_SHARD = ("SELECT shard_id FROM shards WHERE group_id = %s")

    #Update the state of a shard
    UPDATE_SHARD_STATE = ("UPDATE shards SET state=%s where shard_id=%s")

    def __init__(self, shard_id, group_id,  state="DISABLED"):
        """Initialize the Shards object with the shard to group mapping.

        :param shard_id: An unique identification, a logical representation for
                         a shard of a particular table.
        :param group_id: The group ID to which the shard maps to.
        :param state: Indicates whether a given shard is ENABLED or DISABLED
        """
        super(Shards, self).__init__()
        self.__shard_id = shard_id
        self.__group_id = group_id
        self.__state = state

    @staticmethod
    def create(persister=None):
        """Create the schema to store the current Shard to Group mapping.

        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(Shards.CREATE_SHARDS)

    @staticmethod
    def add(group_id, state="DISABLED", persister=None):
        """Add a Group that will store a shard. A shard ID is automatically
        generated for a given added Group.

        :param group_id: The Group that is being added to store a shard.
        :param persister: A valid handle to the state store.
        :param state: Indicates whether a given shard is ENABLED or DISABLED

        :return: The Shards object containing a mapping between the shard
                    and the group.
        """
        persister.exec_stmt(Shards.INSERT_SHARD, {"params":(group_id, state)})
        row = persister.exec_stmt("SELECT LAST_INSERT_ID()")
        return Shards(int(row[0][0]), group_id, state)

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints on the Shards tables.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Shards.ADD_FOREIGN_KEY_CONSTRAINT_GROUP_ID)

    def remove(self, persister=None):
        """Remove the Shard to Group mapping.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Shards.DELETE_SHARD, \
                            {"params":(self.__shard_id,)})

    @staticmethod
    def fetch(shard_id, persister=None):
        """Fetch the Shards object containing the group_id for the input
        shard ID.

        :param shard_id: That shard ID whose mapping needs to be fetched.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        row = persister.exec_stmt(Shards.SELECT_SHARD, \
                                  {"params":(shard_id,)})

        if row is None:
            return None

        return Shards(row[0][0], row[0][1], row[0][2],)

    def enable(self, persister=None):
        """Set the state of the shard to ENABLED.
        """
        persister.exec_stmt(
          Shards.UPDATE_SHARD_STATE,
                             {"params":('ENABLED', self.__shard_id)})

    def disable(self, persister=None):
        """Set the state of the shard to DISABLED.
        """
        persister.exec_stmt(
          Shards.UPDATE_SHARD_STATE,
                             {"params":('DISABLED', self.__shard_id)})

    @staticmethod
    def lookup_shard_id(group_id,  persister=None):
        """Fetch the shard ID for the given Group.

        :param group_id: The Group that is being looked up.
        :param persister: A valid handle to the state store.

        :return: The shard_id contained in the given group_id.
        """
        row = persister.exec_stmt(Shards.SELECT_GROUP_FOR_SHARD, \
                                  {"params":(group_id,)})
        if row:
            return row[0][0]

    @property
    def shard_id(self):
        """Return the shard ID for the Shard to Group mapping.
        """
        return self.__shard_id

    @property
    def group_id(self):
        """Return the Group ID for the Shard to Group mapping.
        """
        return self.__group_id

    @group_id.setter
    def group_id(self,  group_id,  persister=None):
        """Set the group_id for the Shard.

        :param group_id: The Group that is being added to store a shard.
        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(Shards.UPDATE_SHARD,
                                        {"params":(group_id, self.__shard_id)})
        self.__group_id = group_id

    @property
    def state(self):
        """Return whether the state is ENABLED or DISABLED.
        """
        return self.__state

    @staticmethod
    def dump_shard_indexes(version=None, patterns="", persister=None):
        """Return the list of shard indexes that belong to the shard mappings
        listed in the patterns strings separated by comma.

        :param version: The connectors version of the data.
        :param patterns: shard mapping pattern.
        :param persister: Persister to persist the object to.
        :return: A list of shard indexes belonging to the shard mappings that
                match the provded patterns.
        """

        #This stores the pattern that will be passed to the LIKE MySQL
        #command.
        like_pattern = None

        if patterns is None:
            patterns = ''

        #Split the patterns string into a list of patterns of groups.
        pattern_list = _utils.split_dump_pattern(patterns)

        #Iterate through the pattern list and fire a query for
        #each pattern.
        for find in pattern_list:
            if find == '':
                like_pattern = '%%'
            else:
                like_pattern = '%' + find + '%'
            cur = persister.exec_stmt(Shards.DUMP_SHARD_INDEXES,
                                  {"fetch" : False, "params":(like_pattern,)})
            rows = cur.fetchall()
            for row in rows:
                yield (
                    row[0],
                    row[1],
                    row[2],
                    row[3]
                )

class RangeShardingSpecification(_persistence.Persistable):
    """Represents a RANGE sharding specification. The class helps encapsulate
    the representation of a typical RANGE sharding implementation in the
    state store.

    A typical RANGE sharding representation looks like the following,

        +--------------+---------+-----------+
        | shard_map_id |   LB    |  shard_id |
        +==============+=========+===========+
        |1             |10000    |1          |
        +--------------+---------+-----------+

    The columns in the above table are explained as follows,

    * shard_mapping_id - The unique identification for a shard mapping.
    * LB -The lower bound of the given RANGE sharding scheme instance
    * shard_id - An unique identification, a logical representation for a
                    shard of a particular table.
    """

    #Create the schema for storing the RANGE sharding specificaton.
    CREATE_RANGE_SPECIFICATION = ("CREATE TABLE "
                                "shard_ranges "
                                "(shard_mapping_id INT NOT NULL, "
                                "lower_bound VARBINARY(16) NOT NULL, "
                                "INDEX(lower_bound), "
                                "UNIQUE(shard_mapping_id, lower_bound), "
                                "shard_id INT NOT NULL) "
                                "DEFAULT CHARSET=utf8"
    )

    #Create the referential integrity constraint with the shard_mapping_defn
    #table
    ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = (
        "ALTER TABLE shard_ranges "
        "ADD CONSTRAINT "
        "fk_shard_mapping_id_sharding_spec "
        "FOREIGN KEY(shard_mapping_id) REFERENCES "
        "shard_maps(shard_mapping_id)"
    )

    #Create the referential integrity constraint with the shard_id
    #table
    ADD_FOREIGN_KEY_CONSTRAINT_SHARD_ID = \
                                ("ALTER TABLE shard_ranges "
                                  "ADD CONSTRAINT fk_shard_id_sharding_spec "
                                  "FOREIGN KEY(shard_id) REFERENCES "
                                  "shards(shard_id)")

    #Insert a RANGE of keys and the server to which they belong.
    INSERT_RANGE_SPECIFICATION = ("INSERT INTO shard_ranges"
        "(shard_mapping_id, lower_bound, shard_id) VALUES(%s, %s, %s)")

    #Delete a given RANGE specification instance.
    DELETE_RANGE_SPECIFICATION = ("DELETE FROM shard_ranges "
                                  "WHERE "
                                  "shard_id = %s")

    #Given a Shard ID select the RANGE Scheme that it defines.
    SELECT_RANGE_SPECIFICATION = (
        "SELECT shard_mapping_id, lower_bound, "
        "shard_id "
        "FROM shard_ranges "
        "WHERE shard_id = %s"
    )

    #Given a Shard Mapping ID select all the RANGE mappings that it
    #defines.
    LIST_RANGE_SPECIFICATION = (
        "SELECT "
        "shard_mapping_id, "
        "lower_bound, "
        "shard_id "
        "FROM shard_ranges "
        "WHERE shard_mapping_id = %s"
    )

    #Update the Range for a particular shard. The updation needs to happen
    #for the upper bound and the lower bound simultaneously.
    UPDATE_RANGE = (
        "UPDATE shard_ranges SET lower_bound = %s "
        " WHERE shard_id = %s"
    )

    def __init__(self, shard_mapping_id, lower_bound, shard_id):
        """Initialize a given RANGE sharding mapping specification.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param lower_bound: The lower bound of the given RANGE sharding defn
        :param shard_id: An unique identification, a logical representation
                        for a shard of a particular table.
        """
        super(RangeShardingSpecification, self).__init__()
        self.__shard_mapping_id = shard_mapping_id
        self.__lower_bound = lower_bound
        self.__shard_id = shard_id

    @property
    def shard_mapping_id(self):
        """Return the shard mapping to which this RANGE definition belongs.
        """
        return self.__shard_mapping_id

    @property
    def lower_bound(self):
        """Return the lower bound of this RANGE specification.
        """
        return self.__lower_bound

    @property
    def shard_id(self):
        """Return the Shard ID of the shard for this RANGE sharding
        definition
        """
        return self.__shard_id

    def remove(self, persister=None):
        """Remove the RANGE specification mapping represented by the current
        RANGE shard specification object.

        :param persister: Represents a valid handle to the state store.
        """
        persister.exec_stmt(
            RangeShardingSpecification.DELETE_RANGE_SPECIFICATION,
            {"params":(self.__shard_id,)})

    @staticmethod
    def add(shard_mapping_id, lower_bound, shard_id, persister=None):
        """Add the RANGE shard specification. This represents a single instance
        of a shard specification that maps a key RANGE to a server.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param lower_bound: The lower bound of the given RANGE sharding defn
        :param shard_id: An unique identification, a logical representation
                        for a shard of a particular table.

        :return: A RangeShardSpecification object representing the current
                Range specification.
                None if the insert into the state store failed
        """
        persister.exec_stmt(
            RangeShardingSpecification.INSERT_RANGE_SPECIFICATION, {
                "params":(
                    shard_mapping_id,
                    lower_bound,
                    shard_id
                )
            }
        )
        return RangeShardingSpecification(
            shard_mapping_id,
            lower_bound,
            shard_id
        )

    @staticmethod
    def create(persister=None):
        """Create the schema to store the current RANGE sharding specification.

        :param persister: A valid handle to the state store.
        """

        persister.exec_stmt(
                    RangeShardingSpecification.CREATE_RANGE_SPECIFICATION)

    @staticmethod
    def list(shard_mapping_id, persister=None):
        """Return the RangeShardingSpecification objects corresponding to the
        given sharding scheme.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param persister: A valid handle to the state store.

        :return: A  list of RangeShardingSpecification objects which belong to
                to this shard mapping.
                None if the shard mapping is not found
        """

        cur = persister.exec_stmt(
                    RangeShardingSpecification.LIST_RANGE_SPECIFICATION,
                        {"fetch" : False,
                        "params" : (shard_mapping_id,)})
        rows = cur.fetchall()
        return [ RangeShardingSpecification(*row[0:5]) for row in rows ]

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints on the sharding tables.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(
            RangeShardingSpecification.
            ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID
        )
        persister.exec_stmt(
            RangeShardingSpecification.
            ADD_FOREIGN_KEY_CONSTRAINT_SHARD_ID
        )

    @staticmethod
    def fetch(shard_id, persister=None):
        """Return the RangeShardingSpecification object corresponding to the
        given sharding ID.

        :param shard_id: The unique identification for a shard.
        :param persister: A valid handle to the state store.

        :return: A list that represents the information in the
                    RangeShardingSpecification.
                    An Empty list otherwise.
        """
        cur = persister.exec_stmt(
                    RangeShardingSpecification.SELECT_RANGE_SPECIFICATION,
                        {"fetch" : False,
                        "params" : (shard_id,)})
        row = cur.fetchone()
        if row is None:
            return None
        return RangeShardingSpecification(row[0], row[1],  row[2])

    @staticmethod
    def update_shard(shard_id, lower_bound, persister=None):
        """Update the range for a given shard_id.

        :param shard_id: The ID of the shard whose range needs to be updated.
        :param lower_bound: The new lower bound for the shard.
        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(
            RangeShardingSpecification.UPDATE_RANGE,
            {"params" : (lower_bound, shard_id)}
        )

    @staticmethod
    def lookup(key, shard_mapping_id, type, persister=None):
        """Return the Range sharding specification in whose key range the input
            key falls.

        :param key: The key which needs to be checked to see which range it
                    falls into
        :param shard_mapping_id: The unique identification for a shard mapping.
        :param persister: A valid handle to the state store.

        :return: The Range Sharding Specification that contains the range in
                which the key belongs.
        """
        cur = persister.exec_stmt(SHARDING_DATATYPE_HANDLER[type].LOOKUP_KEY,
                    {"fetch" : False,
                    "params" : (key, shard_mapping_id)})

        row = cur.fetchone()

        if row is None:
            return None
        return RangeShardingSpecification(row[0], row[1],  row[2])

    @staticmethod
    def get_upper_bound(lower_bound, shard_mapping_id, type, persister=None):
        """Return the next value in range for a given lower_bound value.
        This basically helps to form a (lower_bound, upper_bound) pair
        that can be used during a prune.

        :param lower_bound: The lower_bound value whose next range needs to
                            be retrieved.
        :param shard_mapping_id: The shard_mapping_id whose shards should be
                                searched for the given lower_bound.
        :param persister: A valid handle to the state store.

        :return: The next value in the range for the given lower_bound.
        """
        cur = persister.exec_stmt(
                        SHARDING_DATATYPE_HANDLER[type].SELECT_UPPER_BOUND,
                        {"fetch" : False,
                        "params" : (lower_bound, shard_mapping_id)})

        row = cur.fetchone()

        if row is None:
            return None

        return row[0]

    @staticmethod
    def delete_from_shard_db(table_name, type_name, prune_limit):
        """Delete the data from the copied data directories based on the
        sharding configuration uploaded in the sharding tables of the state
        store. The basic logic consists of

        * Querying the shard mapping ID corresponding to the sharding
          table.
        * Using the shard mapping ID to find the type of shard scheme and hence
            the sharding scheme table to query in.
        * Querying the sharding key range using the shard mapping ID.
        * Deleting the sharding keys that fall outside the range for a given
          server.

        :param table_name: The table being sharded.
        :param type_name: The type of the sharding definition.
        :param prune_limit: The number of DELETEs that should be
                            done in one batch.
        """

        shard_mapping = ShardMapping.fetch(table_name)
        if shard_mapping is None:
            raise _errors.ShardingError("Shard Mapping not found.")

        shard_mapping_id = shard_mapping.shard_mapping_id

        shards = RangeShardingSpecification.list(shard_mapping_id)
        if not shards:
            raise _errors.ShardingError("No shards associated with this"
                                                         " shard mapping ID.")

        for shard in shards:
            RangeShardingSpecification.prune_shard_id(shard.shard_id,
                                                      type_name, prune_limit)

    @staticmethod
    def prune_shard_id(shard_id, type_name, prune_limit):
        """Remove the rows in the shard that do not match the metadata
        in the shard_range tables. When the rows are being removed
        foreign key checks will be disabled.

        :param shard_id: The ID of the shard that needs to be pruned.
        :param type_name: The type of the sharding definition.
        :param prune_limit: The number of DELETEs that should be
                            done in one batch.
        """

        range_sharding_spec = RangeShardingSpecification.fetch(shard_id)
        if range_sharding_spec is None:
            raise _errors.ShardingError("No shards associated with this"
                                                         " shard mapping ID.")

        shard = Shards.fetch(shard_id)

        upper_bound = RangeShardingSpecification.get_upper_bound(
                            range_sharding_spec.lower_bound,
                            range_sharding_spec.shard_mapping_id,
                            type_name
                        )

        shard_mappings = \
            ShardMapping.fetch_by_id(range_sharding_spec.shard_mapping_id)
        if shard_mappings is None:
            raise _errors.ShardingError("Shard Mapping not found.")

        #There may be multiple tables sharded by the same sharding defns. We
        #need to run prune for all of them.
        for shard_mapping in shard_mappings:
            table_name = shard_mapping.table_name

            if upper_bound is not None:
                delete_query = (
                    SHARDING_DATATYPE_HANDLER[type_name].\
                        PRUNE_SHARD_WITH_UPPER_BOUND) % (
                    table_name,
                    shard_mapping.column_name,
                    range_sharding_spec.lower_bound,
                    shard_mapping.column_name,
                    upper_bound,
                    prune_limit
                )
            else:
                delete_query = (
                    SHARDING_DATATYPE_HANDLER[type_name].\
                        PRUNE_SHARD_WITHOUT_UPPER_BOUND) % (
                    table_name,
                    shard_mapping.column_name,
                    range_sharding_spec.lower_bound,
                    prune_limit
                )

            shard = Shards.fetch(range_sharding_spec.shard_id)
            if shard is None:
                raise _errors.ShardingError(
                    "Shard not found (%s)" %
                    (range_sharding_spec.shard_id, )
                )

            group = Group.fetch(shard.group_id)
            if group is None:
                raise _errors.ShardingError(
                    "Group not found (%s)" %
                    (shard.group_id, )
                )

            master = MySQLServer.fetch(group.master)
            if master is None:
                raise _errors.ShardingError(
                    "Group Master not found (%s)" %
                    (str(group.master))
                )

            master.connect()

            #Dependencies between tables being pruned may lead to the prune
            #failing. Hence we need to disable foreign key checks during the
            #pruning process.
            master.set_foreign_key_checks(False)
            prune_limit = int(prune_limit)
            deleted = prune_limit
            while deleted == prune_limit:
                delete_cursor = master.exec_stmt(
                    delete_query, {"fetch":False}
                )
                deleted = delete_cursor.rowcount
            #Enable Foreign Key Checking
            master.set_foreign_key_checks(True)

class HashShardingSpecification(RangeShardingSpecification):
    """Represents a HASH sharding specification. The class helps encapsulate
    the representation of a typical HASH sharding implementation and is built
    upon the RANGE implementation. They share the same table in the state
    store.
    """

    #Insert a HASH of keys and the server to which they belong.
    INSERT_HASH_SPECIFICATION = (
        "INSERT INTO shard_ranges("
        "shard_mapping_id, "
        "lower_bound, "
        "shard_id) "
        "VALUES(%s, UNHEX(MD5(%s)), %s)"
    )

    #Insert Split ranges.
    #NOTE: The split lower_bound does not need the md5 algorithm.
    INSERT_HASH_SPLIT_SPECIFICATION = (
        "INSERT INTO shard_ranges("
        "shard_mapping_id, "
        "lower_bound, "
        "shard_id) "
        "VALUES(%s, UNHEX(%s), %s)"
    )

    #Given a Shard ID select the RANGE Scheme that it defines.
    SELECT_RANGE_SPECIFICATION = (
        "SELECT "
        "shard_mapping_id, "
        "HEX(lower_bound), "
        "shard_id "
        "FROM shard_ranges "
        "WHERE shard_id = %s"
    )

    #Given a Shard Mapping ID select all the RANGE mappings that it
    #defines.
    LIST_RANGE_SPECIFICATION = (
        "SELECT "
        "shard_mapping_id, "
        "HEX(lower_bound), "
        "shard_id "
        "FROM shard_ranges "
        "WHERE shard_mapping_id = %s"
    )

    #Select the least LOWER BOUND of all the LOWER BOUNDs for
    #a given shard mapping ID.
    SELECT_LEAST_LOWER_BOUND = (
        "SELECT MIN(HEX(lower_bound)) FROM "
        "shard_ranges "
        "WHERE shard_mapping_id = %s"
        )

    def __init__(self, shard_mapping_id, lower_bound, shard_id):
        """Initialize a given HASH sharding mapping specification.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param lower_bound: The lower bound of the given HASH sharding
                            definition.
        :param shard_id: An unique identification, a logical representation
                        for a shard of a particular table.
        """
        super(HashShardingSpecification, self).__init__(
            shard_mapping_id,
            lower_bound,
            shard_id
        )

    @staticmethod
    def fetch_max_key(shard_id):
        """Fetch the maximum value of the key in this shard. This will
        be used for the shards in the boundary that do not have a upper_bound
        to compare with.

        @param shard_id: The ID of the shard, from which the maximum value
                        of the tables need to be returned.

        :return: A dictionary of maximum values belonging to each of the tables
                in the shard.
        """
        #Store the list of max_keys from all the tables that belong to this
        #shard.
        max_keys = []

        hash_sharding_spec = HashShardingSpecification.fetch(shard_id)
        if hash_sharding_spec is None:
            raise _errors.ShardingError("No shards associated with this"
                                                         " shard mapping ID.")

        shard = Shards.fetch(shard_id)

        shard_mappings = \
            ShardMapping.fetch_by_id(hash_sharding_spec.shard_mapping_id)
        if shard_mappings is None:
            raise _errors.ShardingError("Shard Mapping not found.")

        for shard_mapping in shard_mappings:
            max_query = "SELECT MD5(MAX(%s)) FROM %s" % \
                        (
                        shard_mapping.column_name,
                        shard_mapping.table_name
                        )

            shard = Shards.fetch(shard_id)
            if shard is None:
                raise _errors.ShardingError(
                    "Shard not found (%s)" %
                    (hash_sharding_spec.shard_id, )
                )

            group = Group.fetch(shard.group_id)
            if group is None:
                raise _errors.ShardingError(
                    "Group not found (%s)" %
                    (shard.group_id, )
                )

            master = MySQLServer.fetch(group.master)
            if master is None:
                raise _errors.ShardingError(
                    "Group Master not found (%s)" %
                    (group.master, )
                )

            master.connect()

            cur = master.exec_stmt(max_query, {"fetch" : False})

            row = cur.fetchone()

            if row is not None:
                max_keys.append(str(row[0]))

        #max_keys stores  all the maximum values in all the tables. We will
        #fetch the maximum values among all these values and use it as the
        #maximum values for the given shard_id.
        max_key = None
        for key in max_keys:
            if max_key is None:
                max_key = key
            elif int(key, 16) > int(max_key, 16):
                max_key = key
            else:
                pass
        #Return the maximum value among all the maximum values from
        #all the tables.
        return max_key

    @staticmethod
    def add(shard_mapping_id, shard_id, persister=None):
        """Add the HASH shard specification. This represents a single instance
        of a shard specification that maps a key HASH to a server.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param shard_id: An unique identification, a logical representation
                        for a shard of a particular table.
        """
        shard = Shards.fetch(shard_id)
        persister.exec_stmt(
            HashShardingSpecification.INSERT_HASH_SPECIFICATION, {
                "params":(
                    shard_mapping_id,
                    shard.group_id,
                    shard_id
                )
            }
        )

    @staticmethod
    def add_hash_split(shard_mapping_id, shard_id, lower_bound, persister=None):
        """Add the HASH shard specification after a split. This represents a
        single instance of a shard specification that maps a key HASH to a
        server.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param shard_id: An unique identification, a logical representation
                         for a shard of a particular table.
        :param lower_bound: The new lower_bound being inserted.
        """
        persister.exec_stmt(
            HashShardingSpecification.INSERT_HASH_SPLIT_SPECIFICATION, {
                "params":(
                    shard_mapping_id,
                    lower_bound,
                    shard_id
                )
            }
        )

    @staticmethod
    def lookup(key, shard_mapping_id, type, persister=None):
        """Return the Hash sharding specification in whose hashed key range
        the input key falls.

        :param key: The key which needs to be checked to see which range it
                    falls into
        :param shard_mapping_id: The unique identification for a shard mapping.
        :param persister: A valid handle to the state store.

        :return: The Hash Sharding Specification that contains the range in
                which the key belongs.
        """
        cur = persister.exec_stmt(SHARDING_DATATYPE_HANDLER[type].LOOKUP_KEY, {
                        "fetch" : False,
                        "params" : (
                            key,
                            shard_mapping_id,
                            shard_mapping_id
                        )
                    }
                )

        row = cur.fetchone()
        if row is None:
            return None
        return HashShardingSpecification(row[0], row[1],  row[2])

    @staticmethod
    def fetch(shard_id, persister=None):
        """Return the HashShardingSpecification object corresponding to the
        given sharding ID.

        :param shard_id: The unique identification for a shard.
        :param persister: A valid handle to the state store.

        :return: The HashShardingSpecification object.
                    An Empty list otherwise.
        """
        cur = persister.exec_stmt(
                    HashShardingSpecification.SELECT_RANGE_SPECIFICATION,
                        {"fetch" : False,
                        "params" : (shard_id,)})
        row = cur.fetchone()
        if row is None:
            return None
        return HashShardingSpecification(row[0], row[1],  row[2])


    @staticmethod
    def fetch_least_lower_bound(shard_mapping_id, persister=None):
        """Fetch the least of the lower_bound's for a given shard mapping ID.

        :param shard_mapping_id: The unique identification for a shard mapping.

        :return: The least lower_bound for a given shard mapping ID.
        """
        cur = persister.exec_stmt(
                    HashShardingSpecification.SELECT_LEAST_LOWER_BOUND,
                        {"fetch" : False,
                        "params" : (shard_mapping_id,)})
        row = cur.fetchone()
        if row is None:
            return None
        return row[0]

    @staticmethod
    def list(shard_mapping_id, persister=None):
        """Return the HashShardingSpecification objects corresponding to the
        given sharding scheme.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param persister: A valid handle to the state store.

        :return: A  list of HashShardingSpecification objects which belong to
                to this shard mapping.
                None if the shard mapping is not found
        """
        cur = persister.exec_stmt(
                    HashShardingSpecification.LIST_RANGE_SPECIFICATION,
                        {"fetch" : False,
                        "params" : (shard_mapping_id,)})
        rows = cur.fetchall()
        return [ HashShardingSpecification(*row[0:5]) for row in rows ]

    @staticmethod
    def create(persister=None):
        """We use the relations used to store the RANGE sharding data. Hence
        this method is a dummy here. We need a dummy implementation since
        the framework invokes this method to create the schemas necessary for
        storing relevant data, and we have no schemas.

        :param persister: A valid handle to the state store.
        """
        pass

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints on the sharding tables. We use a dummy
        implementation here.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        pass

    @staticmethod
    def delete_from_shard_db(table_name, type_name, prune_limit):
        """Delete the data from the copied data directories based on the
        sharding configuration uploaded in the sharding tables of the state
        store. The basic logic consists of

        * Querying the shard mapping ID corresponding to the sharding
          table.
        * Using the shard mapping ID to find the type of shard scheme and hence
            the sharding scheme table to query in.
        * Querying the sharding key range using the shard mapping ID.
        * Deleting the sharding keys that fall outside the range for a given
          server.

        :param table_name: The table being sharded.
        :param type_name: The type of the sharding definition.
        :param prune_limit: The number of DELETEs that should be
                            done in one batch.
        """

        shard_mapping = ShardMapping.fetch(table_name)
        if shard_mapping is None:
            raise _errors.ShardingError("Shard Mapping not found.")

        shard_mapping_id = shard_mapping.shard_mapping_id

        shards = HashShardingSpecification.list(shard_mapping_id)
        if not shards:
            raise _errors.ShardingError("No shards associated with this"
                                        " shard mapping ID.")
        for shard in shards:
            HashShardingSpecification.prune_shard_id(shard.shard_id, type_name,
                                                     prune_limit)

    @staticmethod
    def get_upper_bound(lower_bound, shard_mapping_id, type, persister=None):
        """Return the next value in range for a given lower_bound value.
        This basically helps to form a (lower_bound, upper_bound) pair
        that can be used during a prune.

        :param lower_bound: The lower_bound value whose next range needs to
                            be retrieved.
        :param shard_mapping_id: The shard_mapping_id whose shards should be
                                searched for the given lower_bound.
        :param persister: A valid handle to the state store.

        :return: The next value in the range for the given lower_bound.
        """
        cur = persister.exec_stmt(
                        SHARDING_DATATYPE_HANDLER[type].SELECT_UPPER_BOUND,
                        {"fetch" : False,
                        "params" : (lower_bound, shard_mapping_id)})

        row = cur.fetchone()

        if row is None:
            return None

        return row[0]

    @staticmethod
    def prune_shard_id(shard_id, type_name, prune_limit):
        """Remove the rows in the shard that do not match the metadata
        in the shard_range tables.

        :param shard_id: The ID of the shard that needs to be pruned.
        :param type_name: The type of the sharding definition.
        :param prune_limit: The number of DELETEs that should be
                            done in one batch.
        """

        hash_sharding_spec = HashShardingSpecification.fetch(shard_id)
        if hash_sharding_spec is None:
            raise _errors.ShardingError("No shards associated with this"
                                                         " shard mapping ID.")

        shard = Shards.fetch(shard_id)

        upper_bound = HashShardingSpecification.get_upper_bound(
                            hash_sharding_spec.lower_bound,
                            hash_sharding_spec.shard_mapping_id,
                            type_name
                        )

        shard_mappings = \
            ShardMapping.fetch_by_id(hash_sharding_spec.shard_mapping_id)
        if shard_mappings is None:
            raise _errors.ShardingError("Shard Mapping not found.")

        for shard_mapping in shard_mappings:
            table_name = shard_mapping.table_name

            if upper_bound is not None:
                delete_query = (
                    SHARDING_DATATYPE_HANDLER[type_name].\
                        PRUNE_SHARD_WITH_UPPER_BOUND
                ) % (
                    table_name,
                    shard_mapping.column_name,
                    hash_sharding_spec.lower_bound,
                    shard_mapping.column_name,
                    upper_bound,
                    prune_limit
                )
            else:
                #HASH based sharding forms a circular ring. Hence
                #when there is no upper_bound, all the values, that
                #circle around from the largest lower_bound to the
                #least upper_bound need to be present in this shard.
                delete_query = (
                    SHARDING_DATATYPE_HANDLER[type_name].\
                        PRUNE_SHARD_WITHOUT_UPPER_BOUND
                ) % (
                    table_name,
                    shard_mapping.column_name,
                    HashShardingSpecification.fetch_least_lower_bound(
                        shard_mapping.shard_mapping_id
                    ),
                    shard_mapping.column_name,
                    hash_sharding_spec.lower_bound,
                    prune_limit
                )

            shard = Shards.fetch(hash_sharding_spec.shard_id)
            if shard is None:
                raise _errors.ShardingError(
                    "Shard not found (%s)" %
                    (hash_sharding_spec.shard_id, )
                )

            group = Group.fetch(shard.group_id)
            if group is None:
                raise _errors.ShardingError(
                    "Group not found (%s)" %
                    (shard.group_id, )
                )

            master = MySQLServer.fetch(group.master)
            if master is None:
                raise _errors.ShardingError(
                    "Group Master not found (%s)" %
                    (group.master, )
                )

            master.connect()

            #Dependencies between tables being pruned may lead to the prune
            #failing. Hence we need to disable foreign key checks during the
            #pruning process.
            master.set_foreign_key_checks(False)
            prune_limit = int(prune_limit)
            deleted = prune_limit
            while deleted == prune_limit:
                delete_cursor = master.exec_stmt(
                    delete_query, {"fetch":False}
                )
                deleted = delete_cursor.rowcount
            #Enable the Foreign Key checks after the prune.
            master.set_foreign_key_checks(True)

class MappingShardsGroups(_persistence.Persistable):
    """This class defines queries that are used to retrieve information
    on groups through a given key which could be a shard identification,
    a shard mapping identification or a table name.

    The group is used by the lock control system to avoid concurrent
    procedures simultaneously accessing the same group thus causing
    problems such as setting up a slave of a wrong master while a
    switch or fail over is going on.
    """
    # Return the local group associated with a shard_id.
    SELECT_LOCAL_GROUP_BY_SHARD_ID = \
        ("SELECT group_id FROM shards WHERE shard_id = %s")

    # Return the global group associated with a shard_id.
    SELECT_GLOBAL_GROUP_BY_SHARD_ID = \
       ("SELECT global_group AS group_id FROM shard_maps WHERE "
        "shard_mapping_id = (SELECT shard_mapping_id FROM shard_ranges WHERE "
        "shard_id = %s LIMIT 1)"
       )

    # Return the local group(s) associated with a shard_mapping_id.
    SELECT_LOCAL_GROUP_BY_SHARD_MAPPING_ID = \
       ("SELECT DISTINCT shards.group_id AS group_id FROM shard_ranges, "
        "shards WHERE shard_ranges.shard_id = shards.shard_id AND "
        "shard_ranges.shard_mapping_id = %s"
       )

    # Return the global group associated with a shard_mapping_id.
    SELECT_GLOBAL_GROUP_BY_SHARD_MAPPING_ID = \
       ("SELECT global_group AS group_id FROM shard_maps WHERE "
        "shard_mapping_id = %s"
       )

    # Return the local group(s) associated with a table_name.
    SELECT_LOCAL_GROUP_BY_TABLE_NAME = \
       ("SELECT DISTINCT shards.group_id AS group_id FROM shard_ranges, "
        "shards WHERE shard_ranges.shard_id = shards.shard_id AND "
        "shard_ranges.shard_mapping_id IN (SELECT shard_mapping_id FROM "
        "shard_tables WHERE table_name = %s)"
       )

    # Return the global group(s) associated with a table_name.
    SELECT_GLOBAL_GROUP_BY_TABLE_NAME = \
       ("SELECT global_group AS group_id FROM shard_maps WHERE "
        "shard_mapping_id = (SELECT shard_mapping_id FROM shard_tables "
        "WHERE table_name = %s)"
       )

    # Index queries through a set of pre-defined information.
    PARAM_QUERIES = {
        "local" :
        {
          "shard_id" : SELECT_LOCAL_GROUP_BY_SHARD_ID,
          "shard_mapping_id" : SELECT_LOCAL_GROUP_BY_SHARD_MAPPING_ID,
          "table_name" : SELECT_LOCAL_GROUP_BY_TABLE_NAME,
        },
        "global" :
        {
          "shard_id" : SELECT_GLOBAL_GROUP_BY_SHARD_ID,
          "shard_mapping_id" : SELECT_GLOBAL_GROUP_BY_SHARD_MAPPING_ID,
          "table_name" : SELECT_GLOBAL_GROUP_BY_TABLE_NAME,
        },
    }

    @staticmethod
    def get_group(locality, criterion, value, persister=None):
        """Return group based on a set of filters.

        :param locality: Determine if the group to be returned is global or
                         local.
        :param criterion: The criterion to retrieve a simple group: shard_id,
                          shard_mapping_id and table_name.
        :param value: Value of the criterion that will be used.
        :return: Rows with the objects requested.
        """
        assert(locality in ("local", "global"))
        assert(criterion in ("shard_id", "shard_mapping_id", "table_name"))
        rows = persister.exec_stmt(
            MappingShardsGroups.PARAM_QUERIES[locality][criterion],
            {"params" : (value, )}
        )
        return rows

SHARDING_DATATYPE_HANDLER = {
    "RANGE": RangeShardingIntegerHandler,
    "RANGE_INTEGER": RangeShardingIntegerHandler,
    "RANGE_STRING": RangeShardingStringHandler,
    "RANGE_DATETIME": RangeShardingDateTimeHandler,
    "HASH": HashShardingHandler
}

SHARDING_SPECIFICATION_HANDLER = {
    "RANGE": RangeShardingSpecification,
    "RANGE_INTEGER": RangeShardingSpecification,
    "RANGE_STRING": RangeShardingSpecification,
    "RANGE_DATETIME": RangeShardingSpecification,
    "HASH": HashShardingSpecification
}
