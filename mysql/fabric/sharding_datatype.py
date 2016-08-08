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

"""This module contains the logic necessary for handling heterogenous
datatypes in sharding.
"""

import mysql.fabric.errors as _errors
import mysql.fabric.persistence as _persistence

class ShardingDatatypeHandler(_persistence.Persistable):
    """This is a base class that defines the attributes and methods that
    need to be implemented by a sharding datatype handler. The other
    classes that handle the sharding datatypes extend this class and
    implement the methods of this class.
    """
    #Select the server corresponding to the RANGE to which a given key
    #belongs. The query either selects the least lower_bound that is larger
    #than a given key or selects the largest lower_bound and insert the key
    #in that shard.
    LOOKUP_KEY = ""

    #Select the UPPER BOUND for a given LOWER BOUND value.
    SELECT_UPPER_BOUND = ""

    #Prune shard with upper bound
    PRUNE_SHARD_WITH_UPPER_BOUND = ""

    #Prune shard without upper bound
    PRUNE_SHARD_WITHOUT_UPPER_BOUND = ""

    @staticmethod
    def is_valid_lower_bound(lower_bound):
        """Verify if the given value is a valid INTEGER lower bound.

        :param lower_bound: The value that needs to be verified.
        """
        return True

    @staticmethod
    def split_value(lower_bound, upper_bound):
        """Find the middle value of a sharding range that can be used when
        the split value is not explicitly specified.

        :param lower_bound: The lower bound of the shard range.
        :param upper_bound: The upper bound of the shard range.
        """
        return ""

    @staticmethod
    def is_valid_split_value(split_value, lower_bound, upper_bound,
                             persister=None):
        """Verify if the given split value for a RANGE sharding definition is
        valid.

        :param split_value: The split value.
        :param lower_bound: The lower bound of the sharding definition.
        :param upper_bound: The upper bound of the sharding definition.

        :return: True - If the split value is valid
                False - If the split value is not valid
        """
        return True

class RangeShardingIntegerHandler(ShardingDatatypeHandler):
    """Contains the members that are required to handle a sharding definition
    based on an INTEGER datatype.
    """

   #Select the server corresponding to the RANGE to which a given key
    #belongs. The query either selects the least lower_bound that is larger
    #than a given key or selects the largest lower_bound and insert the key
    #in that shard.
    LOOKUP_KEY = (
        "SELECT "
        "sr.shard_mapping_id, "
        "sr.lower_bound, "
        "s.shard_id "
        "FROM "
        "shard_ranges AS sr, shards AS s "
        "WHERE %s >= CAST(lower_bound AS SIGNED) "
        "AND sr.shard_mapping_id = %s "
        "AND s.shard_id = sr.shard_id "
        "ORDER BY CAST(lower_bound AS SIGNED) DESC "
        "LIMIT 1"
    )

    #Select the UPPER BOUND for a given LOWER BOUND value.
    SELECT_UPPER_BOUND = (
        "SELECT lower_bound FROM "
        "shard_ranges "
        "WHERE  CAST(lower_bound AS SIGNED) > %s AND shard_mapping_id = %s "
        "ORDER BY  CAST(lower_bound AS SIGNED) ASC LIMIT 1"
    )

    #Prune shard with upper bound
    PRUNE_SHARD_WITH_UPPER_BOUND = (
        "DELETE FROM %s WHERE %s < %s OR %s >= %s LIMIT %s"
    )

    #Prune shard without upper bound
    PRUNE_SHARD_WITHOUT_UPPER_BOUND = (
        "DELETE FROM %s WHERE %s < %s LIMIT %s"
    )

    @staticmethod
    def is_valid_lower_bound(lower_bound):
        """Verify if the given value is a valid INTEGER lower bound.

        :param lower_bound: The value that needs to be verified.
        """
        try:
            int(lower_bound)
            return True
        except ValueError:
            return False

    @staticmethod
    def split_value(lower_bound, upper_bound):
        """Find the middle value of a sharding range that can be used when
        the split value is not explicitly specified.

        :param lower_bound: The lower bound of the shard range.
        :param upper_bound: The upper bound of the shard range.
        """
        #If the underlying sharding specification is a RANGE, and the
        #split value is not given, then calculate it as the mid value
        #between the current lower_bound and its next lower_bound.
        lower_bound = int(lower_bound)
        upper_bound = int(upper_bound)
        split_value = lower_bound + (upper_bound - lower_bound) / 2
        return split_value

    @staticmethod
    def is_valid_split_value(split_value, lower_bound, upper_bound,
                             persister=None):
        """Verify if the given split value for a RANGE sharding definition is
        valid.

        :param split_value: The split value.
        :param lower_bound: The lower bound of the sharding definition.
        :param upper_bound: The upper bound of the sharding definition.

        :return: True - If the split value is valid
                False - If the split value is not valid
        """
        #A split value needs to be a valid lower_bound value and should
        #lie between the lower bound and the upper bound values
        if not RangeShardingIntegerHandler.is_valid_lower_bound(split_value):
            return False
        if (int(split_value) > int(lower_bound)) is not True:
            return False
        if upper_bound is not None:
            if (int(split_value) < int(upper_bound)) is not True:
                return False
        return True

class RangeShardingStringHandler(ShardingDatatypeHandler):
    """Contains the members that are required to handle a sharding definition
    based on an STRING datatype.
    """

    #Character set to be used in the state store queries
    CHARACTER_SET = "utf8"

    #Character set encoding to be used in the state store queries.
    COLLATION = "utf8_bin"

    #Select the server corresponding to the RANGE to which a given key
    #belongs. The query either selects the least lower_bound that is larger
    #than a given key or selects the largest lower_bound and insert the key
    #in that shard.
    LOOKUP_KEY = (
        "SELECT "
        "sr.shard_mapping_id, "
        "sr.lower_bound, "
        "s.shard_id "
        "FROM "
        "shard_ranges AS sr, shards AS s "
        "WHERE "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} >= "
        "CAST(lower_bound AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} "
        "AND sr.shard_mapping_id = %s "
        "AND s.shard_id = sr.shard_id "
        "ORDER BY CAST"
        "(lower_bound AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} DESC "
        "LIMIT 1".format(SHARDING_CHARACTER_SET=CHARACTER_SET,
                         SHARDING_COLLATION=COLLATION)
    )

    #Select the UPPER BOUND for a given LOWER BOUND value.
    SELECT_UPPER_BOUND = (
        "SELECT lower_bound FROM "
        "shard_ranges "
        "WHERE "
        "CAST(lower_bound AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} > "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} "
        "AND "
        "shard_mapping_id = %s "
        "ORDER BY "
        "CAST(lower_bound AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} "
        "ASC LIMIT 1".format(SHARDING_CHARACTER_SET=CHARACTER_SET,
                         SHARDING_COLLATION=COLLATION)
    )

    #Prune shard with upper bound
    PRUNE_SHARD_WITH_UPPER_BOUND = (
        "DELETE FROM %s "
        "WHERE "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) < '%s' "
        "OR "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) >= '%s' "
        "LIMIT %s"
        .format(SHARDING_CHARACTER_SET=CHARACTER_SET,
                SHARDING_COLLATION=COLLATION)
    )

    #Prune shard without upper bound
    PRUNE_SHARD_WITHOUT_UPPER_BOUND = (
        "DELETE FROM %s "
        "WHERE "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) < '%s' "
        "LIMIT %s"
        .format(SHARDING_CHARACTER_SET=CHARACTER_SET,
                         SHARDING_COLLATION=COLLATION)
    )

    #Verify if the value used for splitting a shard falls within
    #the upper bound and lower bound definition for that shard.
    VERIFY_SPLIT_VALUE_VALID_WITH_UPPER_BOUND = (
        "SELECT "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} < "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} "
        "AND "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} > "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} "
        "FROM "
        "shard_ranges".format(SHARDING_CHARACTER_SET=CHARACTER_SET,
                         SHARDING_COLLATION=COLLATION)
    )

    #Verify if the value used for splitting a shard is greater than the lower
    #bound of a shard.
    VERIFY_SPLIT_VALUE_VALID_WITHOUT_UPPER_BOUND = (
        "SELECT "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} < "
        "CAST(%s AS CHAR CHARACTER SET {SHARDING_CHARACTER_SET}) "
        "COLLATE {SHARDING_COLLATION} "
        "FROM "
        "shard_ranges".format(SHARDING_CHARACTER_SET=CHARACTER_SET,
                         SHARDING_COLLATION=COLLATION)
    )

    @staticmethod
    def is_valid_lower_bound(lower_bound):
        """Verify if the given value is a valid STRING lower bound.

        :param lower_bound: The value that needs to be verified.
        """
        return type(lower_bound) is str

    @staticmethod
    def split_value(lower_bound, upper_bound):
        """For a Range Sharding definition on string, the split value should,
        be explicitly defined.

        :param lower_bound: The lower bound of the shard range.
        :param upper_bound: The upper bound of the shard range.
        """
        raise _errors.ShardingError("The split value should be defined")

    @staticmethod
    def is_valid_split_value(split_value, lower_bound, upper_bound,
                             persister=None):
        """For a Range definition on String, the validity of the lower_bound is
        at the state store level and not at the Fabric server level. We use a
        query on the state store to determine if the lower bound is valid.

        :param split_value: The split value.
        :param lower_bound: The lower bound of the sharding definition.
        :param upper_bound: The upper bound of the sharding definition.

        :return: True - If the split value is valid
                False - If the split value is not valid
        """
        if upper_bound is not None:
            row = persister.exec_stmt(
                RangeShardingStringHandler.
                VERIFY_SPLIT_VALUE_VALID_WITH_UPPER_BOUND,
                {"params":(lower_bound, split_value, upper_bound, split_value,)}
            )
        else:
            row = persister.exec_stmt(
                RangeShardingStringHandler.
                VERIFY_SPLIT_VALUE_VALID_WITHOUT_UPPER_BOUND,
                {"params":(lower_bound, split_value, )}
            )
        return row[0][0] == 1

class HashShardingHandler(ShardingDatatypeHandler):
    """Contains the members that are required to handle a hash based
    sharding definition.
    """
    #Fetch the shard ID corresponding to the input key.
    LOOKUP_KEY = (
                "("
                "SELECT "
                "sr.shard_mapping_id, "
                "HEX(sr.lower_bound) AS lower_bound, "
                "s.shard_id "
                "FROM shard_ranges AS sr, shards AS s "
                "WHERE MD5(%s) >= HEX(sr.lower_bound) "
                "AND sr.shard_mapping_id = %s "
                "AND s.shard_id = sr.shard_id "
                "ORDER BY HEX(sr.lower_bound) DESC "
                "LIMIT 1"
                ") "
                "UNION ALL "
                "("
                "SELECT "
                "sr.shard_mapping_id, "
                "HEX(sr.lower_bound) AS lower_bound, "
                "sr.shard_id "
                "FROM shard_ranges AS sr, shards AS s "
                "WHERE sr.shard_mapping_id = %s "
                "AND s.shard_id = sr.shard_id "
                "ORDER BY HEX(sr.lower_bound) DESC "
                "LIMIT 1"
                ") "
                "ORDER BY HEX(lower_bound) ASC "
                "LIMIT 1"
                )

    #Select the UPPER BOUND for a given LOWER BOUND value.
    SELECT_UPPER_BOUND = (
        "SELECT HEX(lower_bound) FROM "
        "shard_ranges "
        "WHERE "
        "HEX(lower_bound) > %s "
        "AND "
        "shard_mapping_id = %s "
        "ORDER BY HEX(lower_bound) ASC LIMIT 1"
    )

    #Prune shard with upper bound
    PRUNE_SHARD_WITH_UPPER_BOUND = (
        "DELETE FROM %s WHERE MD5(%s) < '%s' OR MD5(%s) >= '%s' "
        "LIMIT %s"
    )

    #Prune shard without upper bound
    PRUNE_SHARD_WITHOUT_UPPER_BOUND = (
        "DELETE FROM %s WHERE MD5(%s) >= '%s' AND MD5(%s) < '%s' "
        "LIMIT %s"
    )

    @staticmethod
    def is_valid_lower_bound(lower_bound):
        """Lower bounds in hash based sharding are autogenerated.
        This method is thus a no-op for hash based sharding.
        """
        return False

    @staticmethod
    def split_value(lower_bound, upper_bound):
        """Calculate the split value for a hash based shard as the mid-point
        of the lower and upper bounds of the current shard.

        :param lower_bound: The lower bound of the hash based shard.
        :param upper_bound: The upper bound of the hash based shard.

        :return: Hexadecimal representatin of the mid values.
        """
        #Retrieve an integer representation of the hexadecimal
        #lower_bound. The value is actually a long. Python automatically
        #returns a long value.
        lower_bound = int(str(lower_bound), 16)
        upper_bound = int(str(upper_bound), 16)
        #split value after the below computation is actually a long.
        split_value = lower_bound + (upper_bound - lower_bound) / 2
        #split_value after the hex computation gets stored with a prefix
        #0x indicating a hexadecimal value and a suffix of L indicating a
        #Long. Extract the hexadecimal string from this value.
        split_value = "%x" % (split_value)
        return split_value

    @staticmethod
    def is_valid_split_value(split_value, lower_bound, upper_bound,
                             persister=None):
        """For a Hash sharding definition, the lower_bound is determined
        automatically. Hence return True.

        :param split_value: The split value.
        :param lower_bound: The lower bound of the sharding definition.
        :param upper_bound: The upper bound of the sharding definition.
        :param persister: A valid handle to the state store.

        :return: True - If the split value is valid
                False - If the split value is not valid
        """
        return True

class RangeShardingDateTimeHandler(ShardingDatatypeHandler):
    """Contains the members that are required to handle a DATETIME based
    RANGE sharding definition.
    """
    #Verify if the DATETIME value is in the proper format.
    VERIFY_DATE_TIME_VALID = ("SELECT CAST(%s AS DATETIME)")

    #Select the server corresponding to the RANGE to which a given key
    #belongs. The query either selects the least lower_bound that is larger
    #than a given key or selects the largest lower_bound and insert the key
    #in that shard.
    LOOKUP_KEY = (
        "SELECT "
        "sr.shard_mapping_id, "
        "sr.lower_bound, "
        "s.shard_id "
        "FROM "
        "shard_ranges AS sr, shards AS s "
        "WHERE "
        "CAST(%s AS DATETIME) >= "
        "CAST(sr.lower_bound AS DATETIME) "
        "AND sr.shard_mapping_id = %s "
        "AND s.shard_id = sr.shard_id "
        "ORDER BY CAST(sr.lower_bound AS DATETIME) "
        "LIMIT 1"
    )

    #Select the UPPER BOUND for a given LOWER BOUND value.
    SELECT_UPPER_BOUND = (
        "SELECT lower_bound FROM "
        "shard_ranges "
        "WHERE "
        "CAST(lower_bound AS DATETIME) > "
        "CAST(%s AS DATETIME) "
        "AND "
        "shard_mapping_id = %s "
        "ORDER BY "
        "CAST(lower_bound AS DATETIME) "
        "ASC LIMIT 1"
    )

    #Prune shard with upper bound
    PRUNE_SHARD_WITH_UPPER_BOUND = (
        "DELETE FROM %s WHERE "
        "CAST(%s AS DATETIME) < "
        "CAST('%s' AS DATETIME) OR "
        "CAST(%s AS DATETIME) >= "
        "CAST('%s' AS DATETIME) "
        "LIMIT %s"
    )

    #Prune shard without upper bound
    PRUNE_SHARD_WITHOUT_UPPER_BOUND = (
        "DELETE FROM %s WHERE "
        "CAST(%s AS DATETIME) < "
        "CAST('%s' AS DATETIME) "
        "LIMIT %s"
    )

    #Verify if the value used for splitting a shard falls within
    #the upper bound and lower bound definition for that shard.
    VERIFY_SPLIT_VALUE_VALID_WITH_UPPER_BOUND = (
        "SELECT "
        "CAST(%s AS DATETIME) < "
        "CAST(%s AS DATETIME) "
        "AND "
        "CAST(%s AS DATETIME) > "
        "CAST(%s AS DATETIME) "
        "FROM "
        "shard_ranges"
    )

    #Verify if the value used for splitting a shard is greater than the lower
    #bound of a shard.
    VERIFY_SPLIT_VALUE_VALID_WITHOUT_UPPER_BOUND = (
        "SELECT "
        "CAST(%s AS DATETIME) < "
        "CAST(%s AS DATETIME) "
        "FROM "
        "shard_ranges"
    )

    @staticmethod
    def split_value(lower_bound, upper_bound):
        """For a Range Sharding definition on DATETIME, the split value should,
        be explicitly defined.

        :param lower_bound: The lower bound of the shard range.
        :param upper_bound: The upper bound of the shard range.
        """
        raise _errors.ShardingError("The split value should be defined")

    @staticmethod
    def is_valid_lower_bound(lower_bound, persister=None):
        """Verify if the given value is a valid DATETIME lower bound.

        :param lower_bound: The value that needs to be verified.
        :param persister: A valid handle to the state store.

        :return: True - If the split value is valid
                False - If the split value is not valid
        """
        row = persister.exec_stmt(
            RangeShardingDateTimeHandler.VERIFY_DATE_TIME_VALID,
            {"params":(lower_bound,) }
        )
        return row[0][0] is not None

    @staticmethod
    def is_valid_split_value(split_value, lower_bound, upper_bound,
                             persister=None):
        """For a Range definition on String, the validity of the lower_bound is
        at the state store level and not at the Fabric server level. We use a
        query on the state store to determine if the lower bound is valid.

        :param split_value: The split value.
        :param lower_bound: The lower bound of the sharding definition.
        :param upper_bound: The upper bound of the sharding definition.
        :param persister: A valid handle to the state store.

        :return: True - If the split value is valid
                False - If the split value is not valid
        """
        if upper_bound is not None:
            row = persister.exec_stmt(
                RangeShardingDateTimeHandler.\
                    VERIFY_SPLIT_VALUE_VALID_WITH_UPPER_BOUND,
                {"params":(lower_bound, split_value, upper_bound, split_value,)})
        else:
            row = persister.exec_stmt(
                RangeShardingDateTimeHandler.\
                    VERIFY_SPLIT_VALUE_VALID_WITHOUT_UPPER_BOUND,
                {"params":(lower_bound, split_value,)})
        return row[0][0] == 1
