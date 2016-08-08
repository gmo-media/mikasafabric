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
"""Methods and classes to store Fabric's log entries.
"""
import logging

from mysql.fabric import (
    persistence as _persistence,
    config as _config,
)

from mysql.fabric.utils import (
    get_time_from_timestamp,
)

_LOGGER = logging.getLogger(__name__)

_CREATE_FABRIC_LOG = (
    "CREATE TABLE log "
    "(subject VARCHAR(40) NOT NULL, "
    "reported TIMESTAMP /*!50604 (6) */ NOT NULL, "
    "reporter VARCHAR(64) NOT NULL, "
    "message TEXT, "
    "category int NOT NULL, "
    "type int NOT NULL, "
    "INDEX key_subject_reported (subject, reported), "
    "INDEX key_reporter (reporter), "
    "INDEX key_reported (reported), "
    "INDEX key_category (category), "
    "INDEX key_type (type)) DEFAULT CHARSET=utf8"
)

_CREATE_GROUP_VIEW = (
    "CREATE VIEW group_view AS SELECT out_log.subject as group_id, "
    "(SELECT COUNT(*) FROM log as in_log "
    "  WHERE in_log.subject = out_log.subject "
    "    AND in_log.category = %s AND in_log.type = %s) as promote_count, "
    "(SELECT COUNT(*) FROM log as in_log "
    "  WHERE in_log.subject = out_log.subject "
    "    AND in_log.category = %s AND in_log.type = %s) as demote_count "
    "FROM log as out_log WHERE out_log.category = %s GROUP BY out_log.subject"
)

_SELECT_GROUP_VIEW = (
    "SELECT * from group_view WHERE group_id LIKE %s"
)

_CREATE_EVENT_FABRIC_LOG = (
    "CREATE EVENT prune_log "
    "ON SCHEDULE EVERY %s SECOND "
    "DO DELETE FROM log WHERE "
    "TIMEDIFF(UTC_TIMESTAMP(), reported) > MAKETIME(%s,0,0)"
)

_CREATE_PROCEDURE_VIEW = (
    "CREATE VIEW proc_view AS SELECT out_log.subject as proc_name, "
    "(SELECT COUNT(*) FROM log as in_log "
    "  WHERE in_log.subject = out_log.subject "
    "    AND in_log.category = %s AND in_log.type = %s) as call_count, "
    "(SELECT COUNT(*) FROM log as in_log "
    "  WHERE in_log.subject = out_log.subject "
    "    AND in_log.category = %s AND in_log.type = %s) as abort_count "
    "FROM log as out_log WHERE out_log.category = %s GROUP BY out_log.subject"
)

_SELECT_PROCEDURE_VIEW = (
    "SELECT * FROM proc_view WHERE proc_name LIKE %s"
)

_INSERT_FABRIC_LOG = (
    "INSERT INTO log (subject, reported, reporter, "
    "message, category, type) VALUES(%s, %s, %s, %s, %s, %s)"
)


class MySQLFilter(logging.Filter):
    """Filter records that are not supposed to be written to the logging
    table.
    """
    def __init__(self, name):
        """Constructor for the MySQLFilter.
        """
        logging.Filter.__init__(self, name)

    def filter(self, record):
        """Filter a record that is not supposed to be written to the logging
        table. If the arguments to format the record does not contain subject,
        the record is ignored.
        """
        try:
            record.subject
        except AttributeError:
            return 0

        return 1


class MySQLHandler(logging.Handler, _persistence.Persistable):
    """Write information to the logging table if the appropriate options are
    set by the user. For example::

        logger.debug("Group %s has promote server %s to master.",
            group_id, server.uuid,
            extra={
                'subject' : group_id,
                'category' : MySQLHandler.GROUP,
                'type' : MySQLHandler.PROMOTE
            }
        )

    Users willing to write information to the logging table must specify the
    subject, category and type paramters as entries in a dictionary that is
    passed to the logging function through the extra parameter.

    The subject identifies the entity that the information is referring to,
    whereas the subject classifies the entity and the type defines what has
    been reported. In the aforementioned code, the subject is the group
    identifier, the category defines that information on a group is being
    reported and the type says that a new master is being promoted.

    The following categories are supported:

    * GROUP - Defines that information is about a group.
    * NODE - Defines that information is about the Fabric Node.
    * PROCEDURE - Defines that information is about a procedure.
    * SERVER - Defines that information is about a server.

    The following types are supported:

    * START - Defines that an entity has been started.
    * STOP - Defines that an entity has been stopped.
    * ABORT - Defines that an entity has been aborted. This is applicable
              to procedures.
    * PROMOTE - Defines that an entity has been promoted. This is applicable
                to a server or group.
    * DEMOTE - Defines that an entity has been demoted. This is applicable
               to a server or group.

    It is also possible to define the reporter and reported parameters. They
    define who has reported the information and when. If the reporter is not
    set, the name of the logger used to log the call is used. If the reported
    is not set, the time when the logging information was created is used.
    """
    MIN_PRUNE_TIME = 60
    PRUNE_TIME = _DEFAULT_PRUNE_TIME = 3600

    NODE = "NODE"
    PROCEDURE = "PROCEDURE"
    SERVER = "SERVER"
    GROUP = "GROUP"

    CATEGORIES = [ NODE, PROCEDURE, SERVER, GROUP ]

    START = "START"
    STOP = "STOP"
    ABORT = "ABORT"
    PROMOTE = "PROMOTE"
    DEMOTE = "DEMOTE"

    TYPES = [ START, STOP, ABORT, PROMOTE, DEMOTE ]

    @staticmethod
    def create(persister=None):
        """Create the objects(tables) that will store Fabric's logs.

        :param persister: Persister to persist the object to.
        :raises: DatabaseError If the table already exists.
        """
        persister.exec_stmt(_CREATE_FABRIC_LOG)
        persister.exec_stmt(
            _CREATE_EVENT_FABRIC_LOG % (MySQLHandler.PRUNE_TIME,
            MySQLHandler.PRUNE_TIME)
        )
        persister.exec_stmt(
            _CREATE_GROUP_VIEW % (
            MySQLHandler.idx_category(MySQLHandler.GROUP),
            MySQLHandler.idx_type(MySQLHandler.PROMOTE),
            MySQLHandler.idx_category(MySQLHandler.GROUP),
            MySQLHandler.idx_type(MySQLHandler.DEMOTE),
            MySQLHandler.idx_category(MySQLHandler.GROUP))
        )
        persister.exec_stmt(
            _CREATE_PROCEDURE_VIEW % (
            MySQLHandler.idx_category(MySQLHandler.PROCEDURE),
            MySQLHandler.idx_type(MySQLHandler.START),
            MySQLHandler.idx_category(MySQLHandler.PROCEDURE),
            MySQLHandler.idx_type(MySQLHandler.ABORT),
            MySQLHandler.idx_category(MySQLHandler.PROCEDURE))
        )

    @staticmethod
    def add(subject, reported, reporter, info, info_category,
            info_type, persister=None):
        """Add a Fabric's log entry.
        """
        persister.exec_stmt(_INSERT_FABRIC_LOG,
            {"params": (subject, reported, reporter, info, info_category,
            info_type)}
        )

    @staticmethod
    def group_view(group_id=None, persister=None):
        """Fetch information on a Group.

        The following information is returned:

            +----------------+----------------------+
            |    Column      |     Description      |
            +================+======================+
            |   group_id     | Group identification |
            +----------------+----------------------+
            |  promote_count | Many times a new     |
            |                | master was promoted  |
            +----------------+----------------------+
            |  demote_count  | Many times a master  |
            |                | was demoted          |
            +----------------+----------------------+

        :param group_id: Group identifier.
        :param persister: Persister to persist the object to.
        """
        like_pattern = None
        if not group_id:
            like_pattern = '%%'
        else:
            like_pattern = '%' + group_id + '%'

        res = persister.exec_stmt(_SELECT_GROUP_VIEW,
            {"params": (like_pattern, )}
        )
        return res

    @staticmethod
    def procedure_view(procedure_name=None, persister=None):
        """Fetch information on a Procedure.

        The following information is returned:

            +--------------+----------------------+
            |    Column    |     Description      |
            +==============+======================+
            |   proc_name  |    Procedure name    |
            +--------------+----------------------+
            |   call_count | Number of times it   |
            |              | was called           |
            +--------------+----------------------+
            |   call_abort | Number of times it   |
            |              | has failed           |
            +--------------+----------------------+

        :param procedure_name: Procedure name.
        :param persister: Persister to persist the object to.
        """
        like_pattern = None
        if not procedure_name:
            like_pattern = '%%'
        else:
            like_pattern = '%' + procedure_name + '%'

        res = persister.exec_stmt(_SELECT_PROCEDURE_VIEW,
            {"params":(like_pattern, )}
        )
        return res

    def __init__(self):
        """Constructor for MySQLHandler object.
        """
        logging.Handler.__init__(self)
        _persistence.Persistable.__init__(self)
        self.addFilter(MySQLFilter("MySQLHandler"))

    def emit(self, record):
        """Write an entry to the logging table if the extern key was provided as
        part of the extra dictionary and yields True.
        """
        try:
            info_subject = record.subject
            info_category = MySQLHandler.idx_category(record.category)
            info_type = MySQLHandler.idx_type(record.type)

            try:
                info_reporter = record.reporter
            except AttributeError:
                info_reporter = record.name

            try:
                info_reported = record.reported
            except AttributeError:
                info_reported = get_time_from_timestamp(record.created)

            MySQLHandler.add(
                info_subject, info_reported, info_reporter,
                self.format(record), info_category, info_type
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def flush(self):
        """Flush information to the logging table. There is no need to do
        anything here.
        """
        pass

    def close(self):
        """Close the handler.
        """
        logging.Handler.close(self)

    @staticmethod
    def idx_category(info_category):
        """Return an int representing a category.
        """
        return MySQLHandler.CATEGORIES.index(info_category)

    @staticmethod
    def idx_type(info_type):
        """Return an int representing a type.
        """
        return MySQLHandler.TYPES.index(info_type)

def configure(config):
    """Set configuration values.
    """
    try:
        prune_time = int(config.get("statistics", "prune_time"))
        if prune_time < MySQLHandler.MIN_PRUNE_TIME:
            _LOGGER.warning(
                "Prune entries cannot be lower than %s.",
                MySQLHandler.MIN_PRUNE_TIME
            )
            prune_time = MySQLHandler.MIN_PRUNE_TIME
        MySQLHandler.PRUNE_TIME = int(prune_time)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass
