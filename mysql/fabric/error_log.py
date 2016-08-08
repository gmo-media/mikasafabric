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
"""Methods and classes to report servers' errors.
"""
import logging

from mysql.fabric import (
    persistence as _persistence,
    config as _config,
)

from mysql.fabric.utils import (
   get_time,
)

_LOGGER = logging.getLogger(__name__)

class ErrorLog(_persistence.Persistable):
    """Errors reported by an entity that suspects that a server has failed
    are processed and stored in a table (i.e. error_log) for future
    analysis. Each entry has the following fields:

    * erver_uuid - Server's UUID.
    * reported - When the error has been reported.
    * reporter - Who has reported the error.
    * error - Error description.

    See also:

    * :class:`~mysql.fabric.server.Group`.
    * :class:`~mysql.fabric.services.threat.ReportError`.
    * :class:`~mysql.fabric.services.threat.ReportFailure`.
    """

    _MIN_PRUNE_TIME = 60
    _PRUNE_TIME = _DEFAULT_PRUNE_TIME = 3600

    CREATE_SERVER_ERROR_LOG = (
        "CREATE TABLE error_log "
        "(server_uuid VARCHAR(40) NOT NULL, "
        "reported TIMESTAMP /*!50604 (6) */ NOT NULL, "
        "reporter VARCHAR(64) NOT NULL, "
        "error TEXT, "
        "INDEX key_server_uuid_reported (server_uuid, reported), "
        "INDEX key_reporter (reporter)) DEFAULT CHARSET=utf8"
    )

    #SQL Statement for creating event used to prune error logs that are
    #older than PRUNE_TIME.
    CREATE_EVENT_ERROR_LOG = (
        "CREATE EVENT prune_error_log "
        "ON SCHEDULE EVERY %s SECOND "
        "DO DELETE FROM error_log WHERE "
        "TIMEDIFF(UTC_TIMESTAMP(), reported) > MAKETIME(%s,0,0)"
    )

    ADD_FOREIGN_KEY_CONSTRAINT_SERVER_UUID = (
        "ALTER TABLE error_log ADD CONSTRAINT "
        "fk_server_uuid_error_log FOREIGN KEY(server_uuid) "
        "REFERENCES servers(server_uuid)"
    )

    QUERY_SERVER_ERROR_LOG = (
        "SELECT reported, reporter FROM error_log WHERE "
        "server_uuid = %s AND reported >= %s ORDER BY reported ASC"
    )

    REMOVE_SERVER_ERROR_LOG = (
        "DELETE FROM error_log WHERE server_uuid = %s"
    )

    INSERT_SERVER_ERROR_LOG = (
        "INSERT INTO error_log (server_uuid, reported, reporter, "
        "error) VALUES(%s, %s, %s, %s)"
    )

    @staticmethod
    def create(persister=None):
        """Create the objects(tables) that will store server's errors.

        :param persister: Persister to persist the object to.
        :raises: DatabaseError If the table already exists.
        """
        persister.exec_stmt(ErrorLog.CREATE_SERVER_ERROR_LOG)
        persister.exec_stmt(
            ErrorLog.CREATE_EVENT_ERROR_LOG % (ErrorLog._PRUNE_TIME,
            ErrorLog._PRUNE_TIME, )
        )

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints to the error_log table.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(
                ErrorLog.ADD_FOREIGN_KEY_CONSTRAINT_SERVER_UUID)

    @staticmethod
    def add(server, reported, reporter, error, persister=None):
        """Add a server's error log.

        :param server: Server where the error has happened.
        :param reported: When the error has happened.
        :param reporter: Entity that has reported the error.
        :param error: Error reported.
        """
        from mysql.fabric.server import MySQLServer
        assert(isinstance(server, MySQLServer))
        persister.exec_stmt(ErrorLog.INSERT_SERVER_ERROR_LOG,
            {"params": (str(server.uuid), reported, reporter, error)})

    @staticmethod
    def remove(server, persister=None):
        """Remove server's error logs from the state store.

        :param server: A reference to a server.
        :param persister: Persister to persist the object to.
        """
        from mysql.fabric.server import MySQLServer
        assert(isinstance(server, MySQLServer))
        persister.exec_stmt(
            ErrorLog.REMOVE_SERVER_ERROR_LOG,
            {"params": (str(server.uuid), )}
            )

    @staticmethod
    def fetch(server, interval, now=None, persister=None):
        """Return a ErrorLog object corresponding to the
        server.

        :param server: Server whose error has been reported.
        :param interval: Interval of interest.
        :param now: Consider from `now` until `now` - `interval`.
        :param persister: Persister to persist the object to.
        :return: ErrorLog object.
        """
        from mysql.fabric.server import MySQLServer
        assert(isinstance(server, MySQLServer))

        now = now or get_time()
        (whens, reporters) = ErrorLog.compute(
            server.uuid, interval, now
        )
        return ErrorLog(server, interval, now, whens, reporters)

    @staticmethod
    def compute(uuid, interval, now, persister=None):
        """Compute information to build a ErrorLog object.

        :param server: Server's UUID whose error has been reported.
        :param interval: Interval of interest.
        :param now: Consider from `now` until `now` - `interval`.
        :param persister: Persister to persist the object to.
        :return: ErrorLog object.
        """

        cur = persister.exec_stmt(
            ErrorLog.QUERY_SERVER_ERROR_LOG,
            {"fetch" : False,
             "params": (str(uuid), now - interval)
            }
        )

        whens = []
        reporters = []
        for row in cur.fetchall():
            when, reporter = row
            whens.append(when)
            reporters.append(reporter)

        return (whens, reporters)

    def __init__(self, server, interval, now, whens, reporters):
        """Constructor for ErrorLog object.
        """
        from mysql.fabric.server import MySQLServer
        assert(isinstance(server, MySQLServer))
        assert(len(whens) == len(reporters))

        super(ErrorLog, self).__init__()

        self.__server_uuid = server.uuid
        self.__now = now
        self.__interval = interval
        self.__whens = whens or []
        self.__reporters = reporters or []

    @property
    def server_uuid(self):
        """Return the associated server's UUID.
        """
        return self.__server_uuid

    @property
    def whens(self):
        """Return when errors were reported.
        interval.
        """
        return self.__whens

    @property
    def reporters(self):
        """Return who reported errors.
        """
        return self.__reporters

    @property
    def now(self):
        """Base time to analyze errors.
        """
        return self.__now

    @property
    def interval(self):
        """Interval upon which errors are analyzed.
        """
        return self.__interval

    def refresh(self):
        """Refresh set of errors.
        """
        (whens, reporters) = ErrorLog.compute(
            self.__server_uuid, self.__interval, self.__now
        )
        self.__whens = whens
        self.__reporters = reporters

    def is_unstable(self, n_notifications, n_reporters, filter_reporter=None):
        """Check whether a server is unstable or not.

        :param n_notifications: Number of notifications threshold.
        :param n_reporters: Number of reporters threshold.
        :param filter_reporter: Filter errors by reporters before checking
                                whether a server is unstable or not.
        :return: Whether the server can be considered faulty
                 or not. The outcome depends on the parameters.
        """
        assert(
            filter_reporter is None or isinstance(filter_reporter, tuple) or \
            isinstance(filter_reporter, list)
        )

        reporters = self.__reporters
        if filter_reporter is not None:
            reporters = [
                reporter for reporter in self.__reporters \
                if reporter in filter_reporter
            ]

        nreporters = len(set(reporters))
        notifications = len(reporters)
        if nreporters >= n_reporters and notifications >= n_notifications:
            return True

        return False

def configure(config):
    """Set configuration values.
    """
    try:
        prune_time = int(config.get("failure_tracking", "prune_time"))
        if prune_time < ErrorLog._MIN_PRUNE_TIME:
            _LOGGER.warning(
                "Prune entries cannot be lower than %s.",
                ErrorLog._MIN_PRUNE_TIME
            )
            prune_time = ErrorLog._MIN_PRUNE_TIME
        ErrorLog._PRUNE_TIME = int(prune_time)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass
