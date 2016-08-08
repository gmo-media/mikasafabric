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

"""Define interfaces to manage servers, specifically MySQL Servers.

A server is uniquely identified through a *UUID* (Universally Unique
Identifier) and has an *Address* (i.e., Hostname:Port) which is
used to connect to it through the Python Database API. If a server
process such as MySQL already provides a uuid, the server's class
used to create a MySQL object must ensure that they match otherwise
the different uuids may cause problems in other modules.

Any sort of provisioning must not be performed when the server object
is instantiated. The provisioning steps must be done in other modules.

Servers are organized into groups which have unique names. This aims
at defining administrative domains and easing management activities.
"""
import threading
import time
import uuid as _uuid
import logging
import math
import functools
import re

from datetime import (
    datetime,
)

from mysql.fabric import (
    errors as _errors,
    persistence as _persistence,
    utils as _utils,
    error_log as _error_log,
    config as _config,
)

from mysql.fabric.handler import (
    MySQLHandler,
)

from mysql.fabric.server_utils import (
    create_mysql_connection,
    connect_to_mysql,
    exec_mysql_stmt,
    disconnect_mysql_connection,
    destroy_mysql_connection,
    split_host_port,
    is_valid_mysql_connection
)

_LOGGER = logging.getLogger(__name__)

def server_logging(function):
    """This logs information on functions being called within server
    instances.
    """
    @functools.wraps(function)
    def wrapper_check(*args, **kwrds):
        """Inner function that logs information on wrapped function.
        """
        _LOGGER.debug(
            "Start executing function: %s(%s, %s).", function.__name__,
            args, kwrds
        )
        try:
            ret = function(*args, **kwrds)
        except Exception:
            _LOGGER.debug("Error executing function: %s.", function.__name__)
            raise
        else:
            _LOGGER.debug("Finish executing function: %s.", function.__name__)
        return ret
    return wrapper_check


class Group(_persistence.Persistable):
    """Provide interfaces to organize servers into groups.

    :param group_id: The id that uniquely identifies the group.
    :param description: The group's description.
    :param master: The master's uuid in the group.
    :rtype master: UUID
    :param status: Group's status.
    :rtype status: ACTIVE or INACTIVE.
    """
    CREATE_GROUP = ("CREATE TABLE groups"
                    "(group_id VARCHAR(64) NOT NULL, "
                    "description VARCHAR(256), "
                    "master_uuid VARCHAR(40), "
                    "master_defined TIMESTAMP /*!50604 (6) */ NULL, "
                    "status BIT(1) NOT NULL, "
                    "CONSTRAINT pk_group_id PRIMARY KEY (group_id)) "
                    "DEFAULT CHARSET=utf8"
    )

    #Create the table that stores the group replication relationship.
    CREATE_GROUP_REPLICATION = (
                            "CREATE TABLE group_replication"
                            "(master_group_id VARCHAR(64) NOT NULL, "
                            "slave_group_id VARCHAR(64) NOT NULL, "
                            "CONSTRAINT pk_master_slave_group_id "
                            "PRIMARY KEY(master_group_id, slave_group_id), "
                            "CONSTRAINT FOREIGN KEY(master_group_id) "
                            "REFERENCES groups(group_id), "
                            "CONSTRAINT FOREIGN KEY(slave_group_id) "
                            "REFERENCES groups(group_id), "
                            "INDEX idx_slave_group_id(slave_group_id)) "
                            "DEFAULT CHARSET=utf8"
    )

    #SQL statement for inserting a new group into the table
    INSERT_GROUP = ("INSERT INTO groups(group_id, description, status) "
                    "VALUES(%s, %s, %s)")

    #SQL statement for selecting all groups
    QUERY_GROUPS = ("SELECT group_id FROM groups")

    #SQL statement for selecting all groups
    QUERY_GROUPS_BY_STATUS = ("SELECT group_id FROM groups WHERE status = %s")

    #Query the group that is the master of this group.
    QUERY_GROUP_REPLICATION_MASTER = ("SELECT master_group_id FROM "
                                "group_replication WHERE slave_group_id = %s")

    #Query the groups that are the slaves of this group.
    QUERY_GROUP_REPLICATION_SLAVES = ("SELECT slave_group_id FROM "
                                "group_replication WHERE master_group_id = %s")

    #SQL statement for updating the group table identified by the group id.
    UPDATE_GROUP = ("UPDATE groups SET description = %s WHERE group_id = %s")

    #SQL statement used for deleting the group identified by the group id.
    REMOVE_GROUP = ("DELETE FROM groups WHERE group_id = %s")

    #Remove the mapping between a master and a slave group.
    DELETE_MASTER_SLAVE_GROUP_MAPPING = ("DELETE FROM group_replication "
                            "WHERE slave_group_id = %s")

    #Delete all the master to slave mappings for a given master group. A
    #given master group can have multiple slaves.
    DELETE_SLAVE_GROUPS = ("DELETE FROM group_replication "
                            "WHERE master_group_id = %s")

    #Add a Master - Slave Group mapping.
    INSERT_MASTER_SLAVE_GROUP_MAPPING = \
            ("INSERT INTO group_replication"
             "(master_group_id, slave_group_id)"
             " VALUES(%s, %s)")

    #SQL Statement to retrieve a specific group from the state_store.
    QUERY_GROUP = ("SELECT group_id, description, master_uuid, "
                   "master_defined, status FROM groups WHERE group_id = %s")

    #SQL Statement to update the group's master.
    UPDATE_MASTER = ("UPDATE groups SET master_uuid = %s, master_defined = %s "
                     "WHERE group_id = %s")

    #SQL Statement to update the group's status.
    UPDATE_STATUS = ("UPDATE groups SET status = %s WHERE group_id = %s")

    #Create the referential integrity constraint with the servers table
    ADD_FOREIGN_KEY_CONSTRAINT_MASTER_UUID = (
        "ALTER TABLE groups ADD CONSTRAINT fk_master_uid_servers "
        "FOREIGN KEY(master_uuid) REFERENCES servers(server_uuid)"
    )

    #Group's statuses
    INACTIVE, ACTIVE = range(0, 2)

    #List with Group's statuses
    GROUP_STATUS = [INACTIVE, ACTIVE]

    #Failover interval
    _FAILOVER_INTERVAL = _DEFAULT_FAILOVER_INTERVAL = 3600

    def __init__(self, group_id, description=None, master=None,
                 master_defined=None, status=INACTIVE):
        """Constructor for the Group.
        """
        assert(isinstance(group_id, basestring))
        assert(description is None or isinstance(description, basestring))
        assert(master is None or isinstance(master, _uuid.UUID))
        assert(master_defined is None or isinstance(master_defined, datetime))
        assert(status in Group.GROUP_STATUS)
        super(Group, self).__init__()
        self.__group_id = group_id
        self.__description = description
        self.__master = master
        self.__master_defined = master_defined
        self.__status = status

    def __eq__(self,  other):
        """Two groups are equal if they have the same id.
        """
        return isinstance(other, Group) and \
               self.__group_id == other.group_id

    def __hash__(self):
        """A group is hashable through its uuid.
        """
        return hash(self.__group_id)

    @property
    def group_id(self):
        """Return the group's id.
        """
        return self.__group_id

    @property
    def slave_group_ids(self):
        """Property that gives access to the list of Groups that are
        slaves to this group.
        """
        return self.fetch_slave_group_ids()

    def fetch_slave_group_ids(self, persister=None):
        """Return the list of Groups that are slaves to this group.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        ret = []
        rows = persister.exec_stmt(Group.QUERY_GROUP_REPLICATION_SLAVES,
            {"params" : (self.__group_id,)}
        )
        if not rows:
            return ret

        for row in rows:
            ret.append(row[0])
        return ret

    @property
    def master_group_id(self):
        """Property that returns the ID of the master group from which this
        group replicates.
        """
        return self.fetch_master_group_id()

    def fetch_master_group_id(self, persister=None):
        """Return the ID of the master group from which this group replicates.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        row = persister.exec_stmt(Group.QUERY_GROUP_REPLICATION_MASTER,
            {"params" : (self.__group_id,)})
        if not row:
            return None
        return row[0][0]


    def add_slave_group_id(self,  slave_group_id, persister=None):
        """Insert a slave group ID into the slave group ID list. Register a
        slave to this group.

        :param slave_group_id: the group ID of the slave group that needs to
                                              be added.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Group.INSERT_MASTER_SLAVE_GROUP_MAPPING,
                            {"params": (self.__group_id, slave_group_id)})

    def remove_slave_group_id(self,  slave_group_id, persister=None):
        """Remove a slave group ID from the slave group ID list. Unregister a
        slave group.

        :param slave_group_id: the group ID of the slave group that needs to
                                              be removed.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Group.DELETE_MASTER_SLAVE_GROUP_MAPPING,
                            {"params": (slave_group_id, )})

    def remove_slave_group_ids(self, persister=None):
        """Remove slave group ids for a particular group. Unregisters
        all the slave of this group.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Group.DELETE_SLAVE_GROUPS,
                            {"params": (self.__group_id, )})

    def add_master_group_id(self,  master_group_id, persister=None):
        """Set the master group ID. Register a group as a master. This Group
        basically is a slave to the registered group.

        :param master_group_id: The group ID of the master that needs to be
                                                 added.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Group.INSERT_MASTER_SLAVE_GROUP_MAPPING,
                            {"params": (master_group_id, self.__group_id)})

    def remove_master_group_id(self, persister=None):
        """Remove the master group ID. Unregister a master group.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Group.DELETE_MASTER_SLAVE_GROUP_MAPPING,
                            {"params": (self.__group_id, )})

    def add_server(self, server):
        """Add a server into this group.

        :param server: The Server object that needs to be added to this
                       Group.
        """
        assert(isinstance(server, MySQLServer))
        assert(server.group_id == None)
        server.group_id = self.__group_id

    def remove_server(self, server):
        """Remove a server from this group.

        :param server: The Server object that needs to be removed from this
                       Group.
        """
        assert(isinstance(server, MySQLServer))
        assert(server.group_id == self.__group_id)
        server.group_id = None

    @property
    def description(self):
        """Return the description for the group.
        """
        return self.__description

    @description.setter
    def description(self, description=None, persister=None):
        """Set the description for this group. Update the description for the
        Group in the state store.

        :param persister: The DB server that can be used to access the
                          state store.
        :param description: The new description for the group that needs to be
                            updated.
        """
        persister.exec_stmt(Group.UPDATE_GROUP,
            {"params":(description, self.__group_id)})
        self.__description = description

    @property
    def master(self):
        """Return the master for the group.
        """
        return self.__master

    @property
    def master_defined(self):
        """Return the last time the master has changed.
        """
        return self.__master_defined

    @master.setter
    def master(self, master, persister=None):
        """Set the master for this group.

        :param persister: The DB server that can be used to access the
                          state store.
        :param master: The master for the group that needs to be updated.
        """
        assert(master is None or isinstance(master, _uuid.UUID))
        if master is None:
            param_master = None
        else:
            param_master = str(master)

        _LOGGER.info("Master has changed from %s to %s.", self.__master, master,
            extra={
                "subject": self.__group_id,
                "category": MySQLHandler.GROUP,
                "type" : MySQLHandler.PROMOTE if master else \
                         MySQLHandler.DEMOTE
            }
        )
        persister.exec_stmt(Group.UPDATE_MASTER,
            {"params":(param_master, _utils.get_time(), self.__group_id)})
        self.__master = master


    def servers(self):
        """Return a list with the servers in this group.
        """
        return MySQLServer.servers(self.__group_id)

    @property
    def status(self):
        """Return the group's status.
        """
        return self.__status

    def kill_connections_on_servers(self):
        """Kill all the threads on the groups servers.
        """
        server = None
        server_kill_threads = []
        while server in self.servers():
            try:
                server.connect()
            except (_errors.UuidError, _errors.DatabaseError):
                continue
            if server.is_connected():
                server_kill_threads.append(
                    threading.Thread(
                        target=server.kill_processes(server.processes())
                    )
                )
        for thread in server_kill_threads:
            thread.join()

    @status.setter
    def status(self, status, persister=None):
        """Set the group's status.

        :param status: The new group's status.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        assert(status in Group.GROUP_STATUS)
        # Check the maximum number of threads.
        _utils.check_number_threads(1)
        persister.exec_stmt(Group.UPDATE_STATUS,
            {"params":(status, self.__group_id)})
        self.__status = status

    def can_set_server_faulty(self, server, now):
        """Check whether is ipossible to set a new master.

        If `now - master_defined` > Group._FAILOVER_INTERVAL, it is safe
        to set a new master without making the system unstable.
        """
        if self.__master_defined is None:
            return True

        diff = now - self.__master_defined
        interval = _utils.get_time_delta(Group._FAILOVER_INTERVAL)

        if (self.__master == server.uuid and diff >= interval) or \
            self.__master != server.uuid:
            return True

        return False

    @staticmethod
    def groups_by_status(status, persister=None):
        """Return the group_ids of all the available groups.

        :param persister: Persister to persist the object to.
        """
        assert(status in Group.GROUP_STATUS)
        return persister.exec_stmt(Group.QUERY_GROUPS_BY_STATUS,
            {"params":(status, )}
            )

    @staticmethod
    def groups(persister=None):
        """Return the group_ids of all the available groups.

        :param persister: Persister to persist the object to.
        """
        return [ gid[0] for gid in persister.exec_stmt(Group.QUERY_GROUPS) ]

    @staticmethod
    def remove(group, persister=None):
        """Remove the group object from the state store.

        :param group: A reference to a group.
        :param persister: Persister to persist the object to.
        """
        persister.exec_stmt(
            Group.REMOVE_GROUP, {"params" : (group.group_id, )}
            )

    @staticmethod
    def fetch(group_id, persister=None):
        """Return the group object, by loading the attributes for the group_id
        from the state store.

        :param group_id: The group_id for the Group object that needs to be
                         retrieved.
        :param persister: Persister to persist the object to.
        :return: The Group object corresponding to the group_id
                 None if the Group object does not exist.
        """
        group = None
        cur = persister.exec_stmt(
            Group.QUERY_GROUP, {"fetch" : False, "params" : (group_id, )}
        )
        row = cur.fetchone()
        if row:
            group_id, description, master, master_defined, status = row
            if master:
                master = _uuid.UUID(master)
            group = Group(
                group_id=group_id, description=description, master=master,
                master_defined=master_defined, status=status
                )
        return group

    @staticmethod
    def add(group, persister=None):
        """Write a group object into the state store.

        :param group: A reference to a group.
        :param persister: Persister to persist the object to.
        """
        persister.exec_stmt(Group.INSERT_GROUP,
            {"params": (group.group_id, group.description, group.status)}
            )

    @staticmethod
    def create(persister=None):
        """Create the objects(tables) that will store the Group information in
        the state store.

        :param persister: The DB server that can be used to access the
                          state store.
        :raises: DatabaseError If the table already exists.
        """
        persister.exec_stmt(Group.CREATE_GROUP)
        persister.exec_stmt(Group.CREATE_GROUP_REPLICATION)

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints to the groups table.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(
                Group.ADD_FOREIGN_KEY_CONSTRAINT_MASTER_UUID
        )


class ConnectionManager(_utils.Singleton):
    """Manages MySQL Servers' connections.

    The pool is internally implemented as a dictionary that maps a server's
    uuid to a sequence of connections.
    """
    def __init__(self):
        """Creates a ConnectionManager object.
        """
        super(ConnectionManager, self).__init__()
        self.__pool = {}
        self.__lock = threading.RLock()
        self.__tracker = {}

    def _do_create_connection(self, server):
        """Create a connection and return it.

        Connections are established in two phases. First a connection object
        is created and registered into a tracker and than the connection is
        finally established. With a reference to the connection object, an
        external function might kill connections to a server thus unblocking
        any call that might be hanged because of a faulty server.
        """
        cnx = None

        with self.__lock:
            cnx = create_mysql_connection()
            self._track_connection(server, cnx)

        host, port = split_host_port(server.address)
        connect_to_mysql(
            cnx, autocommit=True, host=host, port=port,
            user=server.user, passwd=server.passwd
        )
        return cnx

    def _do_get_connection(self, server):
        """Return a connection from the pool.
        """
        cnx = None
        # The pool of inactive connections holds only those connections,
        # that use the server_user credentials. Other connections are
        # established and disconnected directly, bypassing the pool.
        if server.user != server.server_user:
            return
        # Since we need a server_user connection, we can now immediately
        # pop one from the pool.
        with self.__lock:
            try:
                cnx = self.__pool[server.uuid].pop()
                self._track_connection(server, cnx)
            except (KeyError, IndexError):
                pass
        return cnx

    def _track_connection(self, server, cnx):
        """Register that a connection is about to be used.
        """
        tracker = self.__tracker.get(server.uuid, [])
        assert cnx not in tracker
        tracker.append(cnx)
        self.__tracker[server.uuid] = tracker
        _LOGGER.debug("Track %s %s", str(server.uuid), str(cnx))

    def _untrack_connection(self, server, cnx):
        """Unregister a connection after its use.
        """
        # In some cases it can happen, that a server object is copied.
        # And so it can happen, that the same connection is tried to
        # untrack twice. Be cautious, not to raise an exception in this
        # case.
        try:
            tracker = self.__tracker[server.uuid]
        except KeyError:
            _LOGGER.debug("Nothing tracked for %s %s",
                          str(server.uuid), str(cnx))
        else:
            # tracker is a list of cnx objects.
            try:
                tracker.remove(cnx)
            except ValueError:
                _LOGGER.debug("Not tracked %s %s",
                              str(server.uuid), str(cnx))
            finally:
                # If we removed the last connection for this server,
                # remove the whole mapping entry.
                if len(tracker) == 0:
                    del self.__tracker[server.uuid]
            _LOGGER.debug("Untracking %s %s", str(server.uuid), str(cnx))

    def get_connection(self, server):
        """Get a connection.

        The method gets a connection from a pool if there is any or
        create a fresh one.
        """
        cnx = self._do_get_connection(server)
        while cnx:
            assert server.user != None and cnx.user == server.user
            if is_valid_mysql_connection(cnx):
                return cnx
            cnx = self._do_get_connection(server)
        return self._do_create_connection(server)

    def release_connection(self, server, cnx):
        """Release a connection to the pool.

        It is up to the developer to check if the connection is still
        valid and belongs to this server before returning it to the
        pool.
        """
        assert cnx is not None
        # The pool of inactive connections holds only those connections,
        # that use the server_user credentials. Other connections are
        # established and disconnected directly, bypassing the pool.
        if server.user != server.server_user:
            self._untrack_connection(server, cnx)
            disconnect_mysql_connection(cnx)
            return
        # Since we have a server_user connection, we can put it in the pool.
        with self.__lock:
            try:
                self._untrack_connection(server, cnx)
                if server.uuid not in self.__pool:
                    self.__pool[server.uuid] = []
                self.__pool[server.uuid].append(cnx)
            except (KeyError, ValueError):
                pass

    def get_number_connections(self, server):
        """Return the number of connections available in the pool.
        """
        with self.__lock:
            try:
                return len(self.__pool[server.uuid])
            except KeyError:
                pass
        return 0

    def purge_connections(self, server):
        """Close and remove all connections that belongs to a MySQL Server
        which is associated to a server.
        """
        _LOGGER.debug("Purging connections for %s", str(server.uuid))
        with self.__lock:
            try:
                for cnx in self.__pool[server.uuid]:
                    _LOGGER.debug("Releasing connection (%s).", cnx)
                    destroy_mysql_connection(cnx)
                del self.__pool[server.uuid]
            except KeyError:
                pass
            try:
                for cnx in self.__tracker[server.uuid]:
                    _LOGGER.debug("Releasing connection (%s).", cnx)
                    destroy_mysql_connection(cnx)
                del self.__tracker[server.uuid]
            except KeyError:
                pass

    def kill_connections(self, server):
        """Close all connections that are in use and belong to a MySQL Server.
        """
        with self.__lock:
            try:
                for cnx in self.__tracker[server.uuid]:
                    destroy_mysql_connection(cnx)
            except KeyError:
                pass

class MySQLServer(_persistence.Persistable):
    """Proxy class that provides an interface to access a MySQL Server
    Instance.

    To create a MySQLServer object, one needs to provide at least two
    parameters: uuid and address (i.e., host:port). If the uuid is not known
    beforehand, one can find it out as follows::

      address = "localhost:13000"
      uuid = MySQLServer.discover_uuid(address)
      uuid = _uuid.UUID(uuid)

      server = MySQLServer(uuid=uuid, address=address)

    After creating the object, it is necessary to connect it to the MySQL
    Server by explicitly calling connect(). This is required because all the
    necessary information to connect to the server may not have been defined
    at initialization time. For example, one may have created the object
    before reading its state from a persistence layer and setting them.

    So after connecting the object to a server, users may execute statements
    by calling exec_stmt() as follows::

      server.connect()
      ret = server.exec_stmt("SELECT VERSION()")
      print "MySQL Server has version", ret[0][0]

    Changing the value of the properties user or password triggers a call to
    disconnect.

    :param uuid: The uuid of the server.
    :param address:  The address of the server.
    :param user: The user's name used to access the server.
    :param passwd: The password used to access the server.
    :param mode: Server's mode.
    :type mode: OFFLINE, READ_ONLY, READ_WRITE.
    :param status: Server's status.
    :type status: FAULTY, SPARE, SECONDARY, PRIMARY.
    :param weight: Server's weight which determines its likelihood of
                   receiving requests.
    :type weight: Float
    :param group_id: Group's id which the server belongs to.
    :param row: Row with information on the server.
    """
    #SQL Statement for creating the table used to store details about the
    #server.
    CREATE_SERVER = (
        "CREATE TABLE servers "
        "(server_uuid VARCHAR(40) NOT NULL, "
        "server_address VARCHAR(128) NOT NULL, "
        "mode INTEGER NOT NULL, "
        "status INTEGER NOT NULL, "
        "weight FLOAT NOT NULL, "
        "group_id VARCHAR(64), "
        "CONSTRAINT pk_server_uuid PRIMARY KEY (server_uuid), "
        "INDEX idx_group_id (group_id), "
        "UNIQUE INDEX idx_server_address (server_address)) "
        "DEFAULT CHARSET=utf8"
    )

    #Create the referential integrity constraint with the groups table
    ADD_FOREIGN_KEY_CONSTRAINT_GROUP_ID = (
        "ALTER TABLE servers ADD CONSTRAINT fk_group_id_servers "
        "FOREIGN KEY(group_id) REFERENCES groups(group_id)"
    )

    #SQL statement for inserting a new server into the table
    INSERT_SERVER = ("INSERT INTO servers(server_uuid, server_address, mode, "
                     "status, weight, group_id) values(%s, %s, %s, %s, %s, %s)")

    #SQL statement for updating the server table identified by the server id.
    UPDATE_SERVER_MODE = (
        "UPDATE servers SET mode = %s WHERE server_uuid = %s"
        )

    #SQL statement for updating the server table identified by the server id.
    UPDATE_SERVER_STATUS = (
        "UPDATE servers SET status = %s WHERE server_uuid = %s"
        )

    #SQL statement for updating the server table identified by the server id.
    UPDATE_SERVER_WEIGHT = (
        "UPDATE servers SET weight = %s WHERE server_uuid = %s"
        )

    #SQL statement for updating the server table identified by the server id.
    UPDATE_SERVER_GROUP_ID = (
        "UPDATE servers SET group_id = %s WHERE server_uuid = %s"
        )

    #SQL statement used for deleting the server identified by the server id.
    REMOVE_SERVER = ("DELETE FROM servers WHERE server_uuid = %s")

    #SQL Statement to retrieve the server from the state store.
    QUERY_SERVER_BY_UUID = (
        "SELECT server_uuid, server_address, mode, status, weight, group_id "
        "FROM servers WHERE server_uuid = %s"
        )

    #SQL Statement to retrieve a set of servers in a group from the state store.
    QUERY_SERVER_BY_GROUP_ID = (
        "SELECT server_uuid, server_address, mode, status, weight, group_id "
        "FROM servers WHERE group_id = %s"
        )

    #SQL Statement to retrieve a server from the state store.
    QUERY_SERVER_BY_ADDRESS = (
        "SELECT server_uuid, server_address, mode, status, weight, group_id "
        "FROM servers WHERE server_address = %s"
        )

    #SQL Statement to retrieve the servers belonging to a group.
    DUMP_SERVERS = (
        "SELECT server_uuid, group_id, server_address, mode, status, weight "
        "FROM servers WHERE group_id LIKE %s AND group_id IS NOT NULL AND "
        "status != %s ORDER BY group_id, server_address, server_uuid"
        )

    #Default weight for the server
    DEFAULT_WEIGHT = 1.0

    # Define a session context for a variable.
    SESSION_CONTEXT = "SESSION"

    # Define a global context for a variable.
    GLOBAL_CONTEXT = "GLOBAL"

    # Set of contexts.
    CONTEXTS = [SESSION_CONTEXT, GLOBAL_CONTEXT]

    # Define the offline mode.
    OFFLINE = "OFFLINE"

    # Define the read-only mode.
    READ_ONLY = "READ_ONLY"

    # Define the write-only mode.
    WRITE_ONLY = "WRITE_ONLY"

    # Define the read-write mode.
    READ_WRITE = "READ_WRITE"

    # Define default mode
    DEFAULT_MODE = READ_ONLY

    # Set of possible modes.
    SERVER_MODE = [OFFLINE, READ_ONLY, WRITE_ONLY, READ_WRITE]

    # Define the faulty status.
    FAULTY = "FAULTY"

    # Define the spare status.
    SPARE = "SPARE"

    # Define the secondary status.
    SECONDARY = "SECONDARY"

    # Define the primary status.
    PRIMARY = "PRIMARY"

    # Define default status
    DEFAULT_STATUS = SECONDARY

    # Set of possible statuses.
    SERVER_STATUS = [FAULTY, SPARE, SECONDARY, PRIMARY]

    USER = None

    PASSWD = None

    #
    # The server user could have all privileges.
    #
    ALL_PRIVILEGES = [        # GRANT ... ON *.*
        "ALL PRIVILEGES"
    ]

    #
    # Minimum set of global privileges.
    #
    SERVER_PRIVILEGES = [     # GRANT ... ON *.*
        "DELETE",             # prune_shard
        "PROCESS",            # list sessions to kill
        "RELOAD",             # RESET SLAVE
        "REPLICATION CLIENT", # SHOW SLAVE STATUS
        "REPLICATION SLAVE",  # SHOW SLAVE HOSTS
        "SELECT",             # prune_shard
        "SUPER",              # CHANGE MASTER TO
        "TRIGGER"             # CREATE TRIGGER
    ]

    #
    # Minimum set of database privileges for the mysql_fabric database,
    # which is used to provide shard boundaries.
    #
    SERVER_PRIVILEGES_DB = [  # GRANT ... ON mysql_fabric.*
        "ALTER",              # alter some database objects
        "CREATE",             # create most database objects
        "DELETE",             # delete rows
        "DROP",               # drop most database objects
        "INSERT",             # insert rows
        "SELECT",             # select rows
        "UPDATE",             # update rows
    ]

    NO_USER_DATABASES = ["performance_schema", "information_schema", "mysql"]

    def __init__(self, uuid=None, address=None, user=None, passwd=None,
                 mode=DEFAULT_MODE, status=DEFAULT_STATUS,
                 weight=DEFAULT_WEIGHT, group_id=None, row=None):
        """Constructor for MySQLServer.
        """
        super(MySQLServer, self).__init__()

        if row is not None:
            assert(uuid is None and address is None)
            uuid, address, idx_mode, idx_status, weight, group_id = row
            mode = MySQLServer.get_mode(idx_mode)
            status = MySQLServer.get_status(idx_status)
            uuid = _uuid.UUID(uuid)

        assert(isinstance(uuid, _uuid.UUID))
        assert(mode in MySQLServer.SERVER_MODE)
        assert(status in MySQLServer.SERVER_STATUS)

        self.__cnx = None
        self.__uuid = uuid
        self.__address = "{0}:{1}".format(*split_host_port(address))
        self.__group_id = group_id
        self.__cnx_manager = ConnectionManager()
        self.__user = user if user != None else MySQLServer.USER
        self.__passwd = passwd if passwd != None else MySQLServer.PASSWD
        self.__mode = mode
        self.__status = status
        self.__weight = weight
        self.__read_only = None
        self.__server_id = None
        self.__version = None
        self.__gtid_enabled = None
        self.__binlog_enabled = None

    @staticmethod
    @server_logging
    def discover_uuid(address, user=None, passwd=None,
                      connection_timeout=None):
        """Retrieve the uuid from a server.

        :param address: Server's address.
        :param user: The user's name used to access the server.
        :param passwd: The password used to access the server.
        :param connection_timeout: Time in seconds after which an error is
                                   reported if the UUID is not retrieved.
        :return: UUID.
        """

        host, port = split_host_port(address)
        port = int(port)

        user = user if user != None else MySQLServer.USER
        passwd = passwd if passwd != None else MySQLServer.PASSWD
        cnx = connect_to_mysql(
            user=user, passwd=passwd, host=host, port=port, autocommit=True,
            connection_timeout=connection_timeout
        )

        try:
            row = exec_mysql_stmt(cnx, "SELECT @@GLOBAL.SERVER_UUID")
            server_uuid = str(row[0][0])
        finally:
            destroy_mysql_connection(cnx)

        return server_uuid

    def connect(self):
        """Connect to a MySQL Server instance.
        """
        self.disconnect()

        # Set up an internal connection.
        _LOGGER.debug("Server (%s) Connecting (%s).", id(self), self.__user)
        self.__cnx = self.__cnx_manager.get_connection(self)
        _LOGGER.debug("Server (%s) Using connection (%s).",
                      id(self), self.__cnx)

        # Get server's uuid
        ret_uuid = self.get_variable("SERVER_UUID")
        ret_uuid = _uuid.UUID(ret_uuid)
        if ret_uuid != self.uuid:
            self.disconnect()
            raise _errors.UuidError(
                "UUIDs do not match (stored (%s), read (%s))." %
                (self.uuid, ret_uuid)
            )

        # Get server's id.
        self.__server_id = int(self.get_variable("SERVER_ID"))

        # Get server's version.
        self.__version = self.get_variable("VERSION")

        # Get information on gtid support.
        if not self.check_version_compat((5, 6, 5)):
            self.__gtid_enabled = False
        else:
            ret_gtid = self.get_variable("GTID_MODE")
            self.__gtid_enabled = ret_gtid in ("ON", "1")

        ret_binlog = self.get_variable("LOG_BIN")
        self.__binlog_enabled = not ret_binlog in ("OFF", "0")

        self._check_read_only()

        _LOGGER.debug("Connected to server with uuid (%s), server_id (%d), "
                      "version (%s), gtid (%s), binlog (%s), read_only (%s).",
                      self.uuid, self.__server_id, self.__version,
                      self.__gtid_enabled, self.__binlog_enabled,
                      self.__read_only)

    def disconnect(self):
        """Disconnect from the server.
        """
        if self.__cnx is not None:
            _LOGGER.debug("Disconnecting from server with uuid (%s), "
                          "server_id (%s), version (%s), gtid (%s), "
                          "binlog (%s), read_only (%s).", self.uuid,
                          self.__server_id, self.__version,
                          self.__gtid_enabled, self.__binlog_enabled,
                          self.__read_only)
            _LOGGER.debug("Server (%s) Releasing connection (%s).",
                          id(self), self.__cnx)
            self.__cnx_manager.release_connection(self, self.__cnx)
            self.__cnx = None
            self.__read_only = None
            self.__server_id = None
            self.__version = None
            self.__gtid_enabled = None
            self.__binlog_enabled = None

    def has_privileges(self, required_privileges, level=None):
        """Check whether the current user has the required privileges.

        :param required_privileges: List or tuple of required privileges which
                                    a user who is connected to the current
                                    server must have.
        :param level: Level of the set of privileges.
        """
        assert(isinstance(required_privileges, list) or \
               isinstance(required_privileges, tuple))

        required_privileges = set(required_privileges)
        required_level = level or "*.*"
        all_privileges = "ALL PRIVILEGES"
        all_level = "*.*"

        # Log user name with host name, as it is seen by the server.
        ret = self.exec_stmt("SELECT CURRENT_USER()")
        current_user = ret[0][0]
        _LOGGER.debug("Check privileges (%s ON %s) for current user (%s)",
                      ", ".join(required_privileges), required_level,
                       current_user)

        ret = self.exec_stmt("SHOW GRANTS")
        check = re.compile("GRANT (?P<privileges>.*?) ON (?P<level>.*?) TO")
        for row in ret:
            _LOGGER.debug("Row: %s", row[0])
            res = check.match(row[0])
            if res:
                privileges = [ privilege.strip() \
                      for privilege in res.group("privileges").split(",")
                ]
                level = res.group("level").replace('`', "")
                if (all_privileges in privileges and all_level == level) or \
                    (required_privileges.issubset(set(privileges)) and \
                    required_level == level):
                    _LOGGER.debug("match")
                    return True
        _LOGGER.debug("no match")
        return False

    def check_privileges(self, privileges, level=None):
        """Check whether the current user has a minimum set of privileges.
        :param privileges: String of comma separated MySQL server privileges.
        :return: None.
        :raises: ServerError on missing privileges.
        """
        if not self.is_connected():
            self.connect()

        if not self.has_privileges(privileges, level):
            # Include the user with host, as the server sees it, into the report
            ret = self.exec_stmt("SELECT CURRENT_USER()")
            current_user = ret[0][0]
            required_level = level or "*.*"
            raise _errors.ServerError("User (%s) does not have appropriate"
                                      " privileges (%s ON %s)"
                                      " on server (%s, %s)." %
                                      (current_user, ", ".join(privileges),
                                       required_level,
                                       self.address, self.uuid,))

    def check_server_privileges(self):
        """Check whether the current user has the privileges to manage
        a server in the farm.
        The required privileges are some global privileges (on *.*),
        and some database privileges on mysql_fabric.*.
        :return: None.
        :raises: ServerError on missing privileges.
        """
        self.check_privileges(MySQLServer.SERVER_PRIVILEGES, "*.*")
        self.check_privileges(MySQLServer.SERVER_PRIVILEGES_DB,
                              "mysql_fabric.*")

    def is_connected(self):
        """Determine whether the proxy object (i.e. the server) is connected
        to actual server.
        """
        return self.__cnx is not None

    @staticmethod
    def is_alive(server, connection_timeout=None):
        """Determine whether the server is dead or alive by trying to create
        a new connection.

        :param connection_timeout: Timeout waiting for getting a new
                                   connection.
        """
        res = False
        try:
            host, port = split_host_port(server.address)
            cnx = connect_to_mysql(
                autocommit=True, host=host, port=port,
                user=server.user, passwd=server.passwd,
                connection_timeout=connection_timeout
            )
            res = True
            destroy_mysql_connection(cnx)
        except _errors.DatabaseError:
            pass

        return res

    def _check_read_only(self):
        """Check if the database was set to read-only mode.
        """
        ret_read_only = self.get_variable("READ_ONLY")
        self.__read_only = not ret_read_only in ("OFF", "0")

    @property
    def uuid(self):
        """Return the server's uuid.
        """
        return self.__uuid

    @property
    def address(self):
        """Return the server's address.
        """
        return self.__address

    @property
    def read_only(self):
        """Check read only mode on/off.

        :return True If read_only is set
                False If read_only is not set.
        """
        return self.__read_only

    @read_only.setter
    def read_only(self, enabled):
        """Turn read only mode on/off. Persist the information in the state
        store.

        :param enabled The read_only flag value.
        """
        self.set_variable("READ_ONLY", "ON" if enabled else "OFF")
        self._check_read_only()

    @property
    def server_id(self):
        """Return the server id.
        """
        return self.__server_id

    @property
    def version(self):
        """Return version number of the server.
        """
        return self.__version

    @property
    def gtid_enabled(self):
        """Return if GTID is enabled.
        """
        return self.__gtid_enabled

    @property
    def binlog_enabled(self):
        """Check binary logging status.
        """
        return self.__binlog_enabled

    @property
    def user(self):
        """Return user's name who is used to connect to a server.
        """
        # Return the default user, if __user == None,
        # but not if it is the empty string
        return self.__user if self.__user != None else MySQLServer.USER

    @user.setter
    def user(self, user):
        """Set user's name who is used to connect to a server.

        :param user: User's name.
        """
        if self.__user != user:
            self.disconnect()
            self.__user = user

    @property
    def passwd(self):
        """Return user's password who is used to connect to a server. Load
        the server information from the state store and return the password.
        """
        # Return the default password, if __passwd == None,
        # but not if it is the empty string
        return self.__passwd if self.__passwd != None else MySQLServer.PASSWD

    @passwd.setter
    def passwd(self, passwd):
        """Set user's passord who is used to connect to a server.

        :param passwd: User's password.
        """
        if self.__passwd != passwd:
            self.disconnect()
            self.__passwd = passwd

    @property
    def server_user(self):
        """Return the name of the configured server user.
        """
        return MySQLServer.USER

    @property
    def mode(self):
        """Return the server's mode.
        """
        return self.__mode

    @mode.setter
    def mode(self, mode, persister=None):
        """Set the server's mode.

        :param mode: The new server's mode.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        assert(mode in MySQLServer.SERVER_MODE)
        idx = MySQLServer.get_mode_idx(mode)
        persister.exec_stmt(MySQLServer.UPDATE_SERVER_MODE,
                            {"params":(idx, str(self.uuid))})
        self.__mode = mode

    @staticmethod
    def get_mode_idx(mode):
        """Return the index associated to a mode.
        """
        return MySQLServer.SERVER_MODE.index(mode)

    @staticmethod
    def get_mode(idx):
        """Return the mode associated to an index.
        """
        return MySQLServer.SERVER_MODE[idx]

    @property
    def status(self):
        """Return the server's status.
        """
        return self.__status

    @status.setter
    def status(self, status, persister=None):
        """Set the server's status.

        :param status: The new server's status.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        assert(status in MySQLServer.SERVER_STATUS)
        idx = MySQLServer.get_status_idx(status)
        persister.exec_stmt(MySQLServer.UPDATE_SERVER_STATUS,
                            {"params":(idx, str(self.uuid))})
        self.__status = status

    @staticmethod
    def get_status_idx(status):
        """Return the index associated to a status.
        """
        return MySQLServer.SERVER_STATUS.index(status)

    @staticmethod
    def get_status(idx):
        """Return the status associated to an index.
        """
        return MySQLServer.SERVER_STATUS[idx]

    @property
    def weight(self):
        """Return the server's weight.
        """
        return self.__weight

    @weight.setter
    def weight(self, weight, persister=None):
        """Set the server's weight.

        :param weight: The new server's weight.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        assert(weight > 0.0)
        persister.exec_stmt(MySQLServer.UPDATE_SERVER_WEIGHT,
                            {"params":(weight, str(self.uuid))})
        self.__weight = weight

    @property
    def group_id(self):
        """Return the server's group_id.
        """
        return self.__group_id

    @group_id.setter
    def group_id(self, group_id, persister=None):
        """Set the server's group_id.

        :param group_id: The new server's group_id.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(MySQLServer.UPDATE_SERVER_GROUP_ID,
                            {"params":(group_id, str(self.uuid))})
        self.__group_id = group_id

    @staticmethod
    def servers(group_id, persister=None):
        """Return a list of servers identified by group_id.

        :param group_id: Group's id.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        ret = []
        rows = persister.exec_stmt(MySQLServer.QUERY_SERVER_BY_GROUP_ID,
            {"params" : (group_id, )}
        )
        for row in rows:
            server = MySQLServer(row=row)
            ret.append(server)
        return ret

    def check_version_compat(self, expected_version):
        """Check version of the server against requested version.

        This method can be used to check for version compatibility.

        :param expected_version: Target server version.
        :type expected_version: (major, minor, release)
        :return: True if server version is GE (>=) version specified,
                 False if server version is LT (<) version specified.
        :rtype: Bool
        """
        assert(isinstance(expected_version, tuple))
        index = self.__version.find("-")
        version_str = self.__version[0 : index] \
            if self.__version.find("-") >= 0 else self.__version
        version = tuple(int(part) for part in version_str.split("."))
        return version >= expected_version

    def get_gtid_status(self):
        """Get the GTID information for the server.

        This method attempts to retrieve the GTID lists. If the server
        does not have GTID turned on or does not support GTID, the method
        will throw the exception DatabaseError.

        :return: A named tuple with GTID information.

        In order to access the result set one may do what follows::

          ret = server.get_gtid_status()
          for record in ret:
            print "GTID_EXECUTED", record.GTID_EXECUTED, record[0]
            print "GTID_PURGED", record.GTID_PURGED, record[1]
            print "GTID_OWNED", record_GTID_OWNED, record[2]
        """
        # Check servers for GTID support
        if self.__cnx and not self.__gtid_enabled:
            raise _errors.ProgrammingError("Global Transaction IDs are not "
                                           "supported.")

        query_str = (
            "SELECT @@GLOBAL.GTID_EXECUTED as GTID_EXECUTED, "
            "@@GLOBAL.GTID_PURGED as GTID_PURGED, "
            "@@GLOBAL.GTID_OWNED as GTID_OWNED"
        )
        return self.exec_stmt(query_str, {"columns" : True})

    def has_storage_engine(self, target):
        """Check to see if an engine exists and is supported.

        :param target: Name of engine to find.
        :return: True if engine exists and is active. False if it does
                 not exist or is not supported/not active/disabled.
        """
        if len(target) == 0:
            return True # This says we will use default engine on the server.

        query_str = (
            "SELECT UPPER(engine) as engine, UPPER(support) as support "
            "FROM INFORMATION_SCHEMA.ENGINES"
        )

        if target:
            engines = self.exec_stmt(query_str)
            for engine in engines:
                if engine[0].upper() == target.upper() and \
                   engine[1].upper() in ['YES', 'DEFAULT']:
                    return True
        return False

    def get_binary_logs(self):
        """Return information on binary logs. Look up `SHOW BINARY LOGS` in
        the MySQL Manual for further details.

        :return: A named tuple with information on binary logs.
        """
        return self.exec_stmt("SHOW BINARY LOGS", {"columns" : True})

    def set_session_binlog(self, enabled=True):
        """Enable or disable binary logging for the client.

        :param disable: If 'disable', turn off the binary log
                        otherwise turn binary log on.

        .. note::

           User must have SUPER privilege to execute this.
        """
        self.set_variable("SQL_LOG_BIN", "ON" if enabled else "OFF",
                          MySQLServer.SESSION_CONTEXT)

    def session_binlog_enabled(self):
        """Check if binary logging is enabled for the client.
        """
        ret = self.get_variable("SQL_LOG_BIN",
                                MySQLServer.SESSION_CONTEXT)
        return ret in ["ON", '1']

    def foreign_key_checks_enabled(self):
        """Check foreign key status for the client.
        """
        ret = self.get_variable("FOREIGN_KEY_CHECKS",
                                MySQLServer.SESSION_CONTEXT)
        return ret in ["ON", '1']

    def set_foreign_key_checks(self, enabled=True):
        """Enable or disable foreign key checks for the client.

        :param disable: If True, turn off foreign key checks otherwise turn
                        foreign key checks on.
        """
        self.set_variable("FOREIGN_KEY_CHECKS", "ON" if enabled else "OFF",
                          MySQLServer.SESSION_CONTEXT)

    def get_variable(self, variable, context=None):
        """Execute the SELECT command for the client and return a result set.
        """
        if not context:
            context = MySQLServer.GLOBAL_CONTEXT
        assert(context in MySQLServer.CONTEXTS)
        ret = self.exec_stmt("SELECT @@%s.%s as %s" %
                             (context, variable, variable))
        return str(ret[0][0])

    def set_variable(self, variable, value, context=None):
        """Execute the SET command for the client and return a result set.
        """
        if not context:
            context = MySQLServer.GLOBAL_CONTEXT
        assert(context in MySQLServer.CONTEXTS)
        return self.exec_stmt("SET @@%s.%s = %s" %
                              (context, variable, value))

    def processes(self, system_user=False, current_connection=False):
        """Return a list of processes running on the MySQL Server.
        Skip the system processes that might be running on the server.
        Skip also the ID of the current connection used to retrieve the list
        of IDs.

        :param system_user: Set to True if the system users needs to be
                            included in the result.
        :param current_connection: Set to True if the current connection needs
                                    to be included in the result.
        :return: The list of process IDs of processes running on the server.
        """
        SELECT_IDs = "SELECT ID FROM INFORMATION_SCHEMA.PROCESSLIST"
        if not system_user or not current_connection:
            SELECT_IDs += " WHERE"
        if not system_user:
            SELECT_IDs += " User != 'system user' AND User != 'event_scheduler'"
        if not current_connection:
            if not system_user:
                SELECT_IDs += " AND"
            SELECT_IDs += " ID != CONNECTION_ID()"

        #User != 'system user' skips over any replication threads that might be
        #running in the system.
        proc_ids = []
        proc_ids_rows = self.exec_stmt(SELECT_IDs)
        for row in proc_ids_rows:
            proc_ids.append(row[0])
        return proc_ids

    def kill_processes(self,  proc_ids):
        """Kill the list of processes supplied as an argument.

        :param proc_ids: A list containing the process IDs that need to be
                         killed.
        """
        #The below is repeatedly tried since KILL is unreliable and we
        #need to loop until all the processes are eventually terminated.
        #An intersection with the set of processes currently running in
        #the system, gives the processes on which the KILL has still not
        #worked. Hence we need to keep calling KILL repeatedly on these.
        #for e.g.
        #P = {1, 2, 3, 4, 5, 6}
        #p = {1, 2, 3}
        #p &= P => {1,2,3}
        #After the first KILL
        #P = {3, 4, 5, 6}
        #p = {1, 2, 3}
        #p &= P => {3}
        #Now we run KILL on {3}
        #Thus each time we work with the set of proecesses in the original
        #list that are still running.
        proc_ids_set = set(proc_ids)
        proc_ids_set.intersection_update(set(self.processes(True, True)))
        while proc_ids_set:
            for proc_id in proc_ids_set:
                self.exec_stmt("KILL %s", {"params": (proc_id, )})
            #sleep to ensure that the kill command reflects its results
            time.sleep(math.log10(len(proc_ids_set)))
            proc_ids_set.intersection_update(
                set(self.processes(True, True))
            )

    def exec_stmt(self, stmt_str, options=None):
        """Execute statements against the server.
        See :meth:`~mysql.fabric.server_utils.exec_stmt`.
        """
        return exec_mysql_stmt(self.__cnx, stmt_str, options)

    def __del__(self):
        """Destructor for MySQLServer.
        """
        self.disconnect()

    @staticmethod
    def remove(server, persister=None):
        """Remove a server from the state store.

        :param server: A reference to a server.
        :param persister: Persister to persist the object to.
        """
        ConnectionManager().purge_connections(server)
        _error_log.ErrorLog.remove(server)
        persister.exec_stmt(
            MySQLServer.REMOVE_SERVER, {"params": (str(server.uuid), )}
        )

    @staticmethod
    def fetch(server_id, persister=None):
        """Return a server object corresponding to the uuid.

        :param server_id: Id or address of the server object that needs to be
                          returned.
        :param persister: Persister to persist the object to.
        :return: The server object that corresponds to the server id
                 None if the server id does not exist.
        """
        if server_id is None:
            return

        query = None
        try:
            if isinstance(server_id, _uuid.UUID):
                server_id = str(server_id)
            else:
                _uuid.UUID(server_id)
            query = MySQLServer.QUERY_SERVER_BY_UUID
        except (ValueError, TypeError, AttributeError):
            query = MySQLServer.QUERY_SERVER_BY_ADDRESS

        cur = persister.exec_stmt(query, {
            "fetch" : False,
            "params" : (server_id, )
        })

        row = cur.fetchone()
        if row:
            return MySQLServer(row=row)

    @staticmethod
    def dump_servers(version=None, patterns="", persister=None):
        """Return the list of servers that are part of the groups
        listed in the patterns strings separated by comma.

        :param version: The connectors version of the data.
        :param patterns: group pattern.
        :param persister: Persister to persist the object to.
        :return: A list of servers belonging to the groups that match
                the provided pattern.
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
            rows = persister.exec_stmt(MySQLServer.DUMP_SERVERS,
                {"params":(like_pattern, "FAULTY")}
            )

            for row in rows:
                host, port = split_host_port(row[2])
                yield (row[0], row[1], host, port, row[3], row[4], row[5])

    @staticmethod
    def create(persister=None):
        """Create the objects(tables) that will store the Server
        information in the state store.

        :param persister: Persister to persist the object to.
        :raises: DatabaseError If the table already exists.
        """
        persister.exec_stmt(MySQLServer.CREATE_SERVER)

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints to the servers table.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(
                MySQLServer.ADD_FOREIGN_KEY_CONSTRAINT_GROUP_ID)

    @staticmethod
    def add(server, persister=None):
        """Write a server object into the state store.

        :param server: A reference to a server.
        :param persister: Persister to persist the object to.
        """
        assert(isinstance(server, MySQLServer))

        persister_uuid = persister.uuid
        if persister_uuid is not None and persister_uuid == server.uuid:
            raise _errors.ServerError(
                "The MySQL Server instance used as Fabric's state store "
                "cannot be managed."
            )

        idx_mode = MySQLServer.get_mode_idx(server.mode)
        idx_status = MySQLServer.get_status_idx(server.status)
        persister.exec_stmt(MySQLServer.INSERT_SERVER,
            {"params":(str(server.uuid), server.address, idx_mode, idx_status,
            server.weight,
            server.group_id)}
        )

    def __eq__(self,  other):
        """Two servers are equal if they are both subclasses of MySQLServer
        and have equal UUID.
        """
        return isinstance(other, MySQLServer) and self.__uuid == other.uuid

    def __hash__(self):
        """A server is hashable through its uuid.
        """
        return hash(self.__uuid)

    def __str__(self):
        ret = "<server(uuid={0}, address={1}, mode={2}, status={3}>".\
            format(self.__uuid, self.__address, self.__mode, self.__status)
        return ret

def configure(config):
    """Set configuration values.
    """
    MySQLServer.USER = config.get("servers", "user")

    try:
        MySQLServer.PASSWD = config.get("servers", "password")
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass

    try:
        failover_interval = config.get("failure_tracking", "failover_interval")
        Group._FAILOVER_INTERVAL = int(failover_interval)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass
