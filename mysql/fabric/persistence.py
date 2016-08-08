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

"""Module containing classes and functions for working with the
persistent store.

The two main classes here are the :class:`MySQLPersister` and the
:class:`PersistentMeta`.

Initializing the persistence system
-----------------------------------

Before creating any persisters, it is necessary to set up the
persistance system using connection information where objects should
be persisted::

   import mysql.fabric.persistence as persistence

   persistence.init(host="localhost", port=3307,
                    user="fabric_user", password="xyzzy")


Thread Initialization
~~~~~~~~~~~~~~~~~~~~~

When starting a new thread, a persister have to be created.  Typical
code for initializing the persister system for a thread is::

   import mysql.fabric.persistence as persistence
   persistence.init_thread()

"""
import functools
import inspect
import logging
import threading
import time
import uuid as _uuid

import mysql.fabric.errors as _errors

from mysql.fabric.server_utils import (
    MYSQL_DEFAULT_PORT,
    connect_to_mysql,
    exec_mysql_stmt,
    destroy_mysql_connection,
    is_valid_mysql_connection,
    reestablish_mysql_connection
)

DEFAULT_DATABASE = 'fabric'
DEFAULT_CONNECT_ATTEMPTS = 0
DEFAULT_CONNECT_DELAY = 0

class PersistentMeta(type):
    """Metaclass for persistent classes.

    This is a metaclass for persistent classes. The metaclass is
    responsible for:

    - Keep track of the persister for the thread. There is exactly one
      persister for each thread and a thread is expected call the
      :meth:`init_thread` method with the persister when it starts.

    - Add the 'persister' attribute to each method, giving it the
      persister class assigned to the thread by the object store.

    - Registering the class with the object store so that it will get
      an init() and deinit() call when the state store inits and
      deinits.
    """
    thread_local = threading.local() # Thread-local store
    classes = []                     # List of all persistent classes

    @classmethod
    def init_thread(mcs, persister):
        """Initialize thread-specific data.

        :param persister: Persister to use for the thread.
        """

        mcs.thread_local.persister = persister

    @classmethod
    def deinit_thread(mcs):
        """De-initialize thread-specific data.
        """

        mcs.thread_local.persister = None

    @classmethod
    def wrapfunc(mcs, func):
        """Wrap the function to pass the persister for the thread.

        The function is wrapped by adding the argument 'persister' if
        the function request it by having an explicit argument named
        'persister'.

        The wrapped function will set the 'persister' argument to the
        persister assigned to the thread, but if an explicit persister
        argument has been given in the call, it is passed.

        Note that the function is not wrapped if it has a keywords
        argument (** argument) even though it is possible to pass the
        persister to such a function.  The reason for this is that
        many function wrappers use keywords argument to pass the
        arguments to the underlying function.

        """

        # Check that the function can accept a 'persister' parameter
        # or accepts keywords.
        argspec = inspect.getargspec(func)
        if 'persister' not in argspec.args:
            return func         # No need to wrap it, so just return it

        original = func         # Create closure
        @functools.wraps(func)
        def _wrap(*args, **kwrds):
            """Inner wrapper function.
            """
            # Check if an explicit persister were given to the call or
            # use the thread-assigned persister otherwise.
            if 'persister' not in kwrds or kwrds['persister'] is None:
                kwrds['persister'] = mcs.thread_local.persister
            return original(*args, **kwrds)
        return _wrap


    def __new__(mcs, cname, cbases, cdict):
        """Create a new class from it's pieces.

        The method will construct a new class by wrapping each method
        of the class using a wrapper that adds the "persistor"
        attribute to the keyword arguments.

        Only callable attributes, class methods, static methods, and
        properties not starting with underscore will be wrapped. All
        other functions are left alone.

        """

        mcs.thread_local.persister = None

        for name, func in cdict.items():
            # Check for functions that should not be touched.

            if name.startswith('_'):
                continue        # Special or internal function

            # Wrap function differently depending on what it is.
            if callable(func):
                # Anything callable is wrapped.
                cdict[name] = mcs.wrapfunc(func)
            elif isinstance(func, (staticmethod, classmethod)):
                # Wrap the inner function of static and class methods
                # and re-wrap using the type constructor.
                cdict[name] = type(func)(mcs.wrapfunc(func.__get__(True)))
            elif isinstance(func, property) and func.fset is not None:
                # Properties are re-constructed from its parts and
                # wrap the fset, fget, and fdel functions.
                newfset = func.fset and mcs.wrapfunc(func.fset)
                newfget = func.fget and mcs.wrapfunc(func.fget)
                newfdel = func.fdel and mcs.wrapfunc(func.fdel)
                cdict[name] = property(newfget, newfset, newfdel, func.__doc__)
        return type.__new__(mcs, cname, cbases, cdict)

    def __init__(cls, cname, cbases, cdict):
        """Add the class to the list of classes that should be called
        on init.
        """
        type.__init__(cls, cname, cbases, cdict)
        PersistentMeta.classes.append(cls)

class Persistable(object):
    """Class for all persistable objects.

    Classes whose instances are going to be persisted to the backing store
    should inherit from this class.

    To be usable by the Fabric the persistence class have to define the
    following methods:

    **create**\ (*persister=None*)
      This is a static (or class) method called when the persistence tables need
      to be created because the system is set up initially. The method is
      expected to use the passed persister to create any tables that are
      necessary to persist the instances of this class.

    **drop**\ (*persister=None*)
      This is a static (or class) method called when the persistence tables need
      to be removed because the system is torn down. The method is expected to
      use the passed persister to drop any tables that where created in the
      `create` method.

    **add_constraint**\ (*persister=None*)
      This is a static (or class) method called after all the persistence tables
      are created in the initial set up. The method is expected to use the
      passed persister to add any constraints needed on the tables (for example,
      foreign keys) created in the **create** method for the class.

    **drop_constraint**\ (*persister=None*)
      This is a static (or class) method called before the persistence tables
      are dropped in the tear down. The method is expected to use the passed
      persister to remove any constraints added to the tables so that the table
      can be trivially dropped later.

    The following methods are customary used to manipulate instances of objects
    and can be defined (but do not have to):

    **add**\ (*object*, *persister=None*)
      This is a static (or class) method to add an instance to the collection of
      instances in the persistent store. This method is expected to use the
      passed persister to insert a new object into the tables for the
      persistable object.

    **remove**\ (*object*, *persister=None*)
      This is a static (or class) method to remove an instance from the
      collection of instances in the persistent store. This method is expected
      to use the passed persister to delete the entry for the object from the
      tables in the persistent store.

    **fetch**\ (..., *persister=None*)
      This is a static (or class) method to fetch an already created instance
      from the collection of instances in the persistent store. This method is
      expected to take any arguments necessary to identify the object and fetch
      the object from the persistent store and return it as a new instance of
      the class.

    .. note::

       All the methods accept a *persister* parameter that should be placed last
       and have a default value of `None`.

    A example for how to create a persistable object is::

        from mysql.fabric.persistence import Persistable

        class Car(Persistable):
            def __init__(self, reg_no, model):
                self.reg_no = reg_no
                self.model = model

            @staticmethod
            def create(persister=None):
                persister.exec_stmt(
                    "CREATE TABLE cars ("
                    "  reg_no CHAR(6) PRIMARY KEY,"
                    "  model VARCHAR(16)"
                    ")"
                )

            @staticmethod
            def drop(persister=None):
                persister.exec_stmt("DROP TABLE cars")

            @staticmethod
            def add(car, persister=None):
                persister.exec_stmt(
                    "INSERT INTO cars(reg_no, model) VALUES (%s)",
                    { "params": (car.reg_no, car.model) }
                )

            @staticmethod
            def remove(car, persister=None):
                persister.exec_stmt(
                    "DELETE FROM cars WHERE reg_no = %s",
                    { "params": (car.reg_no,) }
                )

            @staticmethod
            def fetch(reg_no, persister=None):
                row = persister.exec_stmt(
                    "SELECT reg_no, model FROM cars WHERE reg_no = %s",
                    { "params": (reg_no,) }
                ).fetch_one()

                return Car(reg_no=row[0], model=row[1]) if row else None

    The `add`, `remove`, and `fetch` methods can be used to manipuate instances
    of the peristent class, but in this case the *persister* parameter should
    not be used.  The :class:`PersistentMeta` class will add a wrapper to each
    function that adds the persister argument to the class if it was not
    passed.

    The following is an example of code for manipulating persistent objects::

       my_car = Car(reg_no='XYZ123', model='Volvo')
       Car.add(my_car)
       ...
       some_car = Car.fetch(reg_no='XYZ123')
       print some_car.model
       Car.remove(some_car)

    If the connection to the persistent store is temporarily lost because, for
    example, it has been closed by the server after a period of inactivite, the
    persister will try to automatically restablish it before a new transaction
    is implicitly or explicitly started. Failures while executing a statement
    within a transaction context are automatically reported to the caller.

    The autocommit mode is enabled by default so that a new statement is always
    executed within a new transaction context. If users want to explicitly
    create a transaction context, they can do so as follows::

       import mysql.fabric.persistence

       persister = mysql.fabric.persistence.current_persister()

       persister.begin()
       ...
       persister.commit()
    """

    __metaclass__ = PersistentMeta

class MySQLPersister(object):
    """Class responsible for persisting objects to a MySQL database.

    The class is responsible for managing the connection with the database where
    the persistent objects are stored. There should normally be one instance of
    this class available for each thread.

    Before using the persister, the system has to be initialized using
    :meth:`MySQLPersister.init`. This will provide connection information to the
    object database for each :class:`MySQLPersister` instance created, set up
    the object database, and give each subclass of :class:`Persistable` a chance
    to set itself up by calling the class `init` method.
    """

    STORE_PRIVILEGES = [      # GRANT ... ON <fabric_db>.*
        "ALTER",              # alter some database objects
        "CREATE",             # create most database objects
        "CREATE VIEW",        # create view
        "DELETE",             # delete rows
        "DROP",               # drop most database objects
        "EVENT",              # manage events
        "INDEX",              # create index
        "INSERT",             # insert rows
        "REFERENCES",         # foreign keys
        "SELECT",             # select rows
        "UPDATE",             # update rows
    ]

    @classmethod
    def init(cls, host, user, password=None, port=None, database=None,
             connection_timeout=None, connection_attempts=None,
             connection_delay=None, auth_plugin=None):
        """Initialize the object persistance system.

        This function initializes the persistance system. The function
        is idempotent in the sense that it can be called multiple
        times to set up the system. The parameters provided are passed
        to the connector, so typically, these fields need to be
        present.

        :param host: Host name of the database server.
        :param port: Port of the database server. Defaults to 3306.
        :param user: User to use connecting to the database server.
        :param password: Password to use when connecting to the
                         database server.
        :param database: Database where the persistance information is
                         stored. Default is :const:`DEFAULT_DATABASE`.
        :param connection_timeout: Timeout to connect to the database server.
        :param connection_attempts: Number of attempts to connect or reconnect
                                    to the database server. Default is
                                    :const:`DEFAULT_CONNECT_ATTEMPTS`.
        :param connection_delay: Delay after an atempt to connect or reconnect
                                 to the database server. Default is
                                 :const:`DEFAULT_CONNECT_DELAY`.
        :param auth_plugin: Use auth_plugin as authencation plugin for
                            authentication with the database server.
        """
        if port is None:
            port = MYSQL_DEFAULT_PORT

        if database is None:
            database = DEFAULT_DATABASE

        if connection_attempts is None:
            connection_attempts = DEFAULT_CONNECT_ATTEMPTS

        if connection_delay is None:
            connection_delay = DEFAULT_CONNECT_DELAY

        # Save away the connection information, it will be used by the
        # threads.
        cls.connection_info = {
            "host": host, "port": port,
            "user": user, "password": password,
            "connection_timeout" : connection_timeout,
            "auth_plugin": auth_plugin,
            }
        cls.connection_attempts = connection_attempts
        cls.connection_delay = connection_delay
        cls.database = database

    @classmethod
    def setup(cls):
        """Setup the object persistance system.

        Perform initialization, which in this case means creating
        the database if it does not exist.
        """
        assert (cls.connection_info is not None)
        conn = connect_to_mysql(
            autocommit=True, **cls.connection_info
        )
        exec_mysql_stmt(
            conn, "CREATE DATABASE %s DEFAULT CHARSET=utf8" % (cls.database, )
        )

    @classmethod
    def teardown(cls):
        """Tear down the object persistance system.

        This should only be called if the persistance database should
        be removed from the persistance server since it will delete
        all object tables.
        """
        assert (cls.connection_info is not None)
        conn = connect_to_mysql(
            autocommit=True, **cls.connection_info
        )
        exec_mysql_stmt(
            conn, "DROP DATABASE IF EXISTS %s" % (cls.database, )
        )

    def __init__(self):
        """Constructor for MySQLPersister.
        """
        self.__cnx = None
        self.__check_connection = True

        assert (self.connection_info is not None)
        try:
            self.__cnx = connect_to_mysql(
                autocommit=True, database=self.database,
                **self.connection_info
            )
        except _errors.DatabaseError:
            pass

        if self.uuid is None and self.__cnx is not None:
            _LOGGER.warning(
                "Backing store does not support UUID (or not configured "
                "with UUID)."
            )

    def __del__(self):
        """Destructor for MySQLPersister.
        """
        try:
            if self.__cnx:
                destroy_mysql_connection(self.__cnx)
        except AttributeError:
            pass

    def begin(self):
        """Start a new transaction.
        """
        self.exec_stmt("BEGIN")
        self.__check_connection = False

    def commit(self):
        """Commit an on-going transaction.
        """
        try:
            self.exec_stmt("COMMIT")
        finally:
            self.__check_connection = True

    def rollback(self):
        """Roll back an on-going transaction.
        """
        try:
            self.exec_stmt("ROLLBACK")
        finally:
            self.__check_connection = True

    def auth_mysql_token(self):
        """Returns the authentication plugin data found in handshake"""
        return self.__cnx._handshake['scramble']

    @property
    def uuid(self):
        """Return the MySQLPersister's UUID if the server supports it.
        Otherwise, return None.
        """
        try:
            row = exec_mysql_stmt(
                self.__cnx, "SELECT @@GLOBAL.SERVER_UUID"
            )
            return _uuid.UUID(str(row[0][0]))
        except _errors.DatabaseError:
            pass

        return None

    def max_allowed_connections(self):
        """Return the maximum number of allowed connections to server.
        """
        row = exec_mysql_stmt(
            self.__cnx, "SELECT @@GLOBAL.max_connections"
        )
        return int(row[0][0])

    def exec_stmt(self, stmt_str, options=None):
        """Execute statements against the server.

        If a new transaction is about to be started, this method checks whether
        the connection is valid or not. If the connection is invalid, it tries
        to restablish it as MySQL might disconnect inactive connections.

        See :meth:`~mysql.fabric.server_utils.exec_stmt`.
        """
        while True:
            if self.__check_connection and \
                not is_valid_mysql_connection(self.__cnx):
                self._try_to_fix_connection()
            return exec_mysql_stmt(
                self.__cnx, stmt_str, options
            )

    def _try_to_fix_connection(self):
        """Try to get a new connection if the current one is stale.
        """
        for attempt in range(0, self.connection_attempts):
            try:
                if self.__cnx:
                    reestablish_mysql_connection(
                        self.__cnx, attempt=1, delay=0
                    )
                else:
                    self.__cnx = connect_to_mysql(
                        autocommit=True, database=self.database,
                        **self.connection_info
                    )
                return
            except _errors.DatabaseError as error:
                _LOGGER.debug(
                    "Error accessing backing store (%s). Attempt (%s).",
                    error, attempt
                )
            time.sleep(self.connection_delay)

def current_persister():
    """Return the persister for the current thread.
    """
    return PersistentMeta.thread_local.persister

def init_thread():
    """Initialize the persistence system for the thread.
    """
    PersistentMeta.init_thread(MySQLPersister())

def deinit_thread():
    """Initialize the persistence system for the thread.
    """
    PersistentMeta.deinit_thread()

_LOGGER = logging.getLogger(__name__)

def init(host, user, password=None, port=None, database=None,
         connection_timeout=None, connection_attempts=None,
         connection_delay=None, auth_plugin=None):
    """Initialize the persistance system.

    This function is idempotent in the sense that it can be executed
    multiple times without destroying anything. This property is
    important since normally the :func:`setup` and :func:`teardown`
    are not idempotent.

    :param host: Hostname to connect to.
    :param user: User to connect as.
    :param password: Password to use when connecting. Default to the
                     empty password.
    :param port: Port to connect to. Default to 3306.
    :param database: Database to store object data in. Default to
                     :const:`DEFAULT_DATABASE`.
    :param connection_timeout: Timeout to connect to the database server.
    :param connection_attempts: Number of attempts to connect or reconnect to
                                the database server. Default is
                                :const:`DEFAULT_CONNECT_ATTEMPTS`.
    :param connection_delay: Delay after an atempt to connect or reconnect
                             to the database server. Default is
                             :const:`DEFAULT_CONNECT_DELAY`.
    """
    _LOGGER.info(
        "Initializing persister: user (%s), server (%s:%d), database (%s).",
        user, host, port, database
    )

    MySQLPersister.init(
        host=host, port=port, user=user, password=password, database=database,
        connection_timeout=connection_timeout,
        connection_attempts=connection_attempts,
        connection_delay=connection_delay,
        auth_plugin=auth_plugin
    )

def setup(config=None):
    """ Setup the persistance system globally.

    This means creating any databases, tables and constraints necessary in the
    persistence database.
    """
    MySQLPersister.setup()

    persister = MySQLPersister()
    try:
        for cls in PersistentMeta.classes:
            if hasattr(cls, 'create'):
                _LOGGER.debug("Create database objects for %s", cls.__name__)
                if 'config' in inspect.getargspec(cls.create):
                    cls.create(persister=persister, config=config)
                else:
                    # create() does not support config
                    cls.create(persister=persister)

        #Initialize the constraints after creating the tables.
        for cls in PersistentMeta.classes:
            #Call the add_constraints method of those classes that sub-class from
            #Persistence and those which have an implementation of add_constraints.
            if hasattr(cls, 'add_constraints'):
                _LOGGER.debug("Create constraints for %s", cls.__name__)
                cls.add_constraints(persister=persister)
    except _errors.DatabaseError:
        MySQLPersister.teardown()
        raise

def teardown():
    """Teardown the persistance system globally.

    This means removing any tables,constraints created. Normally, this
    function does not have to be executed on shutdown since that would
    emove all necessary tables.
    """
    _LOGGER.info("Teardown persister.")
    MySQLPersister.teardown()

def required_privileges():
    """Return the list of MySQL server privileges needed for the
    backing store.
    """
    return MySQLPersister.STORE_PRIVILEGES
