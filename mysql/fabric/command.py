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

"""Module for supporting definition of Fabric commands.

This module aids in the definition of new commands to be incorporated
into the Fabric. Commands are defined as subclasses of the
:class:`Command` class and are automatically incorporated into the
client or server.

Commands have a *remote* and a *local* part, where the remote part is
executed by sending a request to the Fabric server for execution. The
local part of the command is executed by the client.

The default implementation of the local part just dispatch the command
to the server, so these commands do not need to define a local part at
all.

The documentation string of the command class is used as help text for
the command and can be shown using the "help" command. The first
sentence of the description is the brief description and is shown in
listings, while the remaining text is the more elaborate description
shown in command help message.
"""
import re
import inspect
import logging
import functools
import collections

import mysql.fabric.errors as _errors
import mysql.fabric.executor as _executor
import mysql.fabric.utils as _utils

from cStringIO import StringIO

from mysql.fabric.utils import (
    FABRIC_UUID,
)

from mysql.fabric.handler import (
    MySQLHandler,
)

_LOGGER = logging.getLogger(__name__)

_COMMANDS_CLASS = {}

def register_command(group_name, command_name, command):
    """Register a command within a group.

    :param group_name: The command-group to which a command belongs.
    :param command_name: The command that needs to be registered.
    :param command: The command class that contains the implementation
                    for this command
    """
    commands = _COMMANDS_CLASS.setdefault(group_name, {})
    commands[command_name] = command

def unregister_command(group_name, command_name):
    """Unregister a command within a group.

    :param group_name: The command-group to which a command belongs.
    :param command_name: The command that needs to be registered.
    """
    del _COMMANDS_CLASS[group_name][command_name]
    if not _COMMANDS_CLASS[group_name]:
        del _COMMANDS_CLASS[group_name]

def get_groups():
    """Return registered groups of commands.

    :return: Returns the different command groups.
    """
    return _COMMANDS_CLASS.keys()

def get_commands(group_name):
    """Return registered commands within a group.

    :param group_name: The command group whose commands need to be listed.
    :return: The command classes that handles the command functionality.
    """
    return _COMMANDS_CLASS[group_name].keys()

def get_command(group_name, command_name):
    """Return a registered command within a group.

    :param group_name: The command group whose commands need to be listed.
    :param command_name: The command whose implementation needs to be fetched.
    :return: The command classes that handles the command functionality.
    """
    return _COMMANDS_CLASS[group_name][command_name]

class CommandMeta(type):
    """Metaclass for defining new commands.

    This class will register new commands defined and add them to  the a list
    of existing commands.

    Users willing to create a new command should create a class that inherits
    from either the :class:`ProcedureCommand`, :class:`ProcedureGroup` or
    :class:`ProcedureShard` classes. However any base class defined upon one of
    the aforementioned classes must have its name appended to the
    `IgnoredCommand` attribute. Otherwise, it will erroneously be considered a
    command.
    """
    IgnoredCommand = \
        ("command", "procedurecommand", "proceduregroup", "procedureshard")

    def __init__(cls, cname, cbases, cdict):
        """Register command definitions.
        """
        type.__init__(cls, cname, cbases, cdict)

        try:
            if not cls.group_name:
                raise AttributeError
        except AttributeError:
            cls.group_name = cdict["__module__"]

        try:
            if not cls.command_name:
                raise AttributeError
        except AttributeError:
            cls.command_name = cname.lower()

        if cls.command_name not in CommandMeta.IgnoredCommand and \
            re.match(r"[A-Za-z]\w+", cls.command_name):
            register_command(cls.group_name, cls.command_name, cls)

    @classmethod
    def _wrapfunc(mcs, func, cname):
        """Wrap the a function in order to log when it started and
        finished its execution.
        """
        original = func
        @functools.wraps(func)
        def _wrap(obj, *args, **kwrds):
            """Inner wrapper function.
            """
            group = obj.group_name
            command = obj.command_name
            subject = ".".join([group, command])
            try:
                _LOGGER.debug(
                    "Started command (%s, %s).", group, command,
                    extra={
                        "subject" : subject,
                        "category" : MySQLHandler.PROCEDURE,
                        "type" : MySQLHandler.START
                    }
                )
                ret = original(obj, *args, **kwrds)

                # Check that we really got a result set back. If not,
                # something is amiss.
                #
                # As a special case, if the function returns None it
                # means it finished without throwing an exception, so
                # it trivially succeeded without a result set.
                if ret is None:
                    ret = CommandResult(None)
                elif not isinstance(ret, CommandResult):
                    raise  _errors.InternalError(
                        "Expected '%s', got '%s'" % (
                            CommandResult.__name__,
                            ret,
                        )
                    )
            except Exception as error:
                ret = CommandResult(error=str(error))
            finally:
                _LOGGER.debug("Finished command (%s, %s).", group, command,
                    extra={
                        "subject" : subject,
                        "category" : MySQLHandler.PROCEDURE,
                        "type" : MySQLHandler.ABORT if ret.error else \
                                 MySQLHandler.STOP
                    }
                )
            return ret
        _wrap.original_function = func
        return _wrap

    def __new__(mcs, cname, cbases, cdict):
        """Wrap the execute function in order to log when it starts
        and finishes its execution.
        """
        for name, func in cdict.items():
            if name == "execute" and callable(func):
                cdict[name] = mcs._wrapfunc(func, cname)
        return type.__new__(mcs, cname, cbases, cdict)


class Command(object):
    """Base class for all commands.

    Each subclass implement both the server side and the client side
    of a command.

    When defining a command, implementing the execute method will
    allow execution on the server. If there is anything that needs to
    be done locally, before dispatching the command, it should be
    added to the dispatch method.

    Command instances automatically get a few attributes defined when
    being created. These can be accessed as normal attributes inside
    the command.

    On the client side, the following attributes are defined:

    options
       Any options provided to the command.
    config
       Any information provided through a configuration file.
    client
       A protocol client instance, which can be used to communicate
       with the server. This is normally not necessary, but can be
       used to get access to configuration file information.

    On the server side, the following attributes are defined:

    server
      The protocol server instance the command is set up for. The
      configuration file information can be accessed through this.

    Commands are organized into groups through the *group_name* class
    property. If it is not defined though, the module where the command
    is defined is used as the group name. Something similar happens to
    the command name, which means that if the *command_name* class
    property is not defined, the class name is automatically used.
    """
    __metaclass__ = CommandMeta

    group_name = None

    command_name = None

    def __init__(self):
        """Constructor.

        See class description for information.
        """
        self.__client = None
        self.__server = None
        self.__options = None
        self.__config = None

        try:
            self.command_options
        except AttributeError:
            self.command_options = []

        self.generate_options()

    def execute(self, *args):
        raise NotImplementedError("Unimplemented execute method")

    def generate_options(self):
        """Use the execute / dispatch method signature to build the
        optional argument list passed to the command line parser.
        """
        # Extract the default values from the method signature and build
        # the optional argument list.
        cargs = get_arguments(self)

        if cargs.defaults is not None:
            action = ""
            # Easier to build the default args and values pairs in reverse
            for opt, value in zip(reversed(cargs.args), reversed(cargs.defaults)):
                # Set the action while parsing optional arguments by
                # inspecting the defaults.
                if type(value) is bool:
                    if value:
                        action = "store_false"
                    else:
                        action = "store_true"
                else:
                    action = "store"

                command_option = {
                    'options':["--" + opt],
                    'dest':opt,
                    'default':value,
                    'action':action
                }
                self.command_options.append(command_option)

            # Reverse the extracted list
            self.command_options.reverse()

        # Options for all commands
        command_option = {
            'options': ["--user"],
            'dest': 'auth_user',
            'default': None,
            'action': "store"
        }
        self.command_options.append(command_option)

    def append_options_to_args(self, args):
        """Append the optional arguments and their values to the
        argument list being passed to the remote server.

        @param args: The list of compulsory arguments to the command.
        """
        args_list = []
        if args:
            args_list.extend(args)
        if self.command_options:
            #Get the optional parameters from the options object. Append these
            #to the arguments list so that they can be passed to the execute
            #method.
            for option in self.command_options:
                if option['dest'] == 'auth_user':
                    continue
                args_list.append(getattr(self.options, option['dest']))
        return args_list

    @property
    def client(self):
        """Return the client proxy.
        """
        return self.__client

    @property
    def server(self):
        """Return the server proxy.
        """
        return self.__server

    @property
    def options(self):
        """Return command line options.
        """
        return self.__options

    @property
    def config(self):
        """Return configuration options.
        """
        return self.__config

    def setup_client(self, client, options, config):
        """Provide client-side information to the command.

        This is called after an instance of the command have been
        created on the client side and provide the client instance and
        options to the command.

        The client instance can be used to dispatch the command to the
        server.

        :param client: The client instance for the command.
        :param options: The options for the command.
        :param config: The configuration for the command.
        """
        assert self.__server is None
        self.__client = client
        self.__options = options
        self.__config = config

    def setup_server(self, server, options, config):
        """Provide server-side information to the command.

        This function is called after creating an instance of the
        command on the server-side and will set the server of the
        command. There will be one command instance for each protocol
        server available.

        :param server: Protocol server instance for the command.
        :param options: The options for the command.
        :param config: The configuration for the command.
        """
        assert self.__client is None
        self.__server = server
        self.__options = options
        self.__config = config

    def add_options(self, parser):
        """Method called to set up options from the class instance.

        :param parser: The parser used for parsing the command options.
        """
        try:
            for option in self.command_options:
                kwargs = option.copy()
                del kwargs['options']
                parser.add_option(*option['options'], **kwargs)
        except AttributeError:
            pass

    def dispatch(self, *args):
        """Default dispatch method, executed on the client side.

        The default dispatch method just call the server-side of the
        command.

        :param args: The arguments for the command dispatch.
        """
        return self.client.dispatch(self, *args)

    @classmethod
    def get_signature(cls):
        """Get the signature of the command.

        This is done by inspecting the arguments to the execute or
        dispatch method.

        :return string: The signature of the command as a string
        """
        # The signatures of the execute/dispatch methods are used to
        # build the help string to be used in the commands.
        cargs = get_arguments(cls)

        #Build the help text for the compulsory arguments of the command
        help_positional_arguments = ""
        if cargs.args is not None:
            if cargs.defaults is not None:
                default_len = len(cargs.defaults)
            else:
                default_len = 0
            #Skip the name of the functio and iterate till the beginning of the
            #default arguments.
            for arg in cargs.args[1:len(cargs.args)-default_len]:
                help_positional_arguments += (arg + " ")

        #Build the help text for the optional arguments for the command
        help_default_arguments = ""
        if cargs.defaults is not None:
            default_params = []
            #Iterate through the default arguments building a key value pair
            for opt, value in \
                zip(reversed(cargs.args), reversed(cargs.defaults)):
                if type(value) is not bool:
                    tmp = "[--" + str(opt) + "=" + str(value).upper() + "]"
                else:
                    tmp = "[--" + str(opt) + "]"
                default_params.append(tmp)
            default_params.reverse()
            for param in default_params:
                help_default_arguments += (param + " ")

        return "%s %s %s %s" % (
            cls.group_name,
            cls.command_name,
            help_positional_arguments,
            help_default_arguments
        )

    @staticmethod
    def generate_output_pattern(func, *params):
        """Call the function with the input params and generate a output pattern
        of {success:True/False, message:<for example exception>,
        return:<return values>}.

        :param func: the function that needs to be called
        :param params: The parameters to the function

        :return: :class:`CommandResult` instance

        """

        status = func(*params)
        _LOGGER.debug(
            "Status from execution of '%s': %s", func.__name__, status
        )
        if len(status) > 0:
            rset = ResultSet(
                names=status[0].keys(),
                types=[type(v) for v in status[0].values()],
            )
            for entry in status:
                rset.append_row(entry.values())
        else:
            rset = None
        return CommandResult(None, results=rset)


def get_arguments(reference):
    """This function returns a reference to an object that reprents arguments
    in either the execute or dispatch method and are used by caller to build
    optional arguments and help strings.

    If the sub-class implements an execute method, arguments are extract from
    there. Otherwise, they are extracted from the dispatch command.

    :param reference: Reference to an object that inherits from Command.
    """
    method = reference.execute.original_function
    this_method = getattr(reference, method.__name__).__func__
    base_method = getattr(Command, method.__name__).__func__
    if this_method == base_method:
        cargs = inspect.getargspec(reference.dispatch)
    else:
        cargs = inspect.getargspec(method)
    assert cargs is not None

    return cargs

class ProcedureCommand(Command):
    """Class used to implement commands that are built as procedures and
    schedule job(s) to be executed. Any command that needs to access the
    state store must be built upon this class.

    A procedure is asynchronously executed and schedules one or more jobs
    (i.e. functions) that are eventually processed. The scheduling is done
    through the executor which enqueues them and serializes their execution
    within a Fabric Server.

    Any job object encapsulates a function to be executed, its parameters,
    its execution's status and its result. Due to its asynchronous nature,
    a job accesses a snapshot produced by previously executed functions
    which are atomically processed so that Fabric is never left in an
    inconsistent state after a failure.

    To make it easy to use these commands, one might hide the asynchronous
    behavior by exploiting the :meth:`wait_for_procedures`.
    """
    def __init__(self):
        """Create the ProcedureCommand object.
        """
        super(ProcedureCommand, self).__init__()

    def dispatch(self, *args):
        """Default dispatch method when the command is build as a
        procedure.

        It calls command.dispatch, gets the result and processes
        it generating a user-friendly result.

        :param args: The arguments for the command dispatch.
        """

        return self.client.dispatch(self, *args)

    @staticmethod
    def wait_for_procedures(procedure_param, synchronous):
        """Wait until a procedure completes its execution and return
        detailed information on it.

        However, if the parameter synchronous is not set, only the
        procedure's uuid is returned because it is not safe to access
        the procedure's information while it may be executing.

        :param procedure_param: Iterable with procedures.
        :param synchronous: Whether should wait until the procedure
                            finishes its execution or not.
        :return: A :class:`CommandResult` instance with the execution result.
        :rtype: CommandResult
        """
        assert len(procedure_param) == 1
        synchronous = str(synchronous).upper() not in ("FALSE", "0")
        if not synchronous:
            info = ResultSet(names=['uuid'], types=[str])
            info.append_row([str(procedure_param[-1].uuid)])
            return CommandResult(None, results=info)

        executor = _executor.Executor()
        for procedure in procedure_param:
            executor.wait_for_procedure(procedure)
        _LOGGER.debug(
            "Result after wait: uuid='%s', status='%s', result='%s'",
            str(procedure_param[-1].uuid), procedure_param[-1].status,
            procedure_param[-1].result
        )

        # We look at the diagnosis of the last entry to decide the
        # status of the procedure execution.
        operation = procedure_param[-1].status[-1]
        complete = operation['state'] == _executor.Job.COMPLETE
        success = operation['success'] == _executor.Job.SUCCESS

        if success:
            result_field = procedure_param[-1].result
            info = ResultSet(
                names=('uuid', 'finished', 'success', 'result'),
                types=(str, bool, bool, type(result_field)),
            )
            info.append_row([
                str(procedure_param[-1].uuid),
                complete,
                success,
                result_field,
            ])
            _LOGGER.debug("Success: uuid='%s', result='%s'",
                          str(procedure_param[-1].uuid),
                          str(procedure_param[-1].result))

            rset = ResultSet(
                names=('state', 'success', 'when', 'description'),
                types=(int, int, float, str),
            )
            for item in procedure_param[-1].status:
                rset.append_row([
                    item['state'],
                    item['success'],
                    item['when'],
                    item['description'],
                ])
            return CommandResult(None, results=[info, rset])
        else:
            # The error message is the last line of the diagnosis, so
            # we get it from there.
            error = operation['diagnosis'].split("\n")[-2]
            _LOGGER.debug("Failure: error='%s'", error)
            return CommandResult(error)

    def get_lockable_objects(self, variable=None, function=None):
        """Return the set of lockable objects by extracting information
        on the parameter's value passed to the function.

        There are derived classes which return specific information according
        to the procedure that is being executed. This implementation returns
        a set with with the string "lock".

        :param variable: Paramater's name from which the value should be
                         extracted.
        :param function: Function where the parameter's value will be
                         searched for.
        """
        return set(["lock"])


class ProcedureGroup(ProcedureCommand):
    """Class used to implement commands that are built as procedures and
    execute operations within a group.
    """
    pass

class ProcedureShard(ProcedureCommand):
    """Class used to implement commands that are built as procedures and
    execute operations within a sharding.
    """
    pass

ResultSetColumn = collections.namedtuple('ResultSetColumn', 'name,type')

class ResultSet(object):
    """A result set returned by a command object.

    Each result set is conceptually a table with columns where each
    column have a name and a type. The name is given as a string and
    the type is given as a Python type.

    Rows are internally stored as tuples, even if other iterables are
    used to add a row to the result set.

    :param names: List (or other iterable) of names of columns.
    :param types: List (or other iterable) of types of columns.

    """

    def __init__(self, names, types):
        """Constructor.

        See class description for more information.

        """

        # Make sure that we always store the column information as a
        # tuple, even if other types of iterables are passed to the
        # function.
        assert len(names) == len(types)
        self.__columns = \
            tuple(ResultSetColumn(nm, tp) for nm, tp in zip(names, types))
        self.__rows = []

    def table_rows(self):
        r"""Create rows for the result set as a table.

        This will create the lines for a result set in a tabular
        format. A typical use-case would be:

        >>> rset = ResultSet(names=('one', 'two'), types=(int, int))
        >>> rset.append_row([1,2])
        >>> rset.append_row([3,4])
        >>> print "\n".join(rset.table_rows())
        one two
        --- ---
          1   2
          3   4

        """
        header = [col.name for col in self.__columns]
        all_rows = [header]
        all_rows.extend(self.__rows)
        width = [max(len(str(r)) for r in col) for col in zip(*all_rows)]
        def _mkline(row):
            """Create an output string per row.
            """
            return " ".join(
                "{0:>{1}}".format(x, width[i]) for i, x in enumerate(row)
            )

        result = [_mkline(header)]
        result.append(_mkline(['-' * w for w in width]))
        for row in self.__rows:
            result.append(_mkline(row))
        return result

    @property
    def rowcount(self):
        """The number of rows in the result set
        """
        return len(self.__rows)

    @property
    def columns(self):
        """An array of the columns defined for the result set.
        """
        return self.__columns

    def __str__(self):
        """Convert the result set to a human-friendly output.
        """
        return "\n".join(self.table_rows())

    def __iter__(self):
        """Iterate over the rows of the result set.

        Each row is a list of column values.

        """

        for row in self.__rows:
            yield row

    def __getitem__(self, index):
        """Indexing operator for result set.

        Will index the rows of the result set.

        """

        return self.__rows[index]

    def append_row(self, row):
        """Append a row to the result set.

        The row is an array (a list or tuple) of values that should be
        added to the result set. When adding the row, it will be
        checked that the length of the row matches the number of
        columns defined for the result set and that the types of the
        values match the type given for the column.

        :param row: An array of the values to add.

        """

        # Check that the length of the row matches the number of
        # columns for the result set
        if len(row) != len(self.__columns):
            message = "Invalid row length: expected %d, was %d" % (
                len(self.__columns), len(row)
            )
            raise _errors.CommandResultError(message)

        self.__rows.append(
            tuple(col.type(val) for col, val in zip(self.__columns, row))
        )


class CommandResult(object):
    """Command result class.

    The command result class contain the result of a procedure
    execution. This covers any errors returned and zero or more result
    sets.

    :param error: Error string, or None if there were no error.
    :param results: List of result sets, or a single result set to add.
    :param uuid: UUID that identifies the Fabric meta-data.
    :param ttl: Time-To-Live (TTL) in seconds.

    """

    def __init__(self, error, results=None, uuid=FABRIC_UUID, ttl=None):
        """Constructor.

        See class description for information.
        """
        self.__error = error
        self.__uuid = uuid
        self.__ttl = _utils.TTL
        self.__results = []

        # Check the TTL information if there is any. Otherwise, use
        # the value specified in the configuration file.
        if ttl is not None:
            try:
                self.__ttl = int(ttl)
            except ValueError:
                pass

        # Ensure that results is a list of results even if a single
        # result (or None) was passed as value.
        if results is None:
            results = []
        elif isinstance(results, ResultSet):
            results = [results]
        elif not isinstance(results, collections.Iterable):
            # The actual contents of the iterable is checked in the
            # append_result function.
            raise TypeError("Expected None, ResultSet, or iterable")

        # Append the result sets one by one to perform the standard
        # checks on adding result sets to the command result.
        for result in results:
            self.append_result(result)

    def emit(self, output):
        """Write a human-readable version of the command result.

        This will print a human-readable version of the command
        result, including all result sets, to the output provided.

        :param output: File object to write to.

        """

        rows = [
            "Fabric UUID:  %s" % self.uuid,
            "Time-To-Live: %d" % self.__ttl,
            "",
        ]

        if self.__error:
            rows.append(self.__error)
        elif self.__results:
            for rset in self.__results:
                rows.extend(rset.table_rows())
                rows.append("")

        output.writelines(row + "\n" for row in rows)
        output.write("\n")

    def __str__(self):
        """The command result as a string.
        """
        output = StringIO()
        self.emit(output)
        return output.getvalue()

    @property
    def uuid(self):
        """Fabric node UUID for command result.
        """
        return self.__uuid

    @property
    def ttl(self):
        """Time-To-Live for command result.
        """
        return self.__ttl

    @property
    def error(self):
        """Command result error, or None if there were no error.
        """
        return self.__error

    @property
    def results(self):
        """List of result sets.
        """
        return self.__results

    def append_result(self, result):
        """Append a result set to the list of result sets.

        :param result: Result set to add last to the list of result sets.
        """
        if not isinstance(result, ResultSet):
            raise _errors.CommandResultError(
                "Result have to be an instance of ResultSet"
            )
        if self.error:
            raise _errors.CommandResultError(
                "Result sets cannot be added for error results"
            )
        self.__results.append(result)

if __name__ == '__main__':
    import doctest
    doctest.testmod()
