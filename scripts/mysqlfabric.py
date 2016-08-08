#!/usr/bin/python
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

import os
import inspect
import sys
import textwrap
from getpass import getpass
from ConfigParser import NoOptionError
from urllib2 import HTTPError, URLError
import copy

# Check that the correct version of dependent packages are
# installed. This has to be done before importing any other packages
# than standard packages and mysql.fabric since the other mysql.fabric
# packages contain dependencies that might try to import things not
# available (e.g., from mysql.connector).
from mysql.fabric import (
    check_dependencies,
    errors,
    config as _config,
)

try:
    check_dependencies()
except errors.ConfigurationError as err:
    sys.stderr.write(str(err) + "\n")
    exit(1)

# We are now ready to start importing stuff that might require
# features in third-party tools.

from mysql.fabric.services import (
    find_commands,
    find_client,
)

from mysql.fabric.command import (
    get_groups,
    get_command,
    get_commands,
    get_arguments,
)

from mysql.fabric import (
    __version__,
    credentials,
)

from mysql.fabric.options import (
    OptionParser,
)

from mysql.fabric.config import (
    Config
)

from mysql.fabric.utils import (
    catch_signals
)

_ERR_COMMAND_MISSING = "command '%s' in group '%s' was not found."
_ERR_GROUP_MISSING = "group '%s' does not exist"
_ERR_EXTRA_ARGS = "Too many arguments to '%s'"

PARSER = OptionParser(
    usage="",
    version="%prog " + __version__,
    description=("MySQL Fabric {0} - MySQL server farm "
                 "management framework").format(__version__)
)


def help_command(group_name, command_name):
    """Print help on a command or group and exit.

    This will print help on a command, or an error if the command does
    not exist, and then exit with exit code 2.

    """
    try:
        # Get the command and information on its parameters.
        cargs = None
        cls = get_command(group_name, command_name)
        command_text = cls.get_signature()
        paragraphs = []
        if cls.__doc__:
            wrapper = textwrap.TextWrapper()
            paragraphs = []
            for para in cls.__doc__.split("\n\n"):
                if para:
                    paragraphs.append(wrapper.fill(para.strip()))
        print command_text, "\n\n", "\n\n".join(paragraphs)
    except KeyError as exc:
        msg = _ERR_COMMAND_MISSING % (command_name, group_name)
        PARSER.print_error(msg + '({0})'.format(exc))

    PARSER.exit(2)


def help_group(group_name):
    """Print help on a command group and exit.

    This will print a list of the commands available in the group, or
    an error message if the group is not available, and then exit with
    exit code 2.

    """
    indent = " " * 4
    try:
        commands = get_commands(group_name)
        print
        print "Commands available in group '%s' are:" % (group_name,)

        for cmdname in commands:
            print indent + get_command(group_name, cmdname).get_signature()
    except KeyError:
        PARSER.print_error(_ERR_GROUP_MISSING % (group_name, ))

    PARSER.exit(2)

def show_groups():
    """List groups that have been registered.

    This function list all groups that have been used anywhere when
    registering commands.

    """

    print "Available groups:", ", ".join(group for group in get_groups())


def show_commands():
    """List the possible commands and their descriptions.

    """

    commands = []
    max_name_size = 0

    for group_name in get_groups():
        for command_name in get_commands(group_name):
            cls = get_command(group_name, command_name)

            doc_text = ""
            if cls.__doc__ and cls.__doc__.find(".") != -1:
                doc_text = cls.__doc__[0: cls.__doc__.find(".") + 1]
            elif cls.__doc__:
                doc_text = cls.__doc__
            doc_text = [text.strip(" ") for text in doc_text.split("\n")]

            commands.append(
                (group_name, command_name, " ".join(doc_text))
            )

            name_size = len(group_name) + len(command_name)
            if name_size > max_name_size:
                max_name_size = name_size

    # Format each description and print the result.
    wrapper = textwrap.TextWrapper(
        subsequent_indent=(" " * (max_name_size + 3)))
    for group_name, command_name, help_text in commands:
        padding_size = max_name_size - len(group_name) - len(command_name)
        padding_size = 0 if padding_size < 0 else padding_size
        padding_text = "".rjust(padding_size, " ")
        help_text = wrapper.fill(help_text)
        text = (group_name, command_name, padding_text, help_text)
        print " ".join(text)


def show_help():
    """Show help on help

    """

    PARSER.print_help()


HELP_TOPIC = {
    'commands': (show_commands, 'List available commands'),
    'groups': (show_groups, 'List available groups'),
    'help': (show_help, 'Show help'),
}


def extract_command(args):
    """Extract group and command.

    If not both a group and command is provided, a usage message will
    be printed and the process will exit.

    """

    if len(args) < 2 or args[0] == "help":
        if len(args) == 0 or len(args) == 1 and args[0] == "help":
            PARSER.print_help()
            PARSER.exit(2)

        if args[0] == "help":
            if args[1] in HELP_TOPIC:  # Print help topic
                func, _ = HELP_TOPIC[args[1]]
                func()
                PARSER.exit(2)
            elif len(args) == 2:  # Print group help
                args.pop(0)
                help_group(args[0])
            elif len(args) == 3:  # Print command help
                args.pop(0)
                help_command(args[0], args[1])
            else:  # Too many arguments
                PARSER.print_error(_ERR_EXTRA_ARGS % 'help')
                PARSER.print_help()
                PARSER.exit(2)

        assert len(args) == 1
        help_group(args[0])

    return args[0], args[1], args[2:]


def authenticate(group_name, command_name, config, options, args):
    """Prompt for username and password when needed
    """
    protocol = "xmlrpc"
    protocol_section = 'protocol.xmlrpc'

    if (group_name == 'manage' and
        command_name in ('setup', 'start', 'teardown')):
        return

    try:
        realm = config.get(protocol_section, 'realm')
    except NoOptionError:
        config.set(protocol_section, 'realm', credentials.FABRIC_REALM_XMLRPC)

    # Handled disabled authentication
    try:
        value = config.get(protocol_section, 'disable_authentication')
        if value.lower() == 'yes':
            config.set(protocol_section, 'user', '')
            config.set(protocol_section, 'password', '')
            return
    except NoOptionError:
        # OK when disable_authentication is missing
        pass

    # The username from command line argument --user
    try:
        cmd_options, _ = PARSER.parse_args(args)
        username = cmd_options.auth_user
    except AttributeError:
        username = None

    password = None
    if not username:
        try:
            username = config.get(protocol_section, 'user')
        except NoOptionError:
            # Referred to default 'admin' user
            username = 'admin'

        try:
            password = config.get(protocol_section, 'password')
        except NoOptionError:
            password = None

    if not password:
        password = getpass('Password for {user}: '.format(user=username))
        config.set(protocol_section, 'password', password.strip())

    config.set(protocol_section, 'user', username.strip())

    credentials.check_credentials(
        group_name, command_name, config, protocol
    )


def create_command(group_name, command_name, options, args, config):
    """Create command object.
    """
    options = None

    try:
        # Fetch command class and create the command instance.
        command = get_command(group_name, command_name)()

        # Set up options for command
        command.add_options(PARSER)

        # Parse arguments
        options, args = PARSER.parse_args(args, options)

        # Create a protocol client for dispatching the command and set
        # up the client-side information for the command. Inside a
        # shell, this only have to be done once, but here we need to
        # set up the client-side of the command each time we call the
        # program.
        client = find_client()
        command.setup_client(client, options, config)
        return command, args
    except KeyError:
        PARSER.error(
            "Command (%s %s) was not found." %
            (group_name, command_name, )
        )


def error_usage_text(group_name, command_name):
    """Print Usage information upon error while invoking the command.

    :param group_name: The Group of the command (sharding, server etc).
    :param command_name: The command name.
    """
    cls = get_command(group_name, command_name)
    command_text = cls.get_signature()
    print "Usage: ", command_text, "\n"
    PARSER.error(
        "Wrong number of parameters were provided "
        "for command '%s %s'." % (
            group_name, command_name,
        )
    )

def fire_command(command, *args):
    """Fire a command.

    :param arg: Arguments used by the command.
    """
    # Get the number of mandatory arguments for the command by inspecting
    # the class definition of the command. It is important to check that the
    # number of mandatory arguments for a command are provided because
    # there is the danger that the value of an optional parameter gets wrapped
    # as the value for a mandatory argument when we don't provide the value
    # for an mandatory argument
    spec = get_arguments(command)

    defaults_len = 0
    if spec.defaults:
        defaults_len = len(spec.defaults)

    if len(args) != len(spec.args) - defaults_len - 1:
        error_usage_text(command.group_name, command.command_name)

    # Execute command by dispatching it on the client side. Append the
    # optional arguments passed by the user to the argument list.
    result = command.dispatch(*(command.append_options_to_args(args)))

    if result is not None:
        result.emit(sys.stdout)
        error = 1 if result.error else 0
    else:
        error = 0

    return error

def main():
    """Start mysqlfabric.py script
    """
    try:
        # Catch some signals such as SIGUSR1 and SIGINT.
        catch_signals()

        # Load information on available commands.
        find_commands()

        # Identify exceptions to normal calling conventions, basically
        # for printing various forms of help.

        # This require us to first fetch all options before the
        # (potential) group and command using the default option
        # parser.
        PARSER.disable_interspersed_args()
        options, args = PARSER.parse_args()
        PARSER.enable_interspersed_args()

        # Options with side effects, such as --help and --version,
        # will never come here, so after this all option arguments are
        # eliminated and the first word in "args" is the the name of a
        # command group.

        # At this point, we (should) have at least one non-option word
        # in the arguments, so we try to get the group and the command
        # for the group. This might fail, if just a group is provided,
        # but then the function will print an apropriate message and
        # exit.
        group_name, command_name, args = extract_command(args)

        # If no configuration file was provided, figure out the
        # location of it based on the installed location of the
        # script location.
        if not options.config_file:
            try:
                directory = os.path.dirname(__file__)
            except NameError:
                if hasattr(sys, 'frozen'):
                    directory = os.path.dirname(sys.executable)
                else:
                    directory = os.path.abspath(
                        inspect.getfile(inspect.currentframe()))
            prefix = os.path.realpath(os.path.join(directory, '..'))
            if os.name == 'posix' and prefix in ('/', '/usr'):
                config_file = '/etc/mysql/fabric.cfg'
            else:
                if hasattr(sys, 'frozen'):
                    prefix = os.path.realpath(directory)
                config_file = os.path.join(prefix, 'etc', 'mysql', 'fabric.cfg')
            options.config_file = os.path.normpath(config_file)

        # Read configuration file
        config = Config(options.config_file, options.config_params)
        _config.global_config = copy.copy(config)

        cmd, cargs = create_command(group_name, command_name,
                                    options, args, config)

        authenticate(group_name, command_name, config, options, args)

        return fire_command(cmd, *cargs)
    except (URLError, HTTPError, NoOptionError) as error:
        if hasattr(error, 'code') and error.code == 400:
            print "Permission denied."
        else:
            print str(error)
    except KeyboardInterrupt:
        print "\nAborted due to keyboard interrupt."
        return 130
    except errors.Error as error:
        print "Error: {0}".format(error)
    except IOError as error:
        print "Error reading configuration file: {0}".format(error)

    return 1

if __name__ == '__main__':
    sys.exit(main())
