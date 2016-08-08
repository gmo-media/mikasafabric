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

import optparse

import mysql.fabric.config as _config

class OptionParser(optparse.OptionParser):
    """Option with default options for all tools.

    This class is used as default option parser and specific commands
    can add their own options.

    There are three options that are provided:

    .. option:: --param <section>.<name>=<value> ...

       This option allow configuration parameters to be overridden by
       providing them on the command line.  The parameters are stored
       in the config_param attribute

    .. option:: --config <filename>

       Name of an extra configuration file to read, in addition to the
       site-wide configuration file. Options given in this file will
       override the options given in the site-wide file.

    Based on this, the options structure returned can hold the
    following attributes:

    .. attribute:: config_file

       File name for extra configuration file to read, provided by the
       :option:`--config` option. Defaults to ``fabric.cfg``.

    .. attribute:: config_param

       The configuration parameters provided with :option:`--param` as a
       dictionary of dictionaries, for example::

         {
            'protocol.xmlrpc': {
               'address': 'localhost:32274',
            },
            'logging': {
               'level': 'INFO',
            },
         }

    """
    def __init__(self, *args, **kwrds):
        optparse.OptionParser.__init__(self, *args, **kwrds)

        self.add_option(
            "--param",
            action="callback", callback=_config.parse_param,
            type="string", nargs=1, dest="config_params",
            help="Override a configuration parameter.")
        self.add_option(
            "--config",
            action="store", dest="config_file", default=None,
            metavar="FILE",
            help="Read configuration from FILE.")

    def print_error(self, msg):
        """Format and print an error message to the standard output.

        :param msg: Error message.
        """
        print "%s: %s\n" % (self.get_prog_name(), msg)

    def print_help(self):
        """Print help information to the standard output.
        """
        self.set_usage(
            "Usage: %prog [--param, --config] <grp> <cmd> [arg, ...]."
        )

        optparse.OptionParser.print_help(self)

        print "\n".join([
                "",
                "Basic commands:",
                "    help <grp> <cmd>  Show help for command",
                "    help commands     List all commands",
                "    help groups       List all groups",
        ])
