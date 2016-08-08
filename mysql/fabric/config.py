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

"""Reading the configuration files and options.

This module implements support needed to read the configuration files
and set up the configuration used both by the Fabric nodes and by the
clients.

Reading the configuration goes through three steps:

1. Read the site-wide configuration file.

2. Read the command-line configuration file (if provided). Settings in
   this file will override any settings read from the site-wide
   configuration file.

3. Some options override settings in both the configuration files.

"""

import ConfigParser
import os
import re

# These are propagated to the importer
from ConfigParser import NoSectionError, NoOptionError

# global storage for the configuration
global_config = None # pylint: disable=C0103

_VALUE_CRE = re.compile(
    r'(?P<section>\w+(?:\.\w+)*)\.(?P<name>\w+)=(?P<value>.*)')

def parse_param(option, _opt, value, parser):
    """Parser to parse a param option of the form x.y.z=some-value.

    This function is used as a callback to parse param values given on
    the command-line.
    """

    mobj = _VALUE_CRE.match(value)
    if mobj:
        field = getattr(parser.values, option.dest)
        if field is None:
            field = {}
            setattr(parser.values, option.dest, field)
        section = field.setdefault(mobj.group('section'), {})
        section[mobj.group('name')] = mobj.group('value')

class Config(ConfigParser.SafeConfigParser):
    """Fabric configuration file parser and configuration handler.

    This class manages the configuration of Fabric nodes and clients,
    including configuration file locations.

    Sample usage::

       from mysql.fabric.options import OptionParser
       from mysql.fabric.config import Config

       parser = OptionParser()
       ...
       options, args = parser.parse_args()
       config = Config(options.config_file, options.config_params)

    """

    def normalize_ssl_config(self, section):
        """Normalizes the SSL option in a section

        :param section: Section from which we read SSL configuration.
        """
        if not self.config_file:
            return
        default_path = os.path.dirname(self.config_file)
        try:
            for option in ('ssl_ca', 'ssl_key', 'ssl_cert'):
                value = self.get(section, option)
                if not value:
                    self.remove_option(section, option)
                elif not os.path.isabs(value):
                    self.set(section, option, os.path.join(default_path, value))
        except NoOptionError:
            # It's OK when SSL is missing
            pass

    def __init__(self, config_file, config_params=None):
        """Create the configuration parser, read the configuration
        files, and set up the configuration from the options.
        """

        ConfigParser.SafeConfigParser.__init__(self)

        if config_file is not None:
            self.readfp(open(config_file))
        self.config_file = config_file

        # Incorporate options into the configuration. These are read
        # from the mapping above and written into the configuration.
        if config_params is not None:
            for section, var_dict in config_params.items():
                if not self.has_section(section):
                    self.add_section(section)
                for key, val in var_dict.items():
                    self.set(section, key, val)

        self.normalize_ssl_config('protocol.xmlrpc')
