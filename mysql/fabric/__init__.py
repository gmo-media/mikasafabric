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
"""MySQL Fabric library
"""

import distutils.version
import os

# Version info as a tuple (major, minor, patch, extra)
__version_info__ = (1, 5, 6, "")

# MySQL Fabric version:
# `PEP-386 <http://www.python.org/dev/peps/pep-0386>`__ format
__version__ = "{0}.{1}.{2}{3}".format(*__version_info__)

# Connector Python version info as a tuple (major, minor, patch, extra)
__cpy_version_info__ = (2, 0, 4, "")
__cpy_with_c_version_info__ = (2, 1, 2, "")

# Connector Python version:
# `PEP-386 <http://www.python.org/dev/peps/pep-0386>`__ format
__cpy_version__ = "{0}.{1}.{2}{3}".format(*__cpy_version_info__)
__cpy_with_c_version__ = "{0}.{1}.{2}{3}".format(*__cpy_with_c_version_info__)

def check_connector():
    """Check if the connector is properly configured and
    has the required version.
    """
    from mysql.fabric.errors import (
        ConfigurationError
    )

    try:
        import mysql.connector as cpy
        path = os.path.dirname(cpy.__file__)

        # Whether the connector has support to C-Extensions can be identified
        # by the minor release number. For that reason, we need at least two
        # fields for the version:  [0] - major version and [1] minor version.
        cpy_ver = distutils.version.LooseVersion(cpy.__version__)
        if len(cpy_ver.version) < 2:
            raise ConfigurationError(
                "Connector has version {cpy_ver} that only has a major "
                "number and a minor number is expected as well (looked "
                "in {path}).".format(path=path, cpy_ver=cpy_ver)
            )
        # [1] Minor version 1 means connector with C-Extensions.
        if cpy_ver.version[1] == 1:
            cpy_req = distutils.version.LooseVersion(__cpy_with_c_version__)
        else:
            cpy_req = distutils.version.LooseVersion(__cpy_version__)

        if  cpy_ver < cpy_req:
            raise ConfigurationError(
                "Connector has version {cpy_ver} but {cpy_req} "
                "(or later) required (looked in {path}).".format(
                    path=path, cpy_ver=cpy_ver, cpy_req=cpy_req
                )
            )
    except ImportError as error:
        import mysql
        path = os.path.dirname(mysql.__file__)
        raise ConfigurationError(
            "Tried to look for mysql.connector at ({path}). Connector not "
            "installed. Error ({error}).".format(path=path, error=error)
        )

def check_dependencies():
    check_connector()

