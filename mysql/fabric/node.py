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
"""Provides an abstraction that represents the Fabric Node.
"""
import uuid as _uuid

from mysql.fabric.utils import Singleton
from mysql.fabric import __version__

class FabricNode(Singleton):
    """An abstraction that represents the Fabric Node.

    :param uuid: The UUID for the FabricNode. None if a UUID should
                 be automatically generated.
    """
    def __init__(self, uuid=None):
        """Constructor for FabricNode.
        """
        assert(uuid is None or isinstance(uuid, _uuid.UUID))
        # This is temporary set to 0 to make it compatible with FABRIC_UUID in
        # utils.py.
        self.__uuid = 0
        self.__group_uuid = 0
        self.startup = None

    @property
    def group_uuid(self):
        """Return Fabric Group Identification.
        """
        return self.__group_uuid

    @property
    def uuid(self):
        """Return Fabric Identification.
        """
        return self.__uuid

    @property
    def version(self):
        """Return Fabric version.
        """
        return __version__
