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
"""This module manages a null provider which is used only for testing
the system.
"""
import logging
import time
import uuid as _uuid

from mysql.fabric.providers import (
    AbstractMachineManager,
    AbstractSnapshotManager
)

from mysql.fabric.machine import (
    Machine,
)

_LOGGER = logging.getLogger(__name__)

def configure_provider():
    """Configure NullProvider.
    """
    return ("NULLPROVIDER", MachineManager, SnapshotManager, 1)

class MachineManager(AbstractMachineManager):
    """Manage machines.
    """
    def __init__(self, provider, version=None):
        super(MachineManager, self).__init__(provider, version)

    def create(self, parameters, wait_spawning):
        """Create machines.
        """
        _LOGGER.debug(
            "Creating a machine: provider (%s), parameters (%s).",
            self.provider, parameters
        )

        machine = Machine(
            uuid=_uuid.uuid4(), provider_id=self.provider.provider_id,
            av_zone="av_zone:host", addresses="127.0.0.1"
        )
        return [machine]

    def search(self, generic_filters, meta_filters):
        """Return machines based on the provided filters.
        """
        machine = Machine(
            uuid=_uuid.uuid4(), provider_id=self.provider.provider_id,
            av_zone="av_zone:host", addresses="127.0.0.1"
        )
        return [machine]

    def destroy(self, machine_uuid):
        """Destroy a machine.
        """
        pass

    def assign_public_ip(self, machine, pool):
        """Assign public IP address to a machine.
        """
        pass

    def remove_public_ip(self, machine):
        """Remove public addresses assigned to a machine.
        """
        pass

class SnapshotManager(AbstractSnapshotManager):
    """Manage Snapshots (i.e. Images).
    """
    def __init__(self, provider, version=None):
        super(SnapshotManager, self).__init__(provider, version)

    def create(self, machine_uuid, wait_spawning):
        """Create a snapshot from a machine.
        """
        return "-".join(["snapshot", machine_uuid, str(time.time())])

    def destroy(self, machine_uuid):
        """Destroy snapshots associated to a machine.
        """
        pass
