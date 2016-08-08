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
"""Provide the necessary interface to manage virtual machine instances.
"""
import logging
import uuid as _uuid

from mysql.fabric.command import (
    ProcedureCommand,
    Command,
    ResultSet,
    CommandResult,
)

from mysql.fabric import (
    events as _events,
)

from mysql.fabric.errors import (
    MachineError,
    ConfigurationError
)

from mysql.fabric.machine import (
    Machine,
)

from mysql.fabric.services.provider import (
    _retrieve_provider,
)

from mysql.fabric.node import (
    FabricNode,
)

from mysql.fabric.utils import (
    kv_to_dict,
)

from mysql.fabric.providers import (
    AbstractMachineManager,
    AbstractSnapshotManager,
    AbstractDatabaseManager
)

from mysql.fabric.services.machine import (
    search,
)

_LOGGER = logging.getLogger(__name__)

class CreateMachine(ProcedureCommand):
    """Create a virtual machine instance:

        mysqlfabric server create provider --image name=image-mysql \
        --flavor name=vm-template --meta db=mysql --meta version=5.6

        mysqlfabric server create provider --image name=image-mysql \
        --flavor name=vm-template --security_groups grp_fabric, grp_ham

    Options that accept a list are defined by providing the same option
    multiple times in the command-line. The image, flavor, files, meta
    and scheduler_hints are those which might be defined multiple times.
    Note the the security_groups option might be defined only once but
    it accept a string with a list of security groups.
    """
    group_name = "server"
    command_name = "create"

    def execute(self, provider_id, image=None, flavor=None,
        number_machines=1, availability_zone=None, key_name=None,
        security_groups=None, private_network=None, public_network=None,
        userdata=None, swap=None, scheduler_hints=None, meta=None,
        datastore=None, datastore_version=None, size=None, databases=None,
        users=None, configuration=None, security=None, skip_store=False,
        wait_spawning=True, synchronous=True):
        """Create a machine.

        :param provider_id: Provider's Id.
        :param image: Image's properties (e.g. name=image-mysql).
        :rtype image: list of key/value pairs
        :param flavor: Flavor's properties (e.g. name=vm-template).
        :rtype flavor: list of key/value pairs
        :param number_machines: Number of machines to be created.
        :rtype number_machines: integer
        :param availability_zone: Name of availability zone.
        :rtype availability_zone: string
        :param key_name: Name of the key previously created.
        :rtype key_name: string
        :param security_groups: Security groups to have access to the
                                machine(s).
        :rtype security_groups: string with a list of security groups
        :param private_network: Name of the private network where the
                                machine(s) will be placed to.
        :rtype key_name: string
        :param public_network: Name of the public network which will provide
                               a public address.
        :rtype key_name: string
        :param userdata: Script that to be used to configure the machine(s).
        :rtype userdata: path to a file
        :param swap: Size of the swap space in megabyte.
        :rtype swap: integer
        :param scheduler_hints: Information on which host(s) the machine(s) will
                                be created in.
        :rtype scheduler_hints: list of key/value pairs
        :param meta: Metadata on the machine(s).
        :rtype meta: list of key/value pairs
        :param datastore: Database Technology (.e.g. MySLQ).
        :rtype datastore: string
        :param datastore_version: Datastore version (.e.g. 5.6).
        :rtype datastore_version: string
        :param size: Storage area reserved to the data store.
        :rtype size: string in Gigabytes
        :param databases: Database objects that will be created.
        :rtype databases: List of strings separated by comma
        :param configuration: Configuration attached to the database.
        :rtype configuration: string
        :param security: By default 0.0.0.0/0 is set. Users who want a differnt
                         permission should specify a different value.
        :rtype security: string
        :param skip_store: Do not store information on machine(s) into the
                           state store. Default is False.
        :param wait_spwaning: Whether one should wait until the provider
                              finishes its task or not.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """
        parameters = {
            'image' : image,
            'flavor' : flavor,
            'number_machines' : number_machines,
            'availability_zone' : availability_zone,
            'key_name' : key_name,
            'security_groups' : security_groups,
            'private_network' : private_network,
            'public_network' : public_network,
            'userdata' : userdata,
            'swap' : swap,
            'block_device' : None,
            'scheduler_hints' : scheduler_hints,
            'private_nics' : None,
            'public_nics' : None,
            'meta' : meta,
            'datastore' : datastore,
            'datastore_version' : datastore_version,
            'size' : size,
            'databases' : databases,
            'users' : users,
            'configuration' : configuration,
            'security' : security,
        }
        machine_group_uuid = str(_uuid.uuid4())
        procedures = _events.trigger(
            _decide_create_event(provider_id), self.get_lockable_objects(),
            provider_id, parameters, machine_group_uuid, skip_store,
            wait_spawning
        )
        return self.wait_for_procedures(procedures, synchronous)

    def generate_options(self):
        """Make some options accept multiple values.
        """
        super(CreateMachine, self).generate_options()
        options = [
            "image", "flavor", "meta", "scheduler_hints", "private_network"
        ]
        for option in self.command_options:
            if option['dest'] in options:
                option['action'] = "append"

def _decide_create_event(provider_id):
    """Decide whether it should call a machine or a database provider.
    """
    provider = _retrieve_provider(provider_id)
    manager = provider.get_provider_machine()
    if issubclass(manager, AbstractMachineManager):
        return 'CREATE_MACHINE'
    elif issubclass(manager, AbstractDatabaseManager):
        return 'CREATE_DATABASE'

    assert False

class MachineDestroy(ProcedureCommand):
    """Destroy a virtual machine instance.
    """
    group_name = "server"
    command_name = "destroy"

    def execute(self, provider_id, machine_uuid, force=False,
                skip_store=False, synchronous=True):
        """Destroy a machine.

        :param provider_id: Provider's Id.
        :param machine_uuid: Machine's uuid.
        :param force: Ignore errors while accessing the cloud provider.
        :param skip_store: Proceed anyway if there is no information on
                           the machine in the state store. Default is False.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            _decide_destroy_event(provider_id), self.get_lockable_objects(),
            provider_id, machine_uuid, force, skip_store
        )
        return self.wait_for_procedures(procedures, synchronous)

def _decide_destroy_event(provider_id):
    """Decide whether it should call a machine or a database provider.
    """
    provider = _retrieve_provider(provider_id)
    manager = provider.get_provider_machine()
    if issubclass(manager, AbstractMachineManager):
        return 'DESTROY_MACHINE'
    elif issubclass(manager, AbstractDatabaseManager):
        return 'DESTROY_DATABASE'

    assert False

class MachineLookup(Command):
    """Return information on existing machine(s) created by a provider.
    """
    group_name = "server"
    command_name = "list"

    def execute(self, provider_id, generic_filters=None, meta_filters=None,
                skip_store=False):
        """Return information on existing machine(s).

        Only a list of machines created by Fabric will be returned by this
        command. The list can be directly obtained from the state store or
        from the provider if the 'skip_store' parameter is defined.

        If the list is retrieved from the provider, a set of filters might
        be specified. The 'generic_filters' parameter accept key-value pairs
        that represent core properties such as id, name, vcpus, ram, etc.
        The actual set of possible key-value pairs depend on the provider
        in use though and the appropriate documentation should be read. The
        'meta_filters' parameter accept the key-value pairs that were set
        when the machine was created.

        :param provider_id: Provider's Id.
        :param generic_filters: Set of key-value pairs that are used to
                                filter the list of returned machines.
        :param meta_filters: Set of key-value pairs that are used to filter
                             the list of returned machines.
        :param skip_store: Don't check the list of machines from the state
                           store.
        """
        return search(provider_id, generic_filters, meta_filters, skip_store)

    def generate_options(self):
        """Make some options accept multiple values.
        """
        super(MachineLookup, self).generate_options()
        options = ["generic_filters", "meta_filters"]
        for option in self.command_options:
            if option['dest'] in options:
                option['action'] = "append"

class SnapshotCreate(ProcedureCommand):
    """Create a snapshot image from a machine.
    """
    group_name = "snapshot"
    command_name = "create"

    def execute(self, provider_id, machine_uuid, skip_store=False,
                wait_spawning=True, synchronous=True):
        """Create a snapshot image from a machine.

        :param provider_id: Provider's Id.
        :param machine_uuid: Machine's uuid.
        :param skip_store: Proceed anyway if there is no information on
                           the machine in the state store. Default is False.
        :param wait_spwaning: Whether one should wait until the provider
                              finishes its task or not.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            'CREATE_SNAPSHOT', self.get_lockable_objects(), provider_id,
            machine_uuid, skip_store, wait_spawning
        )
        return self.wait_for_procedures(procedures, synchronous)

class SnapshotDestroy(ProcedureCommand):
    """Destroy snapshot images associated to a machine.
    """
    group_name = "snapshot"
    command_name = "destroy"

    def execute(self, provider_id, machine_uuid, skip_store=False,
                synchronous=True):
        """Destroy snapshot images associated to a machine.

        :param provider_id: Provider's Id.
        :param machine_uuid: Machine's uuid.
        :param skip_store: Proceed anyway if there is no information on
                           the machine in the state store. Default is False.
        :param wait_spwaning: Whether one should wait until the provider
                              finishes its task or not.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            'DESTROY_SNAPSHOT', self.get_lockable_objects(), provider_id,
            machine_uuid, skip_store
        )
        return self.wait_for_procedures(procedures, synchronous)
