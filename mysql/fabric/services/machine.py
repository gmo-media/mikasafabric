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

_LOGGER = logging.getLogger(__name__)

VALID_PARAMETERS = (
    'image', 'flavor', 'number_machines', 'availability_zone', 'key_name',
    'security_groups', 'private_network', 'public_network', 'userdata',
    'swap', 'block_device', 'scheduler_hints', 'private_nics', 'public_nics',
    'meta'
)

# TODO: TRY TO FIND A BETTER PLACE FOR THIS FUNCTION.
def search(provider_id, generic_filters, meta_filters, skip_store):
    """Return information on existing machine(s).
    """
    provider = _retrieve_provider(provider_id)

    if not skip_store:
        if generic_filters or meta_filters:
            raise ConfigurationError(
                "Filters are only supported when the 'skip_store' option "
                "is set."
            )
        machines = Machine.machines(provider.provider_id)
    else:
        generic_filters, meta_filters = \
           _preprocess_filters(generic_filters, meta_filters)
        manager = provider.get_provider_machine()

        if issubclass(manager, AbstractDatabaseManager):
            meta_filters = {}
            manager = _retrieve_database_manager(provider_id)
        elif issubclass(manager, AbstractMachineManager):
            manager = _retrieve_machine_manager(provider_id)

        machines = manager.search(generic_filters, meta_filters)

    rset = ResultSet(
    names=('uuid', 'provider_id', 'av_zone', 'addresses'),
        types=(str, str, str, str)
    )

    for machine in machines:
        row = (
            str(machine.uuid), machine.provider_id, machine.av_zone,
            machine.addresses
        )
        rset.append_row(row)
    return CommandResult(None, results=rset)

CREATE_MACHINE = _events.Event('CREATE_MACHINE')
@_events.on_event(CREATE_MACHINE)
def _create_machine(provider_id, parameters, machine_group_uuid,
                    skip_store, wait_spawning):
    """Create a machine.
    """
    manager = _retrieve_machine_manager(provider_id)

    _preprocess_parameters(parameters, machine_group_uuid, manager.provider)
    machines = manager.create(parameters, wait_spawning)

    _LOGGER.debug("Created machine(s) (%s).", machines)

    if not skip_store:
        for machine in machines:
            Machine.add(machine)

    return [machine.as_dict() for machine in machines]

@_create_machine.undo
def _undo_create_machine(provider_id, parameters, machine_group_uuid,
                          skip_store, wait_spawning):
    """Remove machines already created if there is an error.
    """
    generic_filters = {}
    meta_filters = {
        'mysql-fabric-machine-group-uuid' : machine_group_uuid
    }
    manager = _retrieve_machine_manager(provider_id)
    for machine in manager.search(generic_filters, meta_filters):
        manager.destroy(str(machine.uuid))

DESTROY_MACHINE = _events.Event('DESTROY_MACHINE')
@_events.on_event(DESTROY_MACHINE)
def _destroy_machine(provider_id, machine_uuid, force, skip_store):
    """Destroy a machine.
    """
    machine = _retrieve_machine(provider_id, machine_uuid, skip_store)
    if machine:
        Machine.remove(machine)
    manager = _retrieve_machine_manager(provider_id)
    try:
        manager.destroy(machine_uuid)
    except MachineError:
        if not force:
            raise

    _LOGGER.debug("Destroyed machine (%s).", machine)

CREATE_SNAPSHOT = _events.Event('CREATE_SNAPSHOT')
@_events.on_event(CREATE_SNAPSHOT)
def _create_snapshot(provider_id, machine_uuid, skip_store, wait_spawning):
    """Create a snapshot.
    """
    _retrieve_machine(provider_id, machine_uuid, skip_store)
    manager = _retrieve_snapshot_manager(provider_id)
    return manager.create(machine_uuid, wait_spawning)

DESTROY_SNAPSHOT = _events.Event('DESTROY_SNAPSHOT')
@_events.on_event(DESTROY_SNAPSHOT)
def _destroy_snapshot(provider_id, machine_uuid, skip_store):
    """Destroy a snapshot.
    """
    _retrieve_machine(provider_id, machine_uuid, skip_store)
    manager = _retrieve_snapshot_manager(provider_id)
    return manager.destroy(machine_uuid)

def _retrieve_machine(provider_id, machine_uuid, skip_store):
    """Return a machine object from an id.
    """
    machine = Machine.fetch(machine_uuid)

    if not machine and not skip_store:
        raise MachineError(
            "Machine (%s) does not exist." % (machine_uuid, )
        )

    if machine and machine.provider_id != provider_id:
        raise MachineError(
            "Machine (%s) does not belong to provider (%s)." %
            (machine_uuid, provider_id)
        )

    return machine

def _retrieve_machine_manager(provider_id):
    """Retrive machine manager for a provider.
    """
    provider = _retrieve_provider(provider_id)
    MachineManager = provider.get_provider_machine()
    if not issubclass(MachineManager, AbstractMachineManager):
        raise ConfigurationError(
            "Provider (%s) does not allow to manage machines directly." %
            (provider_id, )
        )
    return MachineManager(provider)

def _retrieve_snapshot_manager(provider_id):
    """Retrive snapshot manager for a provider.
    """
    provider = _retrieve_provider(provider_id)
    SnapshotManager = provider.get_provider_snapshot()
    if not issubclass(SnapshotManager, AbstractSnapshotManager):
        raise ConfigurationError(
            "Provider (%s) does not allow to manage machine snapshots "
            "directly." % (provider_id, )
        )
    return SnapshotManager(provider)

def _retrieve_database_manager(provider_id):
    """Retrive machine manager for a provider.
    """
    provider = _retrieve_provider(provider_id)
    DatabaseManager = provider.get_provider_machine()
    if not issubclass(DatabaseManager, AbstractDatabaseManager):
        raise ConfigurationError(
            "Provider (%s) does not allow to manage machine databases "
            "directly." % (provider_id, )
        )
    return DatabaseManager(provider)

def _preprocess_parameters(parameters, machine_group_uuid, provider):
    """Process paramaters.
    """
    # Check whether all parameters are expected.
    for key, value in parameters.items():
        if key not in VALID_PARAMETERS and (value is not None and value):
            raise MachineError(
                "Parameter (%s) is not in the set of possible parameters: %s.",
                key, VALID_PARAMETERS
            )
        elif key not in VALID_PARAMETERS:
            del parameters[key]

    # 1. Put image parameter in the appropriate format.
    if parameters['image']:
        parameters['image'] = kv_to_dict(parameters['image'])
    elif provider.default_image:
        parameters['image'] = {'name' : provider.default_image}
    if not parameters['image']:
        raise MachineError("No valid image hasn't been found.")

    # 2. Put flavor parameter in the appropriate format.
    if parameters['flavor']:
        parameters['flavor'] = kv_to_dict(parameters['flavor'])
    elif provider.default_flavor:
        parameters['flavor'] = {'name' : provider.default_flavor}
    if not parameters['flavor']:
        raise MachineError("No valid flavor hasn't been found.")

    # 3. Check the parameter number_machines.
    number_machines = parameters['number_machines']
    try:
        number_machines = int(number_machines)
        parameters['number_machines'] = number_machines
    except TypeError:
        number_machines = 1
        parameters['number_machines'] = number_machines
    if number_machines <= 0:
        raise MachineError(
            "Number of machines must be greater than zero (%s)." %
            (number_machines, )
        )

    # 4. We don't need to check the availability_zone parameter

    # 5. We don't need to check the parameter key_name parameter.

    # 6. Put the security_groups parameter in the appropriate format.
    if parameters['security_groups']:
        security_groups = parameters['security_groups'].split(',')
        parameters['security_groups'] = security_groups

    # 7. Check the private_newtwork parameter.
    private_nics = parameters['private_nics']
    private_network = parameters['private_network']
    if private_network and private_nics:
        raise ConfigurationError(
            "Can't define both private_network (%s) and private_nics "
            "parameters (%s)." % (private_network, private_nics)
        )

    # 8. Check the public_newtwork parameter.
    public_nics = parameters['public_nics']
    public_network = parameters['public_network']
    if public_network and public_nics:
        raise ConfigurationError(
            "Can't define both public_network (%s) and public_nics "
            "parameters (%s)." % (public_network, public_nics)
        )

    # 9. Read userdata parameter which must be a path to a file.
    if parameters['userdata']:
        try:
            src = parameters['userdata']
            userdata = open(src)
        except IOError as error:
            raise ConfigurationError(
                "Can't open '%(src)s': %(exc)s" % {'src': src, 'exc': error}
            )
        parameters['userdata'] = userdata

    # 10. We don't need to check the swap parameter

    # 11. Put the block_device parameter in the appropriate format.
    if parameters['block_device']:
        raise ConfigurationError(
            "Parameter block_device is not supported yet."
        )

    # 12. Put the scheduler_hints parameter in the appropriate format.
    if parameters['scheduler_hints']:
        parameters['scheduler_hints'] = \
            kv_to_dict(parameters['scheduler_hints'])

    # 13. Put the private_nics parameter in the appropriate format.
    if parameters['private_nics']:
        raise ConfigurationError(
            "Parameter private_nics is not supported yet."
        )

    # 14. Put the public_nics parameter in the appropriate format.
    if parameters['public_nics']:
        raise ConfigurationError(
            "Parameter public_nics is not supported yet."
        )

    # 15. Put meta parameter in the appropriate format.
    reserved_value = (
        'True', str(FabricNode().version), str(FabricNode().uuid),
        str(FabricNode().group_uuid), machine_group_uuid
    )
    assert len(reserved_meta) == len(reserved_value)
    if parameters['meta']:
        parameters['meta'] = kv_to_dict(parameters['meta'])
        if any(key in reserved_meta for key in parameters['meta'].iterkeys()):
            raise ConfigurationError(
                "The meta parameter cannot have keywords in the following "
                "list: %s. They are reserved for internal use." %
                (str(reserved_meta), )
            )
    else:
        parameters['meta'] = {}

    meta = dict(zip(reserved_meta, reserved_value))
    parameters['meta'].update(meta)

reserved_meta = (
    'mysql-fabric', 'mysql-fabric-version', 'mysql-fabric-uuid',
    'mysql-fabric-group', 'mysql-fabric-machine-group-uuid'
)

def _preprocess_filters(generic_filters, meta_filters):
    """Process filters.
    """
    if generic_filters:
        generic_filters = kv_to_dict(generic_filters)
    else:
        generic_filters = {}

    if meta_filters:
        meta_filters = kv_to_dict(meta_filters)
    else:
        meta_filters = {}

    meta_filters['mysql-fabric'] = 'True'

    if any(key in reserved_meta for key in generic_filters.iterkeys()):
        raise ConfigurationError(
            "Generic filters option cannot have keyords in the following "
            "list: %s." % (str(reserved_meta), )
        )

    return generic_filters, meta_filters
