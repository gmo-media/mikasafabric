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
"""This module is reponsible for managing cloud providers by providing the
necessary means to register, unresgister and list providers.
"""
import logging

from mysql.fabric.command import (
    ProcedureCommand,
    Command,
    ResultSet,
    CommandResult,
)

from mysql.fabric import (
    events as _events,
    errors as _errors,
)

from mysql.fabric.provider import (
    Provider,
)

from mysql.fabric.machine import (
    Machine,
)

from mysql.fabric.utils import (
    kv_to_dict,
)

_LOGGER = logging.getLogger(__name__)

REGISTER_PROVIDER = _events.Event()
class ProviderRegister(ProcedureCommand):
    """Register a provider.
    """
    group_name = "provider"
    command_name = "register"

    def execute(self, provider_id, username, password, url, tenant=None,
                provider_type="OPENSTACK", default_image=None,
                default_flavor=None, extra=None, synchronous=True):
        """Register a provider.

        :param provider_id: Provider's Id.
        :param username: User name to use during authentication.
        :param password: Password to use during authentication.
        :param url: URL that is used as an access point.
        :param tenant: Tenant's name, i.e. who will access resources
                       in the cloud.
        :param provider_type: Provider type.
        :param image: Default image's name that will be used upon creating
                      a machine if one is not provided.
        :param image: Default flavor's name that will be used upon creating
                      a machine if one is not provided.

        :param extra: Define parameters that are specific to a provider.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            REGISTER_PROVIDER, self.get_lockable_objects(), provider_id,
            provider_type, username, password, url, tenant, default_image,
            default_flavor, extra
        )
        return self.wait_for_procedures(procedures, synchronous)

    def generate_options(self):
        """Make some options accept multiple values.
        """
        super(ProviderRegister, self).generate_options()
        options = ["extra"]
        for option in self.command_options:
            if option['dest'] in options:
                option['action'] = "append"

UNREGISTER_PROVIDER = _events.Event()
class ProviderUnregister(ProcedureCommand):
    """Unregister a provider.
    """
    group_name = "provider"
    command_name = "unregister"

    def execute(self, provider_id, synchronous=True):
        """Unregister a provider.

        :param provider_id: Provider's Id.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            UNREGISTER_PROVIDER, self.get_lockable_objects(), provider_id
        )
        return self.wait_for_procedures(procedures, synchronous)

class ProviderList(Command):
    """Return information on existing provider(s).
    """
    group_name = "provider"
    command_name = "list"

    def execute(self, provider_id=None):
        """Return information on existing provider(s).

        :param provider_id: None if one wants to list the existing providers
                            or provider's id if one wants information on a
                            provider.
        """
        rset = ResultSet(
            names=('provider_id', 'type', 'username', 'url', 'tenant',
                   'default_image', 'default_flavor', 'extra'),
            types=(str, str, str, str, str, str, str, str)
        )

        if provider_id is None:
            for prv in Provider.providers():
                rset.append_row((
                    prv.provider_id, prv.provider_type, prv.username, prv.url,
                    prv.tenant, prv.default_image, prv.default_flavor,
                    prv.extra
                ))
        else:
            prv = _retrieve_provider(provider_id)
            rset.append_row((
                prv.provider_id, prv.provider_type, prv.username, prv.url,
                prv.tenant, prv.default_image, prv.default_flavor, prv.extra
            ))

        return CommandResult(None, results=rset)

@_events.on_event(REGISTER_PROVIDER)
def _register_provider(provider_id, provider_type, username, password, url,
                       tenant, default_image, default_flavor, extra):
    """Register a provider.
    """
    _check_provider_exists(provider_id)
    _check_extra_parameter(extra)
    provider = Provider(provider_id=provider_id, provider_type=provider_type,
        username=username, password=password, url=url, tenant=tenant,
        default_image=default_image, default_flavor=default_flavor, extra=extra
    )
    Provider.add(provider)

    _LOGGER.debug("Registered provider (%s).", provider)

@_events.on_event(UNREGISTER_PROVIDER)
def _unregister_provider(provider_id):
    """Unregister a provider.
    """
    provider = _retrieve_provider(provider_id)
    _check_machines_exist(provider)
    Provider.remove(provider)

    _LOGGER.debug("Unregistered provider (%s).", provider_id)

def _retrieve_provider(provider_id):
    """Return a provider object from an id.
    """
    provider = Provider.fetch(provider_id)
    if not provider:
        raise _errors.ProviderError(
            "Provider (%s) does not exist." % (provider_id, )
        )
    return provider

def _check_provider_exists(provider_id):
    """Check whether a provider exists or not.
    """
    provider = Provider.fetch(provider_id)
    if provider:
        raise _errors.ProviderError(
            "Provider (%s) already exists." % (provider_id, )
        )

def _check_machines_exist(provider):
    """Check whether there is a machine associated to a provider or not.
    """
    for machine in Machine.machines(provider.provider_id):
        raise _errors.ProviderError(
            "There are machines associated to the provider (%s)." %
            (provider.provider_id, )
        )

def _check_extra_parameter(extra):
    """Check whether the extra parameters can be parsed.
    """
    if extra:
        if not isinstance(extra, list):
            _errors.ConfigurationError(
                "Extra parameters must be provided as a list: %s." %
                (extra, )
            )
        kv_to_dict(extra)
