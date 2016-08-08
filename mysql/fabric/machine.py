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
"""This is used to manage the virtual machine instances, or simply machines,
created through the cloud provider. It provides a Machine class which is
used to store machine's data into the state store and retrieve machine's
data from it.

It is not mandatory to save data on created machines into the state store.
Although optional, it is handy when machines are created using multiple
providers as the state store might be used as a central repository.
"""
import uuid as _uuid
import logging

from mysql.fabric import (
    persistence as _persistence,
)

_LOGGER = logging.getLogger(__name__)

_CREATE_INSTANCE = (
    "CREATE TABLE machines "
    "(machine_uuid VARCHAR(40) NOT NULL, "
    "provider_id VARCHAR(128) NOT NULL, "
    "av_zone VARCHAR(256), "
    "addresses TEXT, "
    "INDEX idx_machine_provider_id (provider_id)) "
    "DEFAULT CHARSET=utf8"
)

_ALTER_FOREING_KEY_PROVIDER = (
    "ALTER TABLE machines ADD CONSTRAINT fk_provider_id_providers "
    "FOREIGN KEY(provider_id) REFERENCES providers(provider_id)"
)

_QUERY_INSTANCE = (
    "SELECT machine_uuid, provider_id, av_zone, addresses "
    "FROM machines WHERE machine_uuid = %s"
)

_QUERY_INSTANCES = (
    "SELECT machine_uuid, provider_id, av_zone, addresses "
    "FROM machines WHERE provider_id = %s"
)

_INSERT_INSTANCE = (
    "INSERT INTO machines(machine_uuid, provider_id, av_zone, "
    "addresses) VALUES(%s, %s, %s, %s)"
)

_REMOVE_INSTANCE = (
    "DELETE from machines WHERE machine_uuid = %s "
)

class Machine(_persistence.Persistable):
    """Used to manage a virtual machine instance, or simply machine.

    :param uuid: machines's UUID.
    :param provider_id: Provider's Id.
    :param av_zone: Availability Zone.
    :rtype av_zone: string
    :param addresses: List of addresses associated to the machine.
    :rtype addresses: string
    """
    def __init__(self, uuid, provider_id, av_zone=None, addresses=None):
        """Constructor for the Machine.
        """
        assert uuid is not None
        assert provider_id is not None

        super(Machine, self).__init__()
        self.__uuid = uuid
        self.__provider_id = provider_id
        self.__av_zone = av_zone
        self.__addresses = addresses

    def __eq__(self, other):
        """Two machines are equal if they have the same uuid.
        """
        return isinstance(other, Machine) and self.__uuid == other.uuid

    def __hash__(self):
        """A Machine is hashable through its uuid.
        """
        return hash(self.__uuid)

    @property
    def uuid(self):
        """Return the machine's uuid.
        """
        return self.__uuid

    @property
    def provider_id(self):
        """Return the provider's id.
        """
        return self.__provider_id

    @property
    def av_zone(self):
        """Return the instace's high availability zone.
        """
        return self.__av_zone

    @property
    def addresses(self):
        """Return the addresses assigned to the machine.
        """
        return self.__addresses

    @staticmethod
    def create(persister=None):
        """Create the objects(tables) that will store machine information
        into the state store.

        :param persister: Object to access the state store.
        :raises: DatabaseError If the table already exists.
        """
        persister.exec_stmt(_CREATE_INSTANCE)

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints to the machines table.

        :param persister: Object to access the state store.
        """
        persister.exec_stmt(_ALTER_FOREING_KEY_PROVIDER)

    @staticmethod
    def fetch(uuid, persister=None):
        """Return a Machine object corresponding to the uuid.

        :param uuid: UUID of the machine's that will be returned.
        :param persister: Object to access the state store.
        :return: Machine that corresponds to the UUID or None if it
                 does not exist.
        """
        cur = persister.exec_stmt(_QUERY_INSTANCE,
            {"fetch" : False, "params":(str(uuid), )}
        )
        row = cur.fetchone()
        if row:
            return Machine.construct_from_row(row=row)

    @staticmethod
    def add(machine, persister=None):
        """Write a Machine object into the state store.

        :param machine: A reference to a Machine.
        :param persister: Object to access the state store.
        """
        persister.exec_stmt(_INSERT_INSTANCE,
            {"params": (
                str(machine.uuid),
                machine.provider_id,
                machine.av_zone,
                machine.addresses,
              )
            }
        )

    @staticmethod
    def remove(machine, persister=None):
        """Remove a Machine object from the state store.

        :param machine: A reference to a Machine.
        :param persister: Object to access the state store.
        """
        persister.exec_stmt(_REMOVE_INSTANCE,
            {"params": (str(machine.uuid), )}
        )

    @staticmethod
    def machines(provider_id, persister=None):
        """Return an iterator over a set of machines.

        :param provider_id: Provider's Id.
        :param persister: Object to access the state store.
        :return: Iterator over a set of machines if there is any.
        """
        cur = persister.exec_stmt(_QUERY_INSTANCES,
            {"fetch" : False, "params":(provider_id, )}
        )

        rows = cur.fetchall()
        for row in rows:
            yield Machine.construct_from_row(row=row)

    def as_dict(self):
        """Return the object as a dictionary.
        """
        dictionary = {
            "provider_id" : self.__provider_id,
            "uuid" : str(self.__uuid),
            "av_zone" : self.__av_zone or "",
            "addresses" : self.__addresses or ""
        }
        return dictionary

    @staticmethod
    def construct_from_row(row):
        """Create a Machine object from a row.

        :row param: Record that contains machine's data.
        """
        uuid, provider_id, av_zone, addresses = row
        try:
            uuid = _uuid.UUID(uuid)
        except ValueError:
            pass

        return Machine(
            uuid=uuid, provider_id=provider_id, av_zone=av_zone,
            addresses=addresses
        )
