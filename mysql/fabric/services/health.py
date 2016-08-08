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
"""Responsible for checking servers' health in a group.
"""
import logging

from  mysql.fabric import (
    server as _server,
    replication as _replication,
    errors as _errors,
)

from mysql.fabric.command import (
    Command,
    CommandResult,
    ResultSet,
)

_LOGGER = logging.getLogger(__name__)

class CheckHealth(Command):
    """Check if any server within a group has failed and report health
    information.

    It returns a dictionary where keys are the servers' uuids and the
    values are dictionaries which have the following keys:

    * is_alive - whether it is possible to access the server or not.
    * status - PRIMARY, SECONDARY, SPARE or FAULTY.
    * threads - Information on the replication threads.
    """
    group_name = "group"
    command_name = "health"

    def execute(self, group_id):
        """Check if any server within a group has failed.

        :param group_id: Group's id.
        """

        group = _server.Group.fetch(group_id)
        if not group:
            raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

        info = ResultSet(
            names=[
                'uuid', 'is_alive', 'status',
                'is_not_running', 'is_not_configured', 'io_not_running',
                'sql_not_running', 'io_error', 'sql_error'
            ],
            types=[str, bool, str] + [bool] * 4 + [str, str]
        )
        issues = ResultSet(names=['issue'], types=[str])

        for server in group.servers():
            alive = False
            is_master = (group.master == server.uuid)
            status = server.status
            why_slave_issues = {}
            # These are used when server is not contactable.
            why_slave_issues = {
                'is_not_running': False,
                'is_not_configured': False,
                'io_not_running': False,
                'sql_not_running': False,
                'io_error': False,
                'sql_error': False,
            }
            try:
                # TODO: CHECK WHETHER WE SHOULD USE IS_ALIVE OR NOT.
                server.connect()
                alive = True
                if not is_master:
                    slave_issues, why_slave_issues = \
                        _replication.check_slave_issues(server)
                    str_master_uuid = _replication.slave_has_master(server)
                    if (group.master is None or str(group.master) != \
                        str_master_uuid) and not slave_issues:
                        issues.append_row([
                            "Group has master (%s) but server is connected " \
                            "to master (%s)." % \
                            (group.master, str_master_uuid)
                        ])
            except _errors.DatabaseError:
                status = _server.MySQLServer.FAULTY
            info.append_row([
                server.uuid,
                alive,
                status,
                why_slave_issues['is_not_running'],
                why_slave_issues['is_not_configured'],
                why_slave_issues['io_not_running'],
                why_slave_issues['sql_not_running'],
                why_slave_issues['io_error'],
                why_slave_issues['sql_error'],
            ])

        return CommandResult(None, results=[info, issues])
