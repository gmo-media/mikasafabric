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
"""This module provides the necessary interfaces for reporting errors and
failures.
"""
import logging
import uuid as _uuid

from mysql.fabric.utils import (
    get_time,
    get_time_delta,
)

from mysql.fabric import (
    events as _events,
    server as _server,
    errors as _errors,
    error_log as _error_log,
    config as _config,
)

from mysql.fabric.command import (
    ProcedureGroup,
)

from mysql.fabric.services.server import (
    _retrieve_server
)

_LOGGER = logging.getLogger(__name__)

REPORT_ERROR = _events.Event("REPORT_ERROR")
class ReportError(ProcedureGroup):
    """Report a server error.

    If there are many issues reported by different servers within a period of
    time, the server is marked as faulty. Should the server be a primary, the
    failover mechanism is triggered. Users who only want to set the server's
    status to faulty after getting enough notifications from different clients
    must set the update_only parameter to true. By default its value is false.
    """
    group_name = "threat"
    command_name = "report_error"

    _MIN_NOTIFICATIONS = 1
    _NOTIFICATIONS = _DEFAULT_NOTIFICATIONS = 300

    _MIN_NOTIFICATION_CLIENTS = 1
    _NOTIFICATION_CLIENTS = _DEFAULT_NOTIFICATION_CLIENTS = 50

    _MIN_NOTIFICATION_INTERVAL = 1
    _MAX_NOTIFICATION_INTERVAL = 3600
    _NOTIFICATION_INTERVAL = _DEFAULT_NOTIFICATION_INTERVAL = 60

    def execute(self, server_id, reporter="unknown", error="unknown",
                update_only=False, synchronous=True):
        """Report a server issue.

        :param server_id: Servers's UUID or HOST:PORT.
        :param reporter: Who has reported the issue, usually an IP address or a
                         host name.
        :param error: Error that has been reported.
        :param update_only: Only update the state store and skip provisioning.
        """
        procedures = _events.trigger(
            REPORT_ERROR, self.get_lockable_objects(), server_id, reporter,
            error, update_only
        )
        return self.wait_for_procedures(procedures, synchronous)

REPORT_FAILURE = _events.Event("REPORT_FAILURE")
class ReportFailure(ProcedureGroup):
    """Report with certantity that a server has failed or is unreachable.

    Should the server be a primary, the failover mechanism is triggered.
    Users who only want to set the server's status to faulty must set the
    update_only parameter to True. By default its value is false.
    """
    group_name = "threat"
    command_name = "report_failure"

    def execute(self, server_id, reporter="unknown", error="unknown",
                update_only=False, synchronous=True):
        """Report a server issue.

        :param server_id: Servers's UUID or HOST:PORT.
        :param reporter: Who has reported the issue, usually an IP address or a
                         host name.
        :param error: Error that has been reported.
        :param update_only: Only update the state store and skip provisioning.
        """
        procedures = _events.trigger(
            REPORT_FAILURE, self.get_lockable_objects(), server_id, reporter,
            error, update_only
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(REPORT_ERROR)
def _report_error(server_id, reporter, error, update_only):
    """Report a server error.
    """
    (now, server) = _append_error_log(server_id, reporter, error)

    interval = get_time_delta(ReportError._NOTIFICATION_INTERVAL)
    st = _error_log.ErrorLog.fetch(server, interval, now)

    if st.is_unstable(ReportError._NOTIFICATIONS,
                      ReportError._NOTIFICATION_CLIENTS):
        group = _server.Group.fetch(server.group_id)
        if group.can_set_server_faulty(server, now):
            _set_status_faulty(server, update_only)

@_events.on_event(REPORT_FAILURE)
def _report_failure(server_id, reporter, error, update_only):
    """Report a server failure.
    """
    (_, server) = _append_error_log(server_id, reporter, error)
    _set_status_faulty(server, update_only)

def _set_status_faulty(server, update_only):
    """Set server's status to fauly and trigger a failover if the server
    is a master.

    This function assumes that the SERVER_LOST event is executed before
    the FAIL_OVER event.
    """
    server.status = _server.MySQLServer.FAULTY
    _server.ConnectionManager().kill_connections(server)

    if not update_only:
        group = _server.Group.fetch(server.group_id)
        if group.master == server.uuid:
            _LOGGER.info("Master (%s) in group (%s) has "
                         "been lost.", server.uuid, group.group_id)
            _events.trigger_within_procedure("FAIL_OVER", group.group_id)

    _events.trigger_within_procedure(
        "SERVER_LOST", server.group_id, str(server.uuid)
    )

def _append_error_log(server_id, reporter, error):
    """Check whether the server exist and is not faulty and register
    error log.
    """
    server = _retrieve_server(server_id)
    now = get_time()
    _error_log.ErrorLog.add(server, now, reporter, error)

    _LOGGER.warning("Reported issue (%s) for server (%s).", error, server.uuid)

    return (now, server)

def configure(config):
    """Set configuration values.
    """
    try:
        notifications = int(config.get("failure_tracking", "notifications"))
        if notifications < ReportError._MIN_NOTIFICATIONS:
            _LOGGER.warning(
                "Notifications cannot be lower than %s.",
                ReportError._MIN_NOTIFICATIONS
            )
            notifications = ReportError._MIN_NOTIFICATIONS
        ReportError._NOTIFICATIONS = int(notifications)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass

    try:
        notification_clients = \
            int(config.get("failure_tracking", "notification_clients"))
        if notification_clients < ReportError._MIN_NOTIFICATION_CLIENTS:
            _LOGGER.warning(
                "Notification_clients cannot be lower than %s.",
                ReportError._MIN_NOTIFICATION_CLIENTS
            )
            notification_clients = ReportError._MIN_NOTIFICATION_CLIENTS
        ReportError._NOTIFICATION_CLIENTS = int(notification_clients)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass

    try:
        notification_interval = \
            int(config.get("failure_tracking", "notification_interval"))
        if notification_interval > _error_log.ErrorLog._PRUNE_TIME:
            _LOGGER.warning(
                "Notification interval cannot be greater than prune "
                "interval %s", _error_log.ErrorLog._PRUNE_TIME
            )
            notification_interval = _error_log.ErrorLog._PRUNE_TIME
        if notification_interval > ReportError._MAX_NOTIFICATION_INTERVAL:
            _LOGGER.warning(
                "Notification interval cannot be greater than %s.",
                ReportError._MAX_NOTIFICATION_INTERVAL
            )
            notification_interval = ReportError._MAX_NOTIFICATION_INTERVAL
        if notification_interval < ReportError._MIN_NOTIFICATION_INTERVAL:
            _LOGGER.warning(
                "Notification interval cannot be lower than %s.",
                ReportError._MIN_NOTIFICATION_INTERVAL
            )
            notification_interval = ReportError._MIN_NOTIFICATION_INTERVAL
        ReportError._NOTIFICATION_INTERVAL = int(notification_interval)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass
