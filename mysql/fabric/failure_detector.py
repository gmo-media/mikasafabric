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
"""This modules contains a simple failure detector which is used by Fabric
to monitor the availability of servers within groups.

If a master cannot be accessed through the
:meth:`~mysql.fabric.server.MySQLServer.is_alive` method after `n` consecutive
attempts, the failure detector considers that it has failed and proceeds with
the election of a new master. The failure detector does not choose any new
master but only triggers the :const:`~mysql.fabric.events.REPORT_FAILURE` event
which responsible for doing so.

If a slave cannot be accessed either the same event is triggered but in this
case the server is only marked as faulty.

See :meth:`~mysql.fabric.server.MySQLServer.is_alive`.
See :class:`~mysql.fabric.services.highavailability.PromoteMaster`.
See :class:`~mysql.fabric.services.servers.ReportFailure`.
"""
import threading
import time
import logging

from mysql.fabric import (
    errors as _errors,
    persistence as _persistence,
    config as _config,
    executor as _executor,
)

from mysql.fabric.events import (
    trigger,
)

from mysql.fabric.utils import (
    get_time,
)

_LOGGER = logging.getLogger(__name__)

class FailureDetector(object):
    """Responsible for periodically checking if a set of servers within a
    group is alive.

    It does so by connecting to these servers and executing a query (i.e.
    :meth:`~mysql.fabric.server.MySQLServer.is_alive`.
    """
    LOCK = threading.Condition()
    GROUPS = {}

    _MIN_DETECTION_INTERVAL = 2.0
    _DETECTION_INTERVAL = _DEFAULT_DETECTION_INTERVAL = 5.0

    _MIN_DETECTIONS = 1
    _DETECTIONS = _DEFAULT_DETECTIONS = 2

    _MIN_DETECTION_TIMEOUT = 1
    _DETECTION_TIMEOUT = _DEFAULT_DETECTION_TIMEOUT = 1

    @staticmethod
    def register_groups():
        """Upon startup initializes a failure detector for each group.
        """
        from mysql.fabric.server import Group
        _LOGGER.info("Starting failure detector.")
        for row in Group.groups_by_status(Group.ACTIVE):
            FailureDetector.register_group(row[0])

    @staticmethod
    def register_group(group_id):
        """Start a failure detector for a group.

        :param group_id: Group's id.
        """
        _LOGGER.info("Monitoring group (%s).", group_id)
        with FailureDetector.LOCK:
            if group_id not in FailureDetector.GROUPS:
                detector = FailureDetector(group_id)
                detector.start()
                FailureDetector.GROUPS[group_id] = detector

    @staticmethod
    def unregister_group(group_id):
        """Stop a failure detector for a group.

        :param group_id: Group's id.
        """
        _LOGGER.info("Stop monitoring group (%s).", group_id)
        with FailureDetector.LOCK:
            if group_id in FailureDetector.GROUPS:
                detector = FailureDetector.GROUPS[group_id]
                detector.shutdown()
                del FailureDetector.GROUPS[group_id]

    @staticmethod
    def unregister_groups():
        """Upon shutdown stop all failure detectors that are running.
        """
        _LOGGER.info("Stopping failure detector.")
        with FailureDetector.LOCK:
            for detector in FailureDetector.GROUPS.values():
                detector.shutdown()
            FailureDetector.GROUPS = {}

    def __init__(self, group_id):
        """Constructor for FailureDetector.
        """
        self.__group_id = group_id
        self.__thread = None
        self.__check = False

    def start(self):
        """Start the failure detector.
        """
        self.__check = True
        self.__thread = threading.Thread(target=self._run,
            name="FailureDetector(" + self.__group_id + ")")
        self.__thread.daemon = True
        self.__thread.start()

    def shutdown(self):
        """Stop the failure detector.
        """
        self.__check = False

    def _run(self):
        """Function that verifies servers' availabilities.
        """
        from mysql.fabric.server import (
            Group,
            MySQLServer,
            ConnectionManager,
        )

        ignored_status = [MySQLServer.FAULTY]
        quarantine = {}
        interval = FailureDetector._DETECTION_INTERVAL
        detections = FailureDetector._DETECTIONS
        detection_timeout = FailureDetector._DETECTION_TIMEOUT
        connection_manager = ConnectionManager()

        _persistence.init_thread()

        while self.__check:
            try:
                unreachable = set()
                group = Group.fetch(self.__group_id)
                if group is not None:
                    for server in group.servers():
                        if server.status in ignored_status or \
                            MySQLServer.is_alive(server, detection_timeout):
                            if server.status == MySQLServer.FAULTY:
                                connection_manager.kill_connections(server)
                            continue

                        unreachable.add(server.uuid)

                        _LOGGER.warning(
                            "Server (%s) in group (%s) is unreachable.",
                            server.uuid, self.__group_id
                        )

                        unstable = False
                        failed_attempts = 0
                        if server.uuid not in quarantine:
                            quarantine[server.uuid] = failed_attempts = 1
                        else:
                            failed_attempts = quarantine[server.uuid] + 1
                            quarantine[server.uuid] = failed_attempts
                        if failed_attempts >= detections:
                            unstable = True

                        can_set_faulty = group.can_set_server_faulty(
                            server, get_time()
                        )
                        if unstable and can_set_faulty:
                            # We have to make this transactional and make the
                            # failover (i.e. report failure) robust to failures.
                            # Otherwise, a master might be set to faulty and
                            # a new one never promoted.
                            server.status = MySQLServer.FAULTY
                            connection_manager.kill_connections(server)
                            
                            procedures = trigger("REPORT_FAILURE", None,
                                str(server.uuid),
                                threading.current_thread().name,
                                MySQLServer.FAULTY, False
                            )
                            executor = _executor.Executor()
                            for procedure in procedures:
                                executor.wait_for_procedure(procedure)

                for uuid in quarantine.keys():
                    if uuid not in unreachable:
                        del quarantine[uuid]

            except (_errors.ExecutorError, _errors.DatabaseError):
                pass
            except Exception as error:
                _LOGGER.exception(error)

            time.sleep(interval / detections)

        _persistence.deinit_thread()


def configure(config):
    """Set configuration values.
    """
    try:
        detection_interval = \
            float(config.get("failure_tracking", "detection_interval"))
        if detection_interval < FailureDetector._MIN_DETECTION_INTERVAL:
            _LOGGER.warning(
                "Detection interval cannot be lower than %s.",
                FailureDetector._MIN_DETECTION_INTERVAL
            )
            detection_interval = FailureDetector._MIN_DETECTION_INTERVAL
        FailureDetector._DETECTION_INTERVAL = float(detection_interval)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass

    try:
        detections = int(config.get("failure_tracking", "detections"))
        if detections < FailureDetector._MIN_DETECTIONS:
            _LOGGER.warning(
                "Detections cannot be lower than %s.",
                FailureDetector._MIN_DETECTIONS
            )
            detections = FailureDetector._MIN_DETECTIONS
        FailureDetector._DETECTIONS = int(detections)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass

    try:
        detection_timeout = \
            int(config.get("failure_tracking", "detection_timeout"))
        if detection_timeout < FailureDetector._MIN_DETECTION_TIMEOUT:
            _LOGGER.warning(
                "Detection timeout cannot be lower than %s.",
                FailureDetector._MIN_DETECTION_TIMEOUT
            )
            detection_interval = FailureDetector._MIN_DETECTION_TIMEOUT
        FailureDetector._DETECTION_TIMEOUT = int(detection_timeout)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        pass
