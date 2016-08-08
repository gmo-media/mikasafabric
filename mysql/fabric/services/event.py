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

"""Command interface for working with events and procedures. It provides the
necessary means to trigger an event, to get details on a procedure and wait
until procedures finish their execution.
"""
import logging
import uuid as _uuid

from mysql.fabric import (
    events as _events,
    executor as _executor,
    errors as _errors,
)

from mysql.fabric.command import (
    Command,
    CommandResult,
    ResultSet,
    ProcedureCommand,
)

from mysql.fabric.utils import (
    kv_to_dict,
)

_LOGGER = logging.getLogger(__name__)

class Trigger(Command):
    """Trigger an event.
    """
    group_name = "event"
    command_name = "trigger"

    def execute(self, event, locks=None, args=None, kwargs=None):
        """Trigger the execution of an event.

        :param event: Event's identification.
        :type event: String
        :param args: Event's non-keyworded arguments.
        :param kwargs: Event's keyworded arguments.

        :return: :class:`CommandResult` instance with UUID of the
                 procedures that were triggered.
        """
        # Prepare lockable objects.
        lockable_objects = None
        if locks:
            lockable_objects = set()
            for lock in locks:
                lockable_objects.add(lock.strip())

        # Prepare list arguments.
        param_args = []
        if args is not None:
            param_args = args

        # Prepare key word arguments.
        param_kwargs = {}
        if kwargs is not None:
           param_kwargs = kv_to_dict(kwargs)

        # Define the result set format. 
        rset = ResultSet(names=['uuid'], types=[str])

        _LOGGER.debug(
            "Triggering event (%s) with arguments: %s, %s.", event,
            param_args, param_kwargs
        )

        # Trigger the event and add the UUID of all procedures queued
        # to the result.
        procedures = _events.trigger(
            event, lockable_objects, *param_args, **param_kwargs
        )
        for procedure in procedures:
            rset.append_row([str(procedure.uuid)])

        return CommandResult(None, results=rset)

    def generate_options(self):
        """Make some options accept multiple values.
        """
        super(Trigger, self).generate_options()
        options = ["locks", "args", "kwargs"]
        for option in self.command_options:
            if option['dest'] in options:
                option['action'] = "append"

class WaitForProcedures(Command):
    """Wait until procedures, which are identified through their uuid in a
    list and separated by comma, finish their execution. If a procedure is
    not found an error is returned.
    """
    group_name = "event"
    command_name = "wait_for_procedures"

    def execute(self, proc_uuids=None):
        """Wait until a set of procedures uniquely identified by their uuids
        finish their execution.

        However, before starting waiting, the function checks if the procedures
        exist. If one of the procedures is not found, the following exception
        is raised :class:`~mysql.fabric.errors.ProcedureError`.

        :param proc_uuids: Iterable with procedures' UUIDs.
        """
        if proc_uuids is None:
            raise _errors.ProcedureError(
                "Please, specify which procedure(s) you will be waiting for."
            )

        procedures = []
        for proc_uuid in proc_uuids:
            proc_uuid = _uuid.UUID(proc_uuid.strip())
            procedure = _executor.Executor().get_procedure(proc_uuid)
            if not procedure:
                raise _errors.ProcedureError(
                    "Procedure (%s) was not found." % (proc_uuid, )
                )
            procedures.append(procedure)

        command_results = CommandResult(error=None)
        for procedure in procedures:
            command_result = ProcedureCommand.wait_for_procedures(
                [procedure, ], True
            )
            if command_result.error is None:
                for result in command_result.results:
                    command_results.append_result(result)
            else:
                result = ResultSet(names=['uuid', 'error'], types=[str, str])
                result.append_row([
                    str(procedure.uuid), str(command_result.error)
                ])
                command_results.append_result(result)

        return command_results

    def generate_options(self):
        """Make some options accept multiple values.
        """
        super(WaitForProcedures, self).generate_options()
        options = ["proc_uuids"]
        for option in self.command_options:
            if option['dest'] in options:
                option['action'] = "append"
