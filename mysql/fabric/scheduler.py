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

"""This is the scheduler which is used to guarantee that conflicting procedures
cannot be concurrently executed. Locks are atomically acquired through the
LockManager class in one single step thus avoiding deadlock issues.
"""
import threading
import logging
import Queue

import mysql.fabric.errors as _errors
import mysql.fabric.utils as _utils

_LOGGER = logging.getLogger(__name__)

class Scheduler(object):
    """Class responsible for scheduling procedures.
    """
    def __init__(self):
        """Constructor for Scheduler.
        """
        self.__lock_manager = LockManager()
        self.__queue = Queue.Queue()

    @property
    def lock_manager(self):
        """Return a reference to the LockManager instance.
        """
        return self.__lock_manager

    def enqueue_procedures(self, procedures):
        """Enqueue a list of procedures.

        :param procedures: Reference to a list of procedures.
        """
        for procedure in procedures:
            self.enqueue_procedure(procedure)

    def enqueue_procedure(self, procedure):
        """Enqueue a procedure.

        :param procedure: Reference to a procedure.
        """
        if procedure:
            _LOGGER.debug("Enqueued procedure (%s).", procedure.uuid)
        self.__queue.put(procedure)

    def next_procedure(self, condition=None):
        """Get the next procedure to be executed.

        :param condition: Condition variable which is used to notify the
                          caller when the locks are acquired or another
                          thread breaks its locks.
        :return: Return a reference to a procedure that is ready to be
                 executed.
        :rtype: Procedure
        """
        procedure = self.__queue.get()
        if procedure is not None:
            _LOGGER.debug("Locking procedure (%s).", procedure.uuid)
            self.__lock_manager.lock(
                procedure,
                procedure.get_lockable_objects(),
                procedure.get_priority(),
                condition
            )
            _LOGGER.debug("Locked procedure (%s).", procedure.uuid)
        return procedure

    def done(self, procedure):
        """Inform that a procedure has been executed and unlock
        it.

        :param procedure: Reference to a procedure.
        """
        if procedure is not None:
            self.__lock_manager.release(procedure)
            self.__queue.task_done()
            _LOGGER.debug("Unlocked procedure (%s).", procedure.uuid)


class LockManager(object):
    """Class that implements a lock system.
    """
    def __init__(self):
        """Constructor for the LockSystem.
        """
        # Latch to prevent race conditions.
        self.__lock = threading.Condition()

        # Dictionary that works as a set of queues and maps objects to
        # threads that acquired their locks or are willing to acquire
        # them.
        self.__objects = {}

        # Dictionary that maps a procedure to a 3-tuple that contains
        # the objects that the procedure needs to lock, the thread's id
        # of the caller which acquired the locks and a condition
        # variable if there is any.
        self.__procedures = {}

        # List with procedures that acquire all the necessary locks and
        # can be executed.
        self.__free = []

    @property
    def objects(self):
        """Return a dictionary mapping all locked objects to the procedures
        that grabbed the lock and those that are waiting to do so::

            {obj_1 : [proc_1, proc_2], obj_2 : [proc_1], ...}

        :return: Dictionary with information on lock requests.
        :rtype: Dictionary
        """
        with self.__lock:
            return self.__objects.copy()

    @property
    def procedures(self):
        """Return a dictionary mapping all procedures to the objects that
        they are willing to lock, the threads that requested them and a
        lock object which can be used to notify the caller either when
        the locks are acquired or when they are broken by another thread::

            {proc_1 : ([obj_1, obj_2], thread_1, lock_1),
             proc_2 : ([obj_1], thread_2, lock_2),
             ...
            }

        :return: Dictionary with information on procedures.
        :rtype: Dictionary
        """
        with self.__lock:
            return self.__procedures.copy()

    @property
    def free(self):
        """Return a list with all procedures that can proceed with their
        execution::

            [proc_1]

        :return: A list with all procedures that can proceed with their
                 execution.
        :rtype: List
        """
        with self.__lock:
            return list(self.__free)

    def lock(self, procedure, objects, force=False, condition=None):
        """Request locks for a procedure and wait until they are acquired.
        If the force flag is set, all procedures that conflict with the
        current request will be aborted.

        :param procedure: Reference to procedure.
        :param objects: Set of objects that will be locked.
        :param force: Whether the request has priority over requests
                         previously processed or not.
        :param condition: Condition variable which is used to notify the
                          caller when the locks are acquired or another
                          thread breaks its locks.
        :return: Reference to the incomming procedure.
        """
        try:
            self.__lock.acquire()
            _LOGGER.debug(
                "LockManager - Enqueuing request for procedure (%s) "
                "on objects(%s).", procedure, objects
            )
            if force:
                self._break_conflicts(objects)
            self._enqueue(procedure, objects)
            self._set_notification(procedure, condition)
            return self._wait_for_procedure(procedure, condition)
        finally:
            try:
                self.__lock.release()
            except RuntimeError:
                pass

    def release(self, procedure):
        """Dequeue a procedure's request.

        :param procedure: Reference to procedure.
        """
        with self.__lock:
            _LOGGER.debug(
                "LockManager - Dequeuing request for procedure (%s).",
                procedure
            )
            self._dequeue(procedure)

    def enqueue(self, procedure, objects, force=False):
        """Enqueue a procedure's request. If the force flag is set,
        all procedures that conflict with the current request will be
        aborted.

        :param procedure: Reference to procedure.
        :param objects: Set of objects that will be locked.
        :param force: Whether the request has priority over requests
                      previously processed or not.
        """
        with self.__lock:
            _LOGGER.debug(
                "LockManager - Enqueuing request for procedure (%s) on "
                "objects(%s).", procedure, objects
            )
            if force:
                self._break_conflicts(objects)
            self._enqueue(procedure, objects)

    def get(self, procedure, condition=None):
        """Wait until the procedure acquires all the necessary locks.

        :param procedure: Reference to procedure.
        :param condition: Condition variable which is used to notify the
                          caller when the locks are acquired or another
                          thread breaks its locks.
        :return: Reference to the requested procedure.
        """
        try:
            self.__lock.acquire()
            self._procedure_enqueued(procedure)
            self._set_notification(procedure, condition)
            return self._wait_for_procedure(procedure, condition)
        finally:
            try:
                self.__lock.release()
            except RuntimeError:
                pass

    def break_conflicts(self, objects):
        """Make all procedures, which are holding or waiting for locks
        on a list of objects given as parameter, abort and raise an
        exception within the context of the thread that acquqired the
        locks.

        :param objects: Set of objects that may have associated requests.
        """
        with self.__lock:
            procedures = self._break_conflicts(objects)
            _LOGGER.debug(
                "The following procedures were aborted: (%s).", procedures
            )
            return procedures

    def check_conflicts(self, objects):
        """Return the set of procedures that are holding locks or waiting
        for locks on a list of objects given as parameter.

        :param objects: Set of objects that may have associated requests.
        """
        with self.__lock:
            procedures = self._check_conflicts(objects)
            _LOGGER.debug(
                "The procedures (%s) have locked or are willing to lock "
                "one of the objects in (%s).", procedures, objects
            )
            return procedures

    def _set_notification(self, procedure, condition):
        """Update information that can be used to notify threads on
        possible changes to the lock information.
        """
        assert(condition is None or isinstance(condition, threading.Condition))
        objects, _, _ = self.__procedures[procedure]
        thread = threading.current_thread()
        self.__procedures[procedure] = (objects, thread, condition)

    def _dequeue(self, procedure):
        """Dequeue a procedure's request.
        """
        # Dictionary that maps procedures to objects that it already
        # has acquired a lock on.
        head_procedures = set()
        objects, _, _ = self._procedure_enqueued(procedure)

        # Remove the information on the procedure from the procedures'
        # dictionary and from the free list if it is there.
        del self.__procedures[procedure]
        try:
            self.__free.remove(procedure)
        except ValueError:
            pass

        # Remove the information on the procedure from the objects'
        # dictionary.
        _LOGGER.debug("Released procedure - %s objects %s.", procedure, objects)
        for obj in objects:
            # The wait_queue contains the list of procedures willing
            # to lock the object.
            wait_queue = self.__objects[obj]
            wait_queue.remove(procedure)
            if len(wait_queue) == 0:
                # If the queue is empty, remove the object from the
                # dictionary.
                del self.__objects[obj]
            elif wait_queue[0] not in self.__free:
                # If a procedure is at the head of the queue it has a lock
                # on the object.
                head_procedures.add(wait_queue[0])

        _LOGGER.debug("Possible affected procedures %s.", head_procedures)
        for procedure in head_procedures:
            objects, _, _ = self.__procedures[procedure]
            procedures = set([self.__objects[obj][0] for obj in objects])
            assert(len(procedures) > 0)
            if procedures == set([procedure]):
                _LOGGER.debug("Procedure %s is ready to be executed.", procedure)
                # If the requested set of objects is equal to the acquired
                # set of objects, the procedure is ready to go.
                self.__free.append(procedure)
                self.__lock.notify_all()

    def _enqueue(self, procedure, objects):
        """Enqueue a procedure's request.
        """
        assert(isinstance(objects, set))

        if objects != set(['lock']) :
            _LOGGER.warning(
                "Lock object is not the usual set(['lock']) in %s.", procedure
            )

        try:
            # Verifying if a procedure is not already enqueued.
            objects, _, _ = self.__procedures[procedure]
            raise _errors.LockManagerError(
                "The procedure (%s) is already enqueued." % (procedure, )
            )
        except KeyError:
            pass

        ready = True
        for obj in objects:
            queue = self.__objects.get(obj, [])
            if len(queue) != 0:
                ready = False
            queue.append(procedure)
            self.__objects[obj] = queue

        self.__procedures[procedure] = (objects, None, None)

        if ready:
            self.__free.append(procedure)
            self.__lock.notify_all()

    def _check_conflicts(self, objects):
        """Return the set of procedures that are holding locks or waiting
        for locks on a list of objects given as parameter.
        """
        assert(isinstance(objects, set))
        procedures = [self.__objects.get(obj, []) for obj in objects]
        return list(set([proc for lst_proc in procedures for proc in lst_proc]))

    def _break_conflicts(self, objects):
        """Make all procedures, which are holding or waiting for locks
        on a list of objects given as parameter, abort and raise an
        exception within the context of the thread that acquqired the
        locks.
        """
        assert(isinstance(objects, set))
        procedures = self._check_conflicts(objects)
        for procedure in procedures:
            requested_objects, thread, condition = self.__procedures[procedure]
            assert(len(requested_objects & objects) != 0)
            self._dequeue(procedure)
            if thread and threading.current_thread() != thread:
                try:
                    _utils.async_raise(thread.ident, _errors.LockManagerError)
                except (ValueError, SystemError):
                    _LOGGER.debug(
                        "Error trying to notify thread (%s) that holds locks "
                        "for procedure (%s).", thread, procedure
                    )
            if condition:
                with condition:
                    condition.notify_all()
        return procedures

    def _wait_for_procedure(self, procedure, condition):
        """Wait until procedure acquires all locks requested.
        """
        not_free = True
        while not_free:
            not_free = (procedure not in self.__free)
            if not_free and condition:
                with condition:
                    self.__lock.release()
                    condition.wait()
                    self.__lock.acquire()
            elif not_free:
                self.__lock.wait()
        assert(not_free == False)
        assert(procedure in self.__free)
        return procedure

    def _procedure_enqueued(self, procedure):
        """Return information on the procedure if it is enqueued.
        Otherwise, raise an exception.
        """
        try:
            return self.__procedures[procedure]
        except KeyError:
            raise _errors.LockManagerError(
                "The procedure (%s) was never enqueued." % (procedure, )
            )
