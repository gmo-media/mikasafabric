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

import Queue
import threading
import logging
import uuid as _uuid
import traceback
import time

from weakref import WeakValueDictionary

import mysql.fabric.persistence as _persistence
import mysql.fabric.errors as _errors
import mysql.fabric.scheduler as _scheduler
import mysql.fabric.checkpoint as _checkpoint

from mysql.fabric.utils import Singleton

_LOGGER = logging.getLogger(__name__)

class Procedure(object):
    """Defines the context within which an operation is executed. Explicitly,
    an operation is a code block to be executed and is named a job.

    Any job must belong to a procedure whereas a procedure may have several
    jobs associated to it. When job is created and is about to be scheduled,
    it is added to a set of registered jobs. Upon the end of its execution,
    it is moved from the aforementioned set to a list of executed jobs.
    During the execution of a job, new jobs may be created in the context
    of the current procedure.

    A procedure is marked as finished (i.e. complete) when its last job
    finishes. Specifically, when a job finishes and there is no registered
    job created on behalf of the procedure.

    This class is mainly used to keep track of requests and to provide the
    necessary means to build a synchronous execution.

    :param uuid: Procedure uuid which can be None meaning that a new one
                 will be generated.
    :param lockable_objects: Set of objects to be locked by the concurrency
                             control mechanism.
    """
    def __init__(self, uuid=None, lockable_objects=None):
        """Create a Procedure object.
        """
        assert(uuid is None or isinstance(uuid, _uuid.UUID))
        self.__uuid = uuid or _uuid.uuid4()
        self.__lock = threading.Condition()
        self.__complete = False
        self.__result = True
        self.__registered_jobs = set()
        self.__executed_jobs = []
        self.__status = []
        self.__lockable_objects = lockable_objects
        self.__sequence = 0

        _LOGGER.debug("Created procedure (%s).", self.__uuid)

    def get_lockable_objects(self):
        """Return the objects that need to be locked before this procedure
        starts being executed.

        :return: Set of objects to be locked.
        :rtype: set
        """
        if not self.__lockable_objects:
            return set(["lock"])
        return self.__lockable_objects

    def get_priority(self):
        """Return whether this procedure should have higher priority over
        other procedures that require access to a common subset of objects.

        :return: Whether the procedure has high priority or not.
        :rtype: Boolean
        """
        return False

    def is_complete(self):
        """Return whether the procedure has finished or not.

        :return: Whether the procedure has finished or not.
        :rtype: Boolean
        """
        with self.__lock:
            return self.__complete

    def get_registered_jobs(self):
        """Return the set of jobs to be scheduled on behalf of this
        procedure.

        :return: List of jobs to be scheduled.
        :rtype: List
        """
        with self.__lock:
            return list(self.__registered_jobs)

    def get_executed_jobs(self):
        """Return the set of jobs executed on behalf of this procedure.
        """
        with self.__lock:
            return list(self.__executed_jobs)

    def add_registered_job(self, job):
        """Register that a job is about to be scheduled on behalf of the
        procedure.

        :param job: Job to be scheduled.
        """
        with self.__lock:
            assert(not self.__complete)
            assert(job not in self.__registered_jobs)
            assert(job not in self.__executed_jobs)
            assert(job.procedure == self)

            self.__registered_jobs.add(job)

    def add_executed_job(self, job):
        """Register that a job has been executed on behalf of the
        procedure.

        :param job: Executed job.
        """
        with self.__lock:
            assert(not self.__complete)
            assert(job in self.__registered_jobs)
            assert(job not in self.__executed_jobs)
            assert(job.procedure == self)

            self.__registered_jobs.remove(job)
            self.__executed_jobs.append(job)

            if job.result is not None:
                self.__result = job.result
            self.__status.extend(job.status)

            if not self.__registered_jobs:
                self.__complete = True
                self.__lock.notify_all()
                _checkpoint.Checkpoint.remove(job.checkpoint)
                _LOGGER.debug("Complete procedure (%s).", self.__uuid)

    @property
    def uuid(self):
        """Return the procedure's uuid.
        """
        return self.__uuid

    @property
    def status(self):
        """Return the procedure's status which is a list of the
        statuses of all processes executed.
        """
        with self.__lock:
            assert(self.__complete)
            return self.__status

    @property
    def result(self):
        """Return the procedure's result which is the result of the
        last process executed on behalf of the procedure.
        """
        with self.__lock:
            assert(self.__complete)
            return self.__result

    def wait(self):
        """Wait until the procedure finishes its execution.
        """
        with self.__lock:
            while not self.__complete:
                self.__lock.wait()

    def get_sequence(self):
        """Return the next available identifier/sequence for a job.

        :return: Return the next available sequence for a job.
        """
        self.__sequence = self.__sequence + 1
        return self.__sequence - 1

    def __eq__(self,  other):
        """Two procedures are equal if they have the same uuid.
        """
        return isinstance(other, Procedure) and self.__uuid == other.uuid

    def __hash__(self):
        """A procedure is hashable through its uuid.
        """
        return hash(self.__uuid)

    def __str__(self):
        """Return a description on the procedure: <Procedure object: uuid=...,
        complete=..., exec_jobs=..., sche_jobs=...>.
        """
        with self.__lock:
            ret = "<Procedure object: uuid=%s, complete=%s, exec_jobs=%s, " \
                "sche_jobs=%s>" % (self.__uuid, self.__complete,
                [str(job.uuid) for job in self.__executed_jobs],
                [str(job.uuid) for job in self.__registered_jobs])
            return ret


class Job(object):
    """Encapsulate a code block and is scheduled through the
    executor within the context of a procedure.
    """
    ERROR, SUCCESS = range(1, 3)
    EVENT_OUTCOME = [ERROR, SUCCESS]
    EVENT_OUTCOME_DESCRIPTION = {
        ERROR : "Error",
        SUCCESS : "Success"
    }

    CREATED, PROCESSING, COMPLETE = range(3, 6)
    EVENT_STATE = [CREATED, PROCESSING, COMPLETE]
    EVENT_STATE_DESCRIPTION = {
        CREATED : "Created",
        PROCESSING : "Processing",
        COMPLETE : "Complete"
    }

    def __init__(self, procedure, action, description,
                 args, kwargs, uuid=None):
        """Create a Job object.
        """
        is_recoverable = _checkpoint.Checkpoint.is_recoverable(action)

        if not callable(action):
            raise _errors.NotCallableError("Callable expected")
        elif not is_recoverable:
            # Currently we only print out a warning message. In the future,
            # we may decide to change this and raise an error.
            _LOGGER.warning(
                "(%s) is not recoverable. So after a failure Fabric may "
                "not be able to restore the system to a consistent state.",
                action
            )

        assert(uuid is None or isinstance(uuid, _uuid.UUID))
        self.__uuid = uuid or _uuid.uuid4()
        self.__action = action
        self.__args = args or []
        self.__kwargs = kwargs or {}
        self.__status = []
        self.__result = None
        self.__complete = False
        self.__procedure = procedure
        self.__is_recoverable = is_recoverable
        self.__jobs = []
        self.__procedures = []
        self.__action_fqn = action.__module__ + "." + action.__name__

        self.__checkpoint = _checkpoint.Checkpoint(
            self.__procedure.uuid, self.__procedure.get_lockable_objects(),
            self.__uuid, self.__procedure.get_sequence(), self.__action_fqn,
            args, kwargs,
        )

        self._add_status(Job.SUCCESS, Job.CREATED, description)
        self.__procedure.add_registered_job(self)

    @property
    def uuid(self):
        """Return the job's uuid.
        """
        return self.__uuid

    @property
    def procedure(self):
        """Return a reference to the procedure which the job is
        associated to.
        """
        return self.__procedure

    @property
    def status(self):
        """Return the status of the execution phases (i.e. created,
        processing, completed).

        A status has the following format::

          status = {
            "when": time,
            "state" : state,
            "success" : success,
            "description" : description,
            "diagnosis" : "" if not diagnosis else \\
                          traceback.format_exc()
          }
        """
        assert(self.__complete)
        return self.__status

    @property
    def result(self):
        """Return the job's result.
        """
        assert(self.__complete)
        return self.__result

    @property
    def checkpoint(self):
        """Return the checkpoint associated with the job.
        """
        return self.__checkpoint

    @property
    def is_recoverable(self):
        """Return whether the job is recoverable or not.
        """
        return self.__is_recoverable

    def append_jobs(self, jobs):
        """Gather jobs that shall be scheduled after the current
        job is executed.

        :param jobs: List of jobs.
        """
        assert(isinstance(jobs, list))
        self.__jobs.extend(jobs)

    def append_procedures(self, procedures):
        """Gather procedures that shall be scheduled after the current
        job is executed.

        :param procedures: List of procedures.
        """
        assert(isinstance(procedures, list))
        self.__procedures.extend(procedures)

    def _add_status(self, success, state, description, diagnosis=False):
        """Add a new status to this job.
        """
        assert(success in Job.EVENT_OUTCOME)
        assert(state in Job.EVENT_STATE)
        when = time.time()
        status = {
            "when" : when,
            "state" : state,
            "success" : success,
            "description" : description,
            "diagnosis" : "" if not diagnosis else traceback.format_exc(),
            }
        self.__status.append(status)

        _LOGGER.debug("%s job (%s, %s, %s, %s).",
            Job.EVENT_STATE_DESCRIPTION[state],
            self.__procedure.uuid, self.__uuid, self.__action_fqn,
            Job.EVENT_OUTCOME_DESCRIPTION[success]
        )

    def execute(self, persister, scheduler, queue):
        """Execute the job.

        :param executor_queue: Reference to the executor's queue.
        :param scheduler_queue: Reference to the scheduler's queue.
        """
        try:
            # Execute the job.
            self._start_context(persister)
            self.__result = self.__action(*self.__args, **self.__kwargs)

        except (_errors.ServiceError, _errors.LockManagerError) as error:
            # Report that something did not go as expected but this is
            # fine as the error is not critical.
            _LOGGER.debug(error)
            self._rollback_context(persister)

        except _errors.Error as error:
            # Report that something did not go as expected but the error
            # is somehow generated by Fabric and most likely is not critical.
            _LOGGER.debug(
                "Error in %s.", self.__action.__name__, exc_info=error
            )
            self._rollback_context(persister)

        except Exception as error:
            # Report that something did not go as excepted and that something
            # critical may have happened.
            _LOGGER.error(
                "Error in %s.", self.__action.__name__, exc_info=error
            )
            self._rollback_context(persister)

        else:
            # Everything went well so far.
            self._commit_context(persister, scheduler, queue)

    def _start_context(self, persister):
        """Start transactional context.
        """
        # Update the job status.
        message = "Executing action ({0}).".format(self.__action.__name__)
        self._add_status(Job.SUCCESS, Job.PROCESSING, message)

        # Register that the job has started the execution.
        if self.__is_recoverable:
            self.__checkpoint.begin()

        # Start the job transactional context.
        persister.begin()

    def _rollback_context(self, persister):
        """Roll back transactional context.
        """
        try:
            # Rollback the job transactional context.
            persister.rollback()

        except _errors.DatabaseError as error:
            _LOGGER.error(
                "Error in %s rolling back job's context.",
                self.__action.__name__, exc_info=error
            )

        # Update the job status.
        self.__result = False
        message = "Tried to execute action ({0}).".format(
            self.__action.__name__)
        self._add_status(Job.ERROR, Job.COMPLETE, message, True)

        # Finish context which means mark the job as finished
        # and update procedure's information.
        self._finish_context(False)

    def _commit_context(self, persister, scheduler, queue):
        """Commit transactional context.
        """
        registered_jobs = False
        try:
            # Register information on jobs created within the context of the
            # current job.
            _checkpoint.register(self.__jobs, True)

            # Register information on procedures created within the context
            # of the current job.
            for procedure in self.__procedures:
                assert(len(procedure.get_executed_jobs()) == 0)
                _checkpoint.register(procedure.get_registered_jobs(), True)

            # Register that the job has finished the execution.
            if self.__is_recoverable:
                self.__checkpoint.finish()

            registered_jobs = True

        except _errors.DatabaseError as error:
            _LOGGER.error(
                "Error in %s registering new jobs/procedures.",
                self.__action.__name__, exc_info=error
            )

        if registered_jobs:
            committed = False

            try:
                # Commit the job transactional context.
                # Currently, if the commit fails, we are not sure whether the
                # changes have succeeded or not. This is something that needs
                # to be improved in the near future.
                persister.commit()

                # Schedule jobs and procedures created within the context
                # of the current job.
                queue.schedule(self.__jobs)
                scheduler.enqueue_procedures(self.__procedures)

                committed = True

            except _errors.DatabaseError as error:
                _LOGGER.error(
                    "Error in %s committing job's context.",
                    self.__action.__name__, exc_info=error
                )

            if committed:
                # Update the job status.
                message = \
                    "Executed action ({0}).".format(self.__action.__name__)
                self._add_status(Job.SUCCESS, Job.COMPLETE, message)

                # Finish context which means mark the job as finished
                # and update procedure's information.
                self._finish_context(True)

                return

        # It was not possible to commit the current job.
        self._rollback_context(persister)

    def _finish_context(self, success):
        """Update job's outcome within the procedure's context in order
        so that the next job(s) can be scheduled.
        """
        try:
            # Mark the job as complete.
            self.__complete = True

            # Update the job status within the procedure. Currently, if this
            # fails, we are not sure whether the changes have succeeded or
            # not. This is something that needs to be improved in the near
            # future.
            self.__procedure.add_executed_job(self)

        except _errors.DatabaseError as error:
            _LOGGER.error(
                "Error in %s finishing job's context.",
                self.__action.__name__, exc_info=error
            )

    def __eq__(self,  other):
        """Two jobs are equal if they have the same uuid.
        """
        return isinstance(other, Job) and self.__uuid == other.uuid

    def __hash__(self):
        """A job is hashable through its uuid.
        """
        return hash(self.__uuid)

    def __str__(self):
        """Return a description on the job: <Job object: uuid=..., status=...>.
        """
        ret = "<Job object: uuid=%s, status=%s>" % \
            (self.__uuid, self.__status)
        return ret


class ExecutorThread(threading.Thread):
    """Class representing a executor thread which is responsible for
    executing jobs.
    """
    local_thread = threading.local()

    def __init__(self, scheduler, name):
        """Constructor for ExecutorThread.
        """
        super(ExecutorThread, self).__init__(name=name)
        self.__scheduler = scheduler
        self.__queue = ExecutorQueue()
        self.__persister = None
        self.__job = None
        self.daemon = True

    @staticmethod
    def executor_object():
        """This method returns a reference to the ExecutorThread object
        if the current thread is associated to one. Otherwise, it returns
        None.
        """
        try:
            return ExecutorThread.local_thread.executor_object
        except AttributeError:
            pass
        return None

    @property
    def current_job(self):
        """Return a reference to the current job.
        """
        assert(ExecutorThread.executor_object is not None)
        return self.__job

    def run(self):
        """Run the executor thread.

        This function will repeatedly read jobs from the scheduler and
        execute them.
        """
        _LOGGER.info("Started.")

        ExecutorThread.local_thread.executor_object = self
        self.__persister = _persistence.MySQLPersister()
        _persistence.PersistentMeta.init_thread(self.__persister)

        procedure = None
        while True:
            if procedure is None or procedure.is_complete():
                procedure = self._next_procedure(procedure)
                _LOGGER.debug("Executing procedure (%s).", procedure)
                if procedure is None:
                    break

            self.__job = self.__queue.get()
            _LOGGER.debug("Reading next job from queue, found %s.",
                          self.__job)
            self.__job.execute(self.__persister, self.__scheduler, self.__queue)
            self.__queue.done()

        _persistence.PersistentMeta.deinit_thread()

    def _next_procedure(self, prv_procedure):
        """Remove the current procedure from the scheduler and get
        the next one to be executed within a thread's context.
        """
        assert(prv_procedure is None or prv_procedure.is_complete())
        self.__scheduler.done(prv_procedure)
        procedure = self.__scheduler.next_procedure()
        if procedure is not None:
            assert(not procedure.is_complete())
            assert(len(procedure.get_executed_jobs()) == 0)
            self.__queue.schedule(procedure.get_registered_jobs())
        return procedure

class ExecutorQueue(object):
    """Queue where scheduled jobs are put.
    """
    def __init__(self):
        """Constructor for ExecutorQueue.
        """
        self.__lock = threading.Condition()
        self.__queue = Queue.Queue()

    def get(self):
        """Remove a job from the queue.

        :return: Job or None which indicates that the Executor must
                 stop.
        """
        with self.__lock:
            while True:
                try:
                    job = self.__queue.get(False)
                    self.__lock.notify_all()
                    return job
                except Queue.Empty:
                    self.__lock.wait()

    def schedule(self, jobs):
        """Atomically put a set of jobs in the queue.

        :param jobs: List of jobs to be scheduled.
        """
        assert(isinstance(jobs, list) or jobs is None)
        with self.__lock:
            for job in jobs:
                while True:
                    try:
                        self.__queue.put(job, False)
                        self.__lock.notify_all()
                        break
                    except Queue.Full:
                        self.__lock.wait()

    def done(self):
        """Mark a job as executed in the queue.
        """
        self.__queue.task_done()

class Executor(Singleton):
    """Class responsible for scheduling execution of procedures.

    Any service that uses the :class:`Executor` class to schedule procedures
    must be started after it. Before stopping the :class:`Executor`, it is
    necessary to ensure that any service that may schedule a procedure is
    stopped. Otherwise, the :class:`~mysql.fabric.errors.ExecutorError`
    exception is raised.

    Procedures to be executed are enqueued into the scheduler and concurrently
    executed if they do not conflict among each other. Two procedures conflict
    if they access a common lockable object which can be arbitrarily defined.
    See also the :class:`~mysql.fabric.scheduler.Scheduler`.

    The number of Executor's threads cannot be dynamically changed so that it
    cannot adapt to changes in the load.

    :raises: :class:`~mysql.fabric.errors.ExecutorError` if one tries to
             schedule a procedure when the Executor is stopped.
    """
    def __init__(self):
        """Constructor for the Executor.
        """
        super(Executor, self).__init__()
        self.__scheduler = _scheduler.Scheduler()
        self.__procedures_lock = threading.RLock()
        self.__procedures = WeakValueDictionary()
        self.__threads_lock = threading.RLock()
        self.__executors = []
        self.__number_executors = 1

    def set_number_executors(self, number_executors):
        """Set number of concurrent executors.
        """
        with self.__threads_lock:
            self._assert_not_running()
            self.__number_executors = number_executors

    def get_number_executors(self):
        """Get number of concurrent executors.
        """
        with self.__threads_lock:
            return self.__number_executors

    def start(self):
        """Start the executor.
        """
        with self.__threads_lock:
            self._assert_not_running()

            _LOGGER.info("Starting Executor.")

            _LOGGER.info("Setting %s executor(s).", self.__number_executors)
            for thread_number in range(0, self.__number_executors):
                thread_name = "Executor-{0}".format(thread_number)
                executor = ExecutorThread(self.__scheduler, thread_name)
                try:
                    executor.start()
                except Exception as error:
                    _LOGGER.error("Error starting thread (%s): (%s).",
                        thread_name, error
                    )
                self.__executors.append(executor)

            _LOGGER.info("Executor started.")

    def shutdown(self):
        """Shut down the executor.
        """
        _LOGGER.info("Shutting down Executor.")

        executors = None
        with self.__threads_lock:
            self._assert_running()
            executors = self.__executors
            self.__executors = []
        assert(executors is not None)

        for executor in executors:
            self.__scheduler.enqueue_procedure(None)

        for executor in executors:
            executor.join()

        _LOGGER.info("Executor has stopped.")

    def wait(self):
        """Wait until the executor shuts down.
        """
        executors = None
        with self.__threads_lock:
            executors = self.__executors

        if executors:
            for executor in executors:
                executor.join()

    def enqueue_procedure(self, within_procedure, do_action, description,
                          lockable_objects=None, *args, **kwargs):
        """Schedule a procedure.

        :within_procedure: Define if a new procedure will be created or not.
        :param action: Callable to execute.
        :param description: Description of the job.
        :param lockable_objects: Set of objects to be locked by the concurrency
                                 control mechanism.
        :param args: Non-keyworded arguments to pass to the job.
        :param kwargs: Keyworded arguments to pass to the job.
        :return: Reference to the procedure.
        :rtype: Procedure
        """
        procedures = self.enqueue_procedures(within_procedure,
            [{"action" : (do_action, description, args, kwargs),
              "job" : None
            }], lockable_objects
        )
        return procedures[0]

    def enqueue_procedures(self, within_procedure, actions,
                           lockable_objects=None):
        """Schedule a set of procedures.

        :within_procedure: Define if a new procedure will be created or not.
        :param actions: Set of actions to be scheduled and each action
                        corresponds to a procedure.
        :type actions: Dictionary [{"job" : Job uuid, "action" :
                       (action, description, non-keyword arguments,
                       keyword arguments)}, ...]
        :param lockable_objects: Set of objects to be locked by the concurrency
                                 control mechanism.
        :return: Return a set of procedure objects.
        """
        if not len(actions):
            return []

        with self.__threads_lock:
            self._assert_running()

            return self._do_enqueue_procedures(
                within_procedure, actions, lockable_objects
            )

    def _do_enqueue_procedures(self, within_procedure, actions,
                               lockable_objects):
        """Schedule a set of procedures.
        """
        procedures = None
        executor = ExecutorThread.executor_object()
        if not executor:
            if within_procedure:
                raise _errors.ProgrammingError(
                    "One can only create a new job from a job."
                )
            procedures, jobs = self._create_jobs(actions, lockable_objects)
            assert(len(set(procedures)) == len(set(jobs)))
            # There is no need to catch exceptions at this point. They will
            # be automatically caught by the caller which is usually the
            # XML-RPC session thread.
            _checkpoint.register(jobs, False)
            self.__scheduler.enqueue_procedures(procedures)
        else:
            current_job = executor.current_job
            current_procedure = current_job.procedure
            if within_procedure:
                procedures, jobs = self._create_jobs(
                    actions, lockable_objects, current_procedure.uuid
                )
                assert(set([job.procedure for job in jobs]) ==
                       set(procedures) == set([current_procedure])
                )
                current_job.append_jobs(jobs)
            else:
                procedures, jobs = self._create_jobs(actions, lockable_objects)
                assert(len(set(procedures)) == len(set(jobs)))
                current_job.append_procedures(procedures)
        assert(procedures is not None)
        return procedures

    def reschedule_procedure(self, proc_uuid, actions, lockable_objects=None):
        """Recovers a procedure after a failure by rescheduling it.

        :param proc_uuid: Procedure uuid.
        :param actions: Set of actions to be scheduled on behalf of
                        the procedure.
        :type actions: Dictionary [{"job" : Job uuid, "action" :
                       (action, description, non-keyword arguments,
                       keyword arguments)}, ...]
        :param lockable_objects: Set of objects to be locked by the concurrency
                                 control mechanism.
        :return: Return a procedure object.
        """
        if not len(actions):
            return []

        with self.__threads_lock:
            self._assert_running()

            return self._do_reschedule_procedure(
                proc_uuid, actions, lockable_objects
            )

    def _do_reschedule_procedure(self, proc_uuid, actions, lockable_objects):
        """Recovers a procedure after a failure by rescheduling it.
        """
        if ExecutorThread.executor_object():
            raise _errors.ProgrammingError(
                "One cannot reschedule a procedure from a job."
                )

        procedures, jobs = self._create_jobs(
            actions, lockable_objects, proc_uuid
        )
        self.__scheduler.enqueue_procedures(procedures)
        assert(set([job.procedure for job in jobs]) == set(procedures))
        assert(set([job.procedure.uuid for job in jobs]) ==
               set([procedure.uuid for procedure in procedures])
        )
        assert(procedures is not None)
        return procedures

    def remove_procedure(self, proc_uuid):
        """Although references are store into a WeakValueDictionary, this
        method forces its removal.
        """
        try:
            assert(isinstance(proc_uuid, _uuid.UUID))
            with self.__procedures_lock:
                procedure = self.__procedures[proc_uuid]
                assert(procedure.is_complete())
                del self.__procedures[proc_uuid]
        except (KeyError, ValueError):
            pass

    def get_procedure(self, proc_uuid):
        """Retrieve a reference to a procedure.
        """
        _LOGGER.debug("Checking procedure (%s).", proc_uuid)
        try:
            assert(isinstance(proc_uuid, _uuid.UUID))
            with self.__procedures_lock:
                procedure = self.__procedures[proc_uuid]
        except (KeyError, ValueError):
            procedure = None

        return procedure

    def wait_for_procedure(self, procedure):
        """Wait until the procedure finishes the execution of all
        its jobs.
        """
        if ExecutorThread.executor_object():
            raise _errors.ProgrammingError(
                "One cannot wait for the execution of a procedure from "
                "a job."
                )

        procedure.wait()

    def _assert_running(self):
        """Verify that the executor and by consequence the executors are
        running.
        """
        if not self.__executors:
            raise _errors.ExecutorError("Executor is not running.")

    def _assert_not_running(self):
        """Verify that the executor and by consequence the executors are
        not running.
        """
        if self.__executors:
            raise _errors.ExecutorError("Executor is already running.")

    def _create_jobs(self, actions, lockable_objects, proc_uuid=None):
        """Create a set of jobs.
        """
        procedures = set()
        jobs = []
        for number in range(0, len(actions)):
            job = self._create_job(
                actions[number], lockable_objects, proc_uuid
            )
            jobs.append(job)
            procedures.add(job.procedure)
        return list(procedures), jobs

    def _create_job(self, action, lockable_objects, proc_uuid=None):
        """Create a job.
        """
        procedure = None
        with self.__procedures_lock:
            procedure = self.__procedures.get(proc_uuid, None)
            if procedure is None:
                procedure = Procedure(proc_uuid, lockable_objects)
                self.__procedures[procedure.uuid] = procedure

        assert(procedure is not None)
        do_action, description, args, kwargs = action["action"]
        job_uuid = action["job"]
        return Job(procedure, do_action, description, args, kwargs, job_uuid)
