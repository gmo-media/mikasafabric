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

"""Errors raised within the MySQL Fabric library.
"""

class Error(Exception):
    """Base exception for all errors in the package.
    """
    pass

class NotCallableError(Error):
    """Exception raised when a callable was expected but not provided.
    """
    pass

class NotEventError(Error):
    """Exception raised when a non-event instance was passed where an
    event instance was expected.
    """
    pass

class UnknownCallableError(Error):
    """Exception raised when trying to use a callable that was not
    known when a known callable was expected.
    """
    pass

class ExecutorError(Error):
    """Exception raised when the one tries to access the executor that
    is not properly configured.
    """

    pass

class InvalidGtidError(Error):
    """Exception raised when the one tries to use and make operations with
    invalid GTID(s).
    """

    pass

class InternalError(Error):
    """Exception raised when an internal error occurs.

    Typically it is raised when an extension added does not honor the
    internal interfaces.
    """

    pass

class UuidError(Error):

    """Exception raised when there are problems with uuids. For example,
    if the expected uuid does not match the server's uuid.
    """
    pass

class TimeoutError(Error):
    """Exception raised when there is a timeout.
    """

class DatabaseError(Error):
    """Exception raised when something bad happens while accessing a
    database.
    """
    def __init__(self, msg, errno=None):
        """Constructor for DatabaseError object.
        """
        super(DatabaseError, self).__init__(msg)
        self.errno = errno

class ProgrammingError(Error):
    """Exception raised when a developer tries to use the interfaces and
    executes an invalid operation.
    """
    pass

class ConfigurationError(ProgrammingError):
    """Exception raised when configuration options are not properly set.
    """
    pass

class LockManagerError(Error):
    """Exception raised when an invalid operation is attempted on the
    lock manager or locks are broken.
    """
    pass

class ServiceError(Error):
    """Exception raised when one tries to use the service interface and
    executes an invalid operation.
    """
    pass

class GroupError(ServiceError):
    """Exception raised when one tries to execute an invalid operation on a
    group. For example, it is not possible to create two groups with the
    same id or remove a group that has associated servers.
    """
    pass

class ServerError(ServiceError):
    """Exception raised when one tries to execute an invalid operation on a
    server. For example, it is not possible to create two servers with the
    same uuid.
    """
    pass

class ProcedureError(ServiceError):
    """Exception raised when a procedure is not found.
    """
    pass

class ShardingError(ServiceError):
    """Exception raised when an invalid operation is attempted on the
    sharding system.
    """
    pass

class BackupError(Error):
    """Exception raised when a error occurs in the backup restore framework
    of Fabric.
    """
    pass

class CredentialError(Error):
    """Exception raised when something is wrong with credentials"""
    pass

class CommandResultError(Error):
    """Exception raised for incorrect command result
    """
    pass

class ProviderError(ServiceError):
    """Exception raised when something is wrong while accessing a
    cloud provider.
    """
    pass

class MachineError(ServiceError):
    """Exception while processing a request that requires access to
    provider's machine.
    """
    pass
