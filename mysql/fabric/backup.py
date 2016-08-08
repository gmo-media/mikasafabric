#
# Copyright (c) 2013,2015, Oracle and/or its affiliates. All rights reserved.
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

"""Module contains the abstract classes for implementing backup and
restoring a server from a backup. The module also contains the concrete
class for implementing the concrete class for doing backup using
mysqldump.
"""
import logging
import shlex

import os
import sys
import uuid
import glob

from abc import ABCMeta
from subprocess import (
    Popen,
    PIPE,
)
from urlparse import urlparse

from mysql.fabric import (
    errors as _errors,
)
from mysql.fabric.server_utils import (
    split_host_port
)
from mysql.fabric.server import MySQLServer

_LOGGER = logging.getLogger(__name__)

def run_mysql_client(command, passwd, outstream=None):
    """Run a MySQL client program and securely supply the password.
    :param command:   List of program path and arguments.
    :param passwd:    Password to pass to the program.
    :param outstream: The program's stdout can be redirected into this stream.
    :return:          A tuple (returncode, the ouptput from stderr).
    :raises:          BackupError if client does not prompt for the password.
    """

    #
    # MySQL client programs do normally try to read the password from the
    # controlling tty (Unix) or the console (Windows).
    #
    # On Unix, if the process does not have a controlling tty, then the
    # program prompts for the password on stderr, and reads the password
    # from stdin. The trick here is to start the MySQL client program
    # with no controlling tty. This can be done by running the program
    # in a new "session". This can be done by running the os.setsid()
    # operating system service in the new process before the program is
    # executed (via the preexec_fn parameter). os.setsid() starts a new
    # "session" without a controlling tty.
    #
    # On Windows, there is no way to avoid the console prompt, if no
    # password is specified. The trick here is to supply the password
    # through a temporarily created defaults file.
    #
    is_windows = sys.platform.startswith('win')
    if is_windows:
        return run_mysql_client_windows(command, passwd, outstream)
    else:
        return run_mysql_client_unix(command, passwd, outstream)

def create_defaults_file_name(var_part):
    """Create a file name for a temporary defaults file.
    This can also be used to create a glob pattern for cleanup.
    In this case, var_part can be "*".

    :param var_part: The variable part of the file name, e.g. uuid.uuid4().
                     The string representation of var_part must consist of
                     characters, that are valid for file names.
    """
    # The file name should not be too long, but the fixed parts of the name
    # shall be as unique as possible, to avoid, that a clean up removes
    # wrong files.
    return "mysql_fabric_" + str(var_part) + "_my.cnf"

def cleanup_temp_defaults_files():
    """Clean up temporary defaults files.
    This is meant to be used at Fabric start to get rid of defaults files,
    which might have been left over from a crash.
    """
    for file_name in glob.glob(create_defaults_file_name("*")):
        _LOGGER.debug("Removing temporary defaults file: " + file_name)
        erase_temp_defaults_file(file_name)

def erase_temp_defaults_file(filename):
    """Erase temporary defaults file securely.
    Open the file in read+write mode, do not create it, do not
    truncate it. The file descriptor will point at file begin.
    Overwrite 4096 characters. Then delete the file.
    This assumes, that the temporary defaults files are small.
    The overwrite is done to protect a potential undelete of the file.
    """
    fdesc = -1
    try:
        fdesc = os.open(filename, os.O_RDWR)
        os.write(fdesc, " " * 4096)
        os.close(fdesc)
    except: # pylint: disable=W0702
        try:
            if fdesc != -1:
                os.close(fdesc)
        except: # pylint: disable=W0702
            pass
    try:
        os.remove(filename)
    except: # pylint: disable=W0702
        pass

def run_mysql_client_windows(command, passwd, outstream=None):
    """Run a MySQL client program and supply the password over a file.
    :param command:   List of program path and arguments.
    :param passwd:    Password to pass to the program.
    :param outstream: The program's stdout can be redirected into this stream.
    :return:          A tuple (returncode, the ouptput from stderr).
    """

    #
    # MySQL client programs do normally try to read the password from the
    # controlling tty (Unix) or the console (Windows).
    #
    # On Windows, there is no way to avoid the console prompt, if no
    # password is specified. The trick here is to supply the password
    # through a temporarily created defaults file.
    #
    filename = create_defaults_file_name(uuid.uuid4())

    #
    # Insert a --defaults-file option as the first option.
    #
    command[1:1] = ["--defaults-file=" + filename]
    _LOGGER.debug("Running MySQL client program: " + " ".join(command))

    try:
        #
        # Allow to decide, if the file is open.
        #
        fdesc = -1

        #
        # Open the file only if it does not exist (O_CREAT | O_EXCL).
        # Reduce accessibility to the file as much as possible (0600).
        #
        fdesc = os.open(filename, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0600)

        #
        # Write a client section with the password into the file.
        #
        os.write(fdesc, "[client]\npassword=" + passwd + "\n")

        #
        # Fill the file up to 4096 characters to avoid conclusions on the
        # password length in case someone can stat the file.
        #
        length = os.lseek(fdesc, 0, os.SEEK_CUR)
        if length < 4096:
            length = 4096 - length
            os.write(fdesc, " " * length)

        #
        # Close the file
        #
        os.close(fdesc)
        fdesc = -1

        #
        # Start the client
        #
        process = Popen(command, stdin=None, stdout=outstream, stderr=PIPE)

        #
        # Read stderr until EOF. stdout is either outstream or tty/console.
        # No output lines will be received here.
        #
        dummy_output_lines, error_lines = process.communicate()
        returncode = process.returncode

    except OSError as err:
        _LOGGER.error("Cannot create defaults file '%s': %d, %s",
                      filename, err.errno, err.strerror)
        returncode = err.errno
        error_lines = err.strerror

    finally:

        #
        # Try to close the file, if it could still be open.
        #
        try:
            if fdesc != -1:
                os.close(fdesc)
        except: # pylint: disable=W0702
            pass

        erase_temp_defaults_file(filename)

    #
    # Return the returncode and the output from the program's stderr.
    #
    _LOGGER.debug("MySQL client program returned: %d\n%s",
                  returncode, error_lines)
    return (returncode, error_lines)

def run_mysql_client_unix(command, passwd, outstream=None):
    """Run a MySQL client program and supply the password over a pipe.
    :param command:   List of program path and arguments.
    :param passwd:    Password to send to stdin of the program.
    :param outstream: The program's stdout can be redirected into this stream.
    :return:          A tuple (returncode, the ouptput from stderr).
    :raises:          BackupError if client does not prompt for the password.
    """

    #
    # Insert a --no-defaults option as the first option to avoid stray
    # effects, if the user has some special options for the particular
    # client. Also append the -p option to let it prompt for a password.
    #
    command[1:1] = ["--no-defaults"]
    command.extend(["-p"])
    _LOGGER.debug("Running MySQL client program: " + " ".join(command))

    #
    # MySQL client programs do normally try to read the password from the
    # controlling tty (Unix) or the console (Windows).
    #
    # On Unix, if the process does not have a controlling tty, then the
    # program prompts for the password on stderr, and reads the password
    # from stdin. The trick here is to start the MySQL client program
    # with no controlling tty. This can be done by running the program
    # in a new "session". This can be done by running the os.setsid()
    # operating system service in the new process before the program is
    # executed (via the preexec_fn parameter). os.setsid() starts a new
    # "session" without a controlling tty.
    #
    process = Popen(command, stdin=PIPE, stdout=outstream, stderr=PIPE,
                    preexec_fn=os.setsid)

    #
    # Wait for password prompt on stderr. Normally, the password prompt
    # is 16 characters long: "Enter password: " Read just 14 of them, to
    # be safe from possible glitches or small modifications, and
    # compare. Reading more characters than sent causes us to hang.
    # Since the prompt is not terminated with a newline, readline()
    # would hang as well, waiting for the newline.
    #
    prompt = process.stderr.read(14)

    #
    # Check if the prompt asks for a password. Do a fuzzy compare to be
    # able to deal with a slightly shifted or modified prompt.
    #
    if -1 == prompt.lower().find("passw"):
        _LOGGER.debug("MySQL client program did not prompt for password."
                      " Got '%s'", prompt)
        raise _errors.BackupError("Error while running MySQL client program"
                                  " %s: Missing password prompt. Got '%s'" %
                                  (command[0], prompt,))

    #
    # Yes, we got a password prompt. Since we did read 14 characters
    # only, and 16 are likely available, We will at least try to read up
    # to the colon, which is expected to terminate the prompt. But do
    # this only, if we did not get it within the 14 characters already.
    # If we try to read more than sent by the client, before it waits
    # for the password, we produce a deadlock. But since the remainder
    # of the prompt would otherwise become part of the error messages on
    # stderr, we risk to eat the prompt up to the colon at least.
    #
    if -1 == prompt.find(":"):
        # No colon in the prompt. Read single characters until we get it.
        while True:
            prompt = process.stderr.read(1)
            if prompt == ":":
                break

    #
    # Send password to stdin, and read stderr until EOF. stdout is
    # either outstream or tty/console. No output lines will be
    # received here.
    #
    dummy_output_lines, error_lines = process.communicate(str(passwd) + '\n')
    returncode = process.returncode
    _LOGGER.debug("MySQL client program returned: %d\n%s",
                  returncode, error_lines)

    #
    # Return the returncode and the output from the program's stderr.
    #
    return (returncode, error_lines)

class BackupImage(object):
    """Class that represents a backup image to which the output
    of a backup method is directed.

    :param uri: The URI to the destination on which the backup image needs
                       to be stored.
    """
    def __init__(self, uri):
        """The constructor for initializing the backup image.
        """

        self.__uri = uri
        if self.__uri is not None:
            parsed = urlparse(self.__uri)
            self.__scheme = parsed.scheme
            self.__netloc = parsed.netloc
            self.__path = parsed.path
            self.__params = parsed.params
            self.__query = parsed.query
            self.__fragment = parsed.fragment
            self.__username = parsed.username
            self.__password = parsed.password
            self.__hostname = parsed.hostname
            self.__port = parsed.port

    @property
    def uri(self):
        """Return the uri.
        """
        return self.__uri

    @property
    def scheme(self):
        """Return the type of the URI like ftp, http, file etc.
        """
        return self.__scheme

    @property
    def netloc(self):
        """Return the network location in the URI.
        """
        return self.__netloc

    @property
    def path(self):
        """Return the path to the file represented by the URI.
        """
        return self.__path

    @property
    def params(self):
        """Return the parameters in the URI.
        """
        return self.__params

    @property
    def query(self):
        """Return the query in the backup image URI.
        """
        return self.__query

    @property
    def fragment(self):
        """Return the fragment in the backup image URI.
        """
        return self.__fragment

    @property
    def username(self):
        """Return the username in the backup image URI.
        """
        return self.__username

    @property
    def password(self):
        """Return the password in the backup image URI.
        """
        return self.__password

    @property
    def hostname(self):
        """Return the hostname in the backup image URI.
        """
        return self.__hostname

    @property
    def port(self):
        """Return the port number in the backup image URI.
        """
        return self.__port

class BackupMethod(object):
    """Abstract class that represents the interface methods that need to be
    implemented by a class that encapsulates a MySQL backup and restore
    method.
    """
    __metaclass__ = ABCMeta

    @staticmethod
    def check_backup_privileges(server):
        """Check if the server has privileges for backup.
        :param server: The server to be backed up.
        :return: None.
        :raises: ServerError on missing privileges.
        """
        pass

    @staticmethod
    def check_restore_privileges(server):
        """Check if the server has privileges for restore.
        :param server: The server to be restored.
        :return: None.
        :raises: ServerError on missing privileges.
        """
        pass

    @staticmethod
    def backup(server, backup_user, backup_passwd, mysqldump_binary):
        """Perform the backup.

        :param server: The server that needs to be backed up.
        :param backup_user: The user name used for accessing the server.
        :param backup_passwd: The password used for accessing the server.
        :param mysqldump_binary: The fully qualified mysqldump binary.
        """
        pass

    @staticmethod
    def restore_server(host, port, restore_user, restore_passwd,
                       image, mysqlclient_binary):
        """Restore the backup from the image to a server outside the Fabric
        Farm.

        :param host: The host name of the server on which the backup needs
                    to be restored into.
        :param port: The port number of the server on which the backup needs
                    to be restored into
        :param restore_user: The user name used for accessing the server.
        :param restore_passwd: The password used for accessing the server.
        :param image: The image that needs to be restored.
        :param mysqlclient_binary: The fully qualified mysqlclient binary.
        """
        pass

    @staticmethod
    def restore_fabric_server(server, restore_user, restore_passwd,
                              image, mysqlclient_binary):
        """Restore the backup from the image to a server within the
        fabric farm and managed by the Fabric server.

        :param server: The server on which the backup needs to be restored.
        :param restore_user: The user name used for accessing the server.
        :param restore_passwd: The password used for accessing the server.
        :param image: The image that needs to be restored.
        :param mysqlclient_binary: The fully qualified mysqlclient binary.
        """
        pass

    @staticmethod
    def copy_backup(image):
        """Will be used in cases when the backup needs to be taken on a source
        and will be copied to the destination machine.

        :param image: BackupImage object containning the location where the
                      backup needs to be copied to.
        """
        pass

class MySQLDump(BackupMethod):
    """Class that implements the BackupMethod abstract interface
    using mysqldump.
    """
    BACKUP_PRIVILEGES = [
        "EVENT",              # show event information
        "EXECUTE",            # show routine information inside view
        "SELECT",             # read data
        "SHOW VIEW",          # SHOW CREATE VIEW
        "TRIGGER",            # show trigger information
    ]
    RESTORE_PRIVILEGES = [
        "ALTER",              # ALTER DATABASE
        "ALTER ROUTINE",      # ALTER {PROCEDURE|FUNCTION}
        "CREATE",             # CREATE TABLE
        "CREATE ROUTINE",     # CREATE {PROCEDURE|FUNCTION}
        "CREATE TABLESPACE",  # CREATE TABLESPACE
        "CREATE VIEW",        # CREATE VIEW
        "DROP",               # DROP TABLE (used before CREATE TABLE)
        "EVENT",              # DROP/CREATE EVENT
        "INSERT",             # write data
        "LOCK TABLES",        # LOCK TABLES (--single-transaction)
        "REFERENCES",         # Create tables with foreign keys
        "SELECT",             # LOCK TABLES (--single-transaction)
        "SUPER",              # SET @@SESSION.SQL_LOG_BIN= 0
        "TRIGGER",            # CREATE TRIGGER
    ]

    @staticmethod
    def check_backup_privileges(server):
        """Check if the server has privileges for backup.
        :return: None.
        :raises: ServerError on missing privileges.
        """
        server.check_privileges(MySQLDump.BACKUP_PRIVILEGES)

    @staticmethod
    def check_restore_privileges(server):
        """Check if the server has privileges for restore.
        :return: None.
        :raises: ServerError on missing privileges.
        """
        server.check_privileges(MySQLDump.RESTORE_PRIVILEGES)

    @staticmethod
    def dump_to_log(message, error_lines=None):
        """Write the error lines to the log.

        :param message: The message signifying the start of the below log.
        :param error_lines: The error lines that were output by the command.
        """
        if error_lines:
            _LOGGER.debug(message + ": " + error_lines)

    @staticmethod
    def backup(server, backup_user, backup_passwd, mysqldump_binary):
        """Perform the backup using mysqldump.

        The backup results in creation a .sql file on the FABRIC server,
        this method needs to be optimized going forward. But for now
        this will suffice.

        :param server: The MySQLServer that needs to be backed up.
        :param backup_user: The user name used for accessing the server.
        :param backup_passwd: The password used for accessing the server.
        :param mysqldump_binary: The fully qualified mysqldump binary.
        """
        assert isinstance(server, MySQLServer)

        #Extract the host and the port from the server address.
        host = None
        port = None
        if server.address is not None:
            host, port = split_host_port(server.address)

        #Form the name of the destination .sql file from the name of the
        #server host and the port number that is being backed up.
        destination = "MySQL_{HOST}_{PORT}.sql".format(
                                                    HOST=host,
                                                    PORT=port)

        command = shlex.split(mysqldump_binary)

        #Setup the mysqldump command that is used to backup the server.
        command.extend([
            # A --no-defaults or --defaults-file option is inserted by
            # run_mysql_client().
            "--all-databases",
            "--single-transaction",
            "--add-drop-table",
            "--triggers",
            "--routines",
            "--events",
            "--protocol=tcp",
            "-h" + str(host),
            "-P" + str(port),
            "-u" + str(backup_user)
            # A -p is appended by run_mysql_client(), if required.
        ])

        #Run the backup command
        with open(destination, "w") as fd_file:
            returncode, error_lines = run_mysql_client(command, backup_passwd,
                                                       outstream=fd_file)
            if returncode:
                MySQLDump.dump_to_log(
                    "Error while taking backup using " + command[0],
                    error_lines
                )
                raise _errors.BackupError(
                    "Error while taking backup using " + command[0],
                    error_lines
                )

        #Return the backup image containing the location of the .sql file.
        return BackupImage(destination)

    @staticmethod
    def restore_server(host, port, restore_user, restore_passwd,
                       image, mysqlclient_binary):
        """Restore the backup from the image to a server outside the Fabric
        Farm.

        In the current implementation the restore works by restoring the
        .sql file created on the FABRIC server. This will be optimized in future
        implementation. But for the current implementation this suffices.

        :param host: The host name of the server on which the backup needs
                    to be restored into.
        :param port: The port number of the server on which the backup needs
                    to be restored into
        :param restore_user: The user name used for accessing the server.
        :param restore_passwd: The password used for accessing the server.
        :param image: The image that needs to be restored.
        :param mysqlclient_binary: The fully qualified mysqlclient binary.
        """

        command = shlex.split(mysqlclient_binary)

        #Use the mysql client for the restore using the input image as
        #the restore source.
        command.extend([
            # A --no-defaults or --defaults-file option is inserted by
            # run_mysql_client().
            "--no-auto-rehash",
            "--batch",
            "--protocol=tcp",
            "-h" + str(host),
            "-P" + str(port),
            "-u" + str(restore_user),
            "--execute=source " + str(image.path)
            # A -p is appended by run_mysql_client(), if required.
        ])

        #Fire the mysql client for the restore.
        returncode, error_lines = run_mysql_client(command, restore_passwd)
        if returncode:
            MySQLDump.dump_to_log(
                "Error while restoring the backup using " + command[0],
                error_lines
            )
            raise _errors.BackupError(
                "Error while restoring the backup using " + command[0],
                error_lines
            )

    @staticmethod
    def restore_fabric_server(server, restore_user, restore_passwd,
                              image, mysqlclient_binary):
        """Restore the backup from the image to a server within the
        fabric farm and managed by the Fabric server.

        In the current implementation the restore works by restoring the
        .sql file created on the FABRIC server. This will be optimized in future
        implementation. But for the current implementation this suffices.

        :param server: The server on which the backup needs to be restored.
        :param restore_user: The user name used for accessing the server.
        :param restore_passwd: The password used for accessing the server.
        :param image: The image that needs to be restored.
        :param mysqlclient_binary: The fully qualified mysqlclient binary.
        """

        assert isinstance(server, MySQLServer)
        assert image is None or isinstance(image, BackupImage)

        #Extract the host and the port from the server address.
        host = None
        port = None
        if server.address is not None:
            host, port = split_host_port(server.address)
        MySQLDump.restore_server(host, port, restore_user, restore_passwd,
                                 image, mysqlclient_binary)

    @staticmethod
    def copy_backup(image):
        """Currently the MySQLDump based backup method works by backing
        up on the FABRIC server and restoring from there. This method is not
        required for the current implemention of MySQDump based backup.
        """
        pass
