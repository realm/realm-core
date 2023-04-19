"""Tools to dump debug info for each OS."""

import os
import sys
import tempfile
import itertools
from distutils import spawn  # pylint: disable=no-name-in-module
from collections import namedtuple

from . import process

Dumpers = namedtuple('Dumpers', ['dbg'])


def get_dumpers():
    """Return OS-appropriate dumpers."""

    dbg = None
    jstack = None
    if sys.platform.startswith("linux"):
        dbg = GDBDumper()
    elif sys.platform == "win32" or sys.platform == "cygwin":
        dbg = WindowsDumper()
    elif sys.platform == "darwin":
        dbg = LLDBDumper()

    return Dumpers(dbg=dbg)


class Dumper(object):
    """Abstract base class for OS-specific dumpers."""

    def dump_info(  # pylint: disable=too-many-arguments,too-many-locals
            self, root_logger, logger, pinfo, take_dump):
        """
        Perform dump for a process.
        :param root_logger: Top-level logger
        :param logger: Logger to output dump info to
        :param pinfo: A Pinfo describing the process
        :param take_dump: Whether to take a core dump
        """
        raise NotImplementedError("dump_info must be implemented in OS-specific subclasses")

    @staticmethod
    def get_dump_ext():
        """Return the dump file extension."""
        raise NotImplementedError("get_dump_ext must be implemented in OS-specific subclasses")


class WindowsDumper(Dumper):
    """WindowsDumper class."""

    @staticmethod
    def __find_debugger(logger, debugger):
        """Find the installed debugger."""
        # We are looking for c:\Program Files (x86)\Windows Kits\8.1\Debuggers\x64
        cdb = spawn.find_executable(debugger)
        if cdb is not None:
            return cdb
        from win32com.shell import shell, shellcon

        # Cygwin via sshd does not expose the normal environment variables
        # Use the shell api to get the variable instead
        root_dir = shell.SHGetFolderPath(0, shellcon.CSIDL_PROGRAM_FILESX86, None, 0)

        # Construct the debugger search paths in most-recent order
        debugger_paths = [os.path.join(root_dir, "Windows Kits", "10", "Debuggers", "x64")]
        for idx in reversed(range(0, 2)):
            debugger_paths.append(
                os.path.join(root_dir, "Windows Kits", "8." + str(idx), "Debuggers", "x64"))

        for dbg_path in debugger_paths:
            logger.info("Checking for debugger in %s", dbg_path)
            if os.path.exists(dbg_path):
                return os.path.join(dbg_path, debugger)

        return None

    def dump_info(  # pylint: disable=too-many-arguments
            self, root_logger, logger, pinfo, take_dump):
        """Dump useful information to the console."""
        debugger = "cdb.exe"
        dbg = self.__find_debugger(root_logger, debugger)

        if dbg is None:
            root_logger.warning("Debugger %s not found, skipping dumping of %d", debugger,
                                pinfo.pid)
            return

        root_logger.info("Debugger %s, analyzing %s process with PID %d", dbg, pinfo.name,
                         pinfo.pid)

        dump_command = ""
        if take_dump:
            # Dump to file, dump_<process name>.<pid>.mdmp
            dump_file = "dump_%s.%d.%s" % (os.path.splitext(pinfo.name)[0], pinfo.pid,
                                           self.get_dump_ext())
            dump_command = ".dump /ma %s" % dump_file
            root_logger.info("Dumping core to %s", dump_file)

        cmds = [
            ".symfix",  # Fixup symbol path
            ".symopt +0x10",  # Enable line loading (off by default in CDB, on by default in WinDBG)
            ".reload",  # Reload symbols
            "!peb",  # Dump current exe, & environment variables
            "lm",  # Dump loaded modules
            dump_command,
            "!uniqstack -pn",  # Dump All unique Threads with function arguments
            "!cs -l",  # Dump all locked critical sections
            ".detach",  # Detach
            "q"  # Quit
        ]

        process.call([dbg, '-c', ";".join(cmds), '-p', str(pinfo.pid)], logger)

        root_logger.info("Done analyzing %s process with PID %d", pinfo.name, pinfo.pid)

    @staticmethod
    def get_dump_ext():
        """Return the dump file extension."""
        return "mdmp"


# LLDB dumper is for MacOS X
class LLDBDumper(Dumper):
    """LLDBDumper class."""

    @staticmethod
    def __find_debugger(debugger):
        """Find the installed debugger."""
        return process.find_program(debugger, ['/usr/bin'])

    def dump_info(  # pylint: disable=too-many-arguments,too-many-locals
            self, root_logger, logger, pinfo, take_dump):
        """Dump info."""
        debugger = "lldb"
        dbg = self.__find_debugger(debugger)

        if dbg is None:
            root_logger.warning("Debugger %s not found, skipping dumping of %d", debugger,
                                pinfo.pid)
            return

        root_logger.info("Debugger %s, analyzing %s process with PID %d", dbg, pinfo.name,
                         pinfo.pid)

        lldb_version = process.callo([dbg, "--version"], logger)

        logger.info(lldb_version)

        # Do we have the XCode or LLVM version of lldb?
        # Old versions of lldb do not work well when taking commands via a file
        # XCode (7.2): lldb-340.4.119
        # LLVM - lldb version 3.7.0 ( revision )

        if 'version' not in lldb_version:
            # We have XCode's lldb
            lldb_version = lldb_version[lldb_version.index("lldb-"):]
            lldb_version = lldb_version.replace('lldb-', '')
            lldb_major_version = int(lldb_version[:lldb_version.index('.')])
            if lldb_major_version < 340:
                logger.warning("Debugger lldb is too old, please upgrade to XCode 7.2")
                return

        dump_command = ""
        if take_dump:
            # Dump to file, dump_<process name>.<pid>.core
            dump_file = "dump_%s.%d.%s" % (pinfo.name, pinfo.pid, self.get_dump_ext())
            dump_command = "process save-core %s" % dump_file
            root_logger.info("Dumping core to %s", dump_file)

        cmds = [
            "attach -p %d" % pinfo.pid,
            "target modules list",
            "thread backtrace all",
            dump_command,
            "settings set interpreter.prompt-on-quit false",
            "quit",
        ]

        tf = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8')

        for cmd in cmds:
            tf.write(cmd + "\n")

        tf.flush()

        # Works on in MacOS 10.9 & later
        #call([dbg] +  list( itertools.chain.from_iterable([['-o', b] for b in cmds])), logger)
        process.call(['cat', tf.name], logger)
        process.call([dbg, '--source', tf.name], logger)

        root_logger.info("Done analyzing %s process with PID %d", pinfo.name, pinfo.pid)

    @staticmethod
    def get_dump_ext():
        """Return the dump file extension."""
        return "core"


# GDB dumper is for Linux
class GDBDumper(Dumper):
    """GDBDumper class."""

    @staticmethod
    def __find_debugger(debugger):
        """Find the installed debugger."""
        return process.find_program(debugger, ['/opt/mongodbtoolchain/gdb/bin', '/usr/bin'])

    def dump_info(  # pylint: disable=too-many-arguments,too-many-locals
            self, root_logger, logger, pinfo, take_dump):
        """Dump info."""
        debugger = "gdb"
        dbg = self.__find_debugger(debugger)

        if dbg is None:
            logger.warning("Debugger %s not found, skipping dumping of %d", debugger, pinfo.pid)
            return

        root_logger.info("Debugger %s, analyzing %s process with PID %d", dbg, pinfo.name,
                         pinfo.pid)

        dump_command = ""
        if take_dump:
            # Dump to file, dump_<process name>.<pid>.core
            dump_file = "dump_%s.%d.%s" % (pinfo.name, pinfo.pid, self.get_dump_ext())
            dump_command = "gcore %s" % dump_file
            root_logger.info("Dumping core to %s", dump_file)

        process.call([dbg, "--version"], logger)

        cmds = [
            "set interactive-mode off",
            "set print thread-events off",  # Suppress GDB messages of threads starting/finishing.
            "attach %d" % pinfo.pid,
            "info sharedlibrary",
            "info threads",  # Dump a simple list of commands to get the thread name
            "set python print-stack full",
            "thread apply all bt",
            # Lock the scheduler, before running commands, which execute code in the attached process.
            "set scheduler-locking on",
            dump_command,
            "set confirm off",
            "quit",
        ]

        process.call([dbg, "--quiet", "--nx"] + list(
            itertools.chain.from_iterable([['-ex', b] for b in cmds])), logger)

        root_logger.info("Done analyzing %s process with PID %d", pinfo.name, pinfo.pid)

    @staticmethod
    def get_dump_ext():
        """Return the dump file extension."""
        return "core"

    @staticmethod
    def _find_gcore():
        """Find the installed gcore."""
        dbg = "/usr/bin/gcore"
        if os.path.exists(dbg):
            return dbg

        return None

