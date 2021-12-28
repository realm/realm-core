"""Miscellaneous utility functions used by the hang analyzer."""

import os
import sys
import time
import signal
import logging
import subprocess
from distutils import spawn  # pylint: disable=no-name-in-module

from . import pipe

def call(args, logger):
    """Call subprocess on args list."""
    logger.info(str(args))

    # Use a common pipe for stdout & stderr for logging.
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logger_pipe = pipe.LoggerPipe(logger, logging.INFO, process.stdout)
    logger_pipe.wait_until_started()

    ret = process.wait()
    logger_pipe.wait_until_finished()

    if ret != 0:
        logger.error("Bad exit code %d", ret)
        raise Exception("Bad exit code %d from %s" % (ret, " ".join(args)))


def find_program(prog, paths):
    """Find the specified program in env PATH, or tries a set of paths."""
    loc = spawn.find_executable(prog)

    if loc is not None:
        return loc

    for loc in paths:
        full_prog = os.path.join(loc, prog)
        if os.path.exists(full_prog):
            return full_prog

    return None


def callo(args, logger):
    """Call subprocess on args string."""
    logger.info("%s", str(args))
    return subprocess.check_output(args).decode('utf-8', 'replace')


def signal_python(logger, pinfo):
    """Send appropriate dumping signal to python processes."""

    # On Windows, we set up an event object to wait on a signal. For Cygwin, we register
    # a signal handler to wait for the signal since it supports POSIX signals.
    if sys.platform == "win32":
        logger.debug("Signaling python is not supported on windows")
    else:
        logger.info("Sending signal SIGUSR1 to python process %s with PID %d", pinfo.name,
                    pinfo.pid)
        signal_process(logger, pinfo.pid, signal.SIGUSR1)


def signal_process(logger, pid, signalnum):
    """Signal process with signal, N/A on Windows."""
    try:
        os.kill(pid, signalnum)

        logger.info("Waiting for process to report")
        time.sleep(5)
    except OSError as err:
        logger.error("Hit OS error trying to signal process: %s", err)

    except AttributeError:
        logger.error("Cannot send signal to a process on Windows")
