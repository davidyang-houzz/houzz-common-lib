from __future__ import absolute_import
import subprocess
import select
import logging
import errno

# Base on https://gist.github.com/bgreenlee/1402841


def call_with_logger(
        popenargs, logger, stdout_log_level=logging.DEBUG,
        stderr_log_level=logging.ERROR, **kwargs):
    """
    Variant of subprocess.call that accepts a logger instead of stdout/stderr,
    and logs stdout messages via logger.debug and stderr messages via
    logger.error.
    """
    child = subprocess.Popen(
        popenargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)

    log_level = {
        child.stdout: stdout_log_level,
        child.stderr: stderr_log_level
    }

    def check_io():
        ready_to_read = \
            select.select([child.stdout, child.stderr], [], [], 1000)[0]
        for io in ready_to_read:
            line = io.readline()
            curr_level = log_level[io]
            if curr_level:
                logger.log(curr_level, line[:-1])

    # keep checking stdout/stderr until the child exits
    while child.poll() is None:
        check_io()

    check_io()  # check again to catch anything after the process exits

    return child.wait()


class CallWithLogger(object):

    def __init__(
            self, popenargs, logger, stdout_log_level=logging.DEBUG,
            stderr_log_level=logging.ERROR, **kwargs):
        """
        Variant of subprocess.call that accepts a logger instead of
        stdout/stderr, and logs stdout messages via logger.debug and stderr
        messages via logger.error.
        """
        self._child = subprocess.Popen(
            popenargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            **kwargs)
        self._log_level = {
            self._child.stdout: stdout_log_level,
            self._child.stderr: stderr_log_level,
        }
        self._logger = logger

    def _check_io(self):
        try:
            ready_to_read = \
                select.select(
                    [self._child.stdout, self._child.stderr], [], [], 1000)[0]
            for io in ready_to_read:
                line = io.readline()
                curr_level = self._log_level[io]
                if curr_level:
                    self._logger.log(curr_level, line[:-1])
        except select.error as err:
            if err[0] == errno.EINTR:
                self._logger.debug('CallWithLogger: Process terminated')
            else:
                self._logger.error(
                    'CallWithLogger: Process failed: %s' % (err, ))

    def start(self):
        # keep checking stdout/stderr until the child exits
        while self._child.poll() is None:
            self._check_io()
        self._check_io()  # check again to catch anything after process exits
        return self._child.wait()

    def get_child(self):
        return self._child

    def terminate(self):
        if self._child.poll() is None:
            self._logger.debug('CallWithLogger: terminating child process')
            self._child.terminate()

    def is_running(self):
        return self._child.poll() is None
