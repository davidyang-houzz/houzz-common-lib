# Refactored out of operation_utils.py on 2019-03-12.

from __future__ import absolute_import
import atexit
import logging
import os
import sys
import time

from houzz.common import config

logger = logging.getLogger(__name__)
Config = config.get_config()

def run_only_one_instance(pid_filename, max_allowed_seconds=7200):
    """Guard only allow one process run at given time.
    Usage: call this function from __main__ with your script's name as
    lock file name. Also need to specify how many seconds the job is
    allowed to run. After the time passed, the next run will kill the
    stale process and its children processes.
    """
    import psutil

    def _remove_pid_lock_file(file_path):
        os.remove(file_path)

    pid_file_path = os.path.join(Config.PID_LOCK_FILE_DIR, pid_filename)
    if os.path.isfile(pid_file_path):
        with open(pid_file_path, 'r') as pf:
            pid_str = pf.readline().strip()
            if pid_str:
                pid = int(pid_str)
                try:
                    # check if the pid is still alive
                    os.kill(pid, 0)
                except OSError:
                    logger.info('Last process pid:%s did not exit cleanly. '
                                'Remove pid lock file:%s', pid, pid_file_path)
                    _remove_pid_lock_file(pid_file_path)
                else:
                    # check if the pid is too old
                    p = psutil.Process(pid)
                    age = int(time.time() - p.create_time())
                    if age > max_allowed_seconds:
                        # kill the stale process and its children
                        logger.info('Last process pid:%s (%s) still running. age is %s, '
                                    'larger than max_allowed_seconds %s. Kill it.',
                                    pid, pid_file_path, age, max_allowed_seconds)
                        for child in p.children(recursive=True):
                            child.kill()
                        p.kill()
                    else:
                        # let it continue
                        logger.info('Last process pid:%s (%s) still running. age is %s, '
                                    'less than max_allowed_seconds %s. Do NOT disturb it.',
                                    pid, pid_file_path, age, max_allowed_seconds)
                        sys.exit(0)
            else:
                logger.info('Remove corrupted pid lock file:%s', pid_file_path)
                _remove_pid_lock_file(pid_file_path)
    with open(pid_file_path, 'w') as pf:
        pf.write('%d' % os.getpid())
    # make sure we cleanup the pid lock file whenever we exit
    atexit.register(_remove_pid_lock_file, pid_file_path)
