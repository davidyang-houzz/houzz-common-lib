# A helper module for dumping stacks of every thread, to debug hanging threads.
#
# Import this module, and call dump_all_stacks() to dump the stack trace of all
# threads to stderr.  Alternatively, send 'kill -PWR (pid)' when you have a
# problem.

from __future__ import absolute_import
import logging
import os
import signal
import sys
import threading
import traceback
import six

logger = logging.getLogger()

# Since we may get SIGPWR after fork, let's defer creating the dumper thread
# until we actually get a signal.
dumper_thr = None
dump_sem = None

def dump_all_stacks():
    pid = os.getpid()
    logger.info('===== dumping stack traces for process %d =====', pid)
    try:
        all_threads = {t.ident: t.name for t in threading.enumerate()}
        for thr_id, frame in six.iteritems(sys._current_frames()):
            lines = ['========== Stack for %d/%s (%s) ==========\n' %
                     (pid, thr_id, all_threads.get(thr_id, ''))]
            lines += traceback.format_stack(frame)
            logger.info(''.join(lines))
        logger.info('===== End of stack traces =====')
    except Exception as e:
        logger.error('Exception while dumping stack:')
        logger.error(e)

# Logging is done in a separate thread to avoid potential deadlock as much as
# possible.
def _dumper_thr_main():
    global dump_sem
    while True:
        dump_sem.acquire()
        logger.info('===== SIGPWR received: dumping stacks =====')
        dump_all_stacks()

def _sig_handler(signum, frame):
    global dumper_thr, dump_sem
    if (dumper_thr is not None) and dumper_thr.is_alive():
        # Tell the already running thread to dump stacks.
        dump_sem.release()
    else:
        dump_sem = threading.Semaphore(1)
        dumper_thr = threading.Thread(
            target=_dumper_thr_main, name='StackDumper')
        dumper_thr.daemon = True
        dumper_thr.start()

try:
    signo = signal.SIGPWR
except AttributeError:
    signo = signal.SIGINFO  # on Mac.

signal.signal(signo, _sig_handler)
