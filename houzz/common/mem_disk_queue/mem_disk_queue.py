#!/usr/bin/env python

from __future__ import absolute_import
import glob
import logging
import os
import re
import random
import shutil
import threading
import time
from six.moves.queue import (
    Empty,
    Full,
)

from queuelib import FifoDiskQueue

DEFAULT_CHUNK_SIZE = 100000
DISK_QUEUE_TOP_DIR = '/home/clipu/c2/tmp'
DISK_QUEUE_WORKING_DIR = os.path.join(DISK_QUEUE_TOP_DIR, 'working')
DISK_QUEUE_READY_DIR = os.path.join(DISK_QUEUE_TOP_DIR, 'ready')
DISK_QUEUE_NAME_TMPL = '%(program)s_%(timestamp)s_%(process_id)s_%(thread_name)s'
DISK_QUEUE_NAME_PATTERN = re.compile(
    r'(?P<program>.*)_(?P<timestamp>\d{10})_(?P<process_id>\d+)_(?P<thread_name>.+)')

logger = logging.getLogger('mdq')


def get_new_disk_queue(working_dir, name, thread_name=None, pid=None, ts=None, chunksize=None):
    thread_name = thread_name if thread_name else threading.current_thread().getName()
    pid = pid if pid else os.getpid()
    ts = int(ts if ts else time.time())
    chunksize = int(chunksize) if chunksize else DEFAULT_CHUNK_SIZE
    p = os.path.join(
        working_dir,
        DISK_QUEUE_NAME_TMPL % {
            'program': name,
            'timestamp': ts,
            'process_id': pid,
            'thread_name': thread_name,
        }
    )
    assert not os.path.exists(p), 'disk_queue file %s already exists' % p
    return FifoDiskQueue(p, chunksize)


def process_exists(pid):
    try:
        os.kill(pid, 0)
    except Exception as e:
        return e.errno != 3  # [Errno 3] No such process
    return True


def check_operation_allowed(fp, dead_only=False, max_idle_seconds=60):
    """Check if the MemDiskQueue file can be manipulated by current process.

    It is allowed IFF:
    - current process has write access to the file,
        * AND *
    - if dead_only is True
        - the creating process is dead.
    - if dead_only is False
        - the file is last changed more than max_idle_seconds ago
    """
    match_obj = DISK_QUEUE_NAME_PATTERN.match(os.path.basename(fp))
    if os.access(fp, os.W_OK) and match_obj:
        process_id = int(match_obj.groupdict()['process_id'])
        if dead_only:
            return not process_exists(process_id)
        else:
            return (time.time() - os.path.getmtime(fp)) > max_idle_seconds
    return False


def get_new_path_for_orphan(fp, dest_dir):
    """Rename and move the orphaned MemDiskQueue file to the destination dir.

    The rename is necessary to avoid the race condition between multiple processes.
    """
    path_info = DISK_QUEUE_NAME_PATTERN.match(os.path.basename(fp)).groupdict()
    path_info['process_id'] = os.getpid()  # mark this file owned by current process
    # There could be multiple orphaned folders to be moved by current process. Use
    # a unique new thread_name to distinguish them.
    path_info['thread_name'] = 'orphan%.6f' % time.time()
    return os.path.join(dest_dir, DISK_QUEUE_NAME_TMPL % path_info)


def is_timer_active(timer):
    return bool(timer and timer.is_alive())


def make_dir_if_not_exists(dir_path):
    if os.path.exists(dir_path):
        assert os.path.isdir(dir_path)
    else:
        os.makedirs(dir_path)


def remove_dir(dir_path):
    try:
        shutil.rmtree(dir_path, ignore_errors=True)
    except:
        logger.warn('Failed removing dir: %s. Ignore', dir_path)
        return False
    return True


def move_dir(src, dest):
    try:
        shutil.move(src, dest)
    except Exception as e:
        logger.warn('Failed move %s to %s with exception: %s. Ignore', src, dest, e)
        return False
    return True


class MemDiskQueue(object):
    """A memory and disk combo queue that supports threaded readers and writers.

    When memory queue is full, new writes go to the disk queue.
    When memory queue is empty, new reads go to the disk queue.
    Memory queue is always taking priority ovr the disk queue.

    NOTE: we are using different disk queues for pushing and popping, due to
    the underlining FifoDiskQueue is not thread-safe. This allows concurrent
    pushing and popping to improve throughput. Queue in stdlib uses the same
    deque because it is thread-safe.
    """
    def __init__(self, program_name,
                 mem_q_pusher, mem_q_popper,
                 ready_checker, empty_checker,
                 working_dir=DISK_QUEUE_WORKING_DIR,
                 ready_dir=DISK_QUEUE_READY_DIR,
                 persist_mem_q=False,
                 chunksize=100000, sleep_seconds_init=1.0, sleep_seconds_max=30.0,
                 serialize_func=str, deserialize_func=lambda x: x,
                 cleanup_orphans=False,
                 debug_mdq=False):
        """Constructor.

        program_name: string, prefix for the disk queue file name
        mem_q_pusher: func to push into memory queue. It has to take positional
                      parameter (block: bool, timeout: float)
        mem_q_popper: func to pop out of memory queue. It has to take positional
                      parameter (block: bool, timeout: float)
        ready_checker: func to check if the memory queue is not full
        persist_mem_q: bool, if True, persist memory queue data to disk when
                       closing / exiting
        chunksize: int, max number of items in each FifoDiskQueue file
        sleep_seconds_init: float, initial sleep duration between flushes
        sleep_seconds_max: float, maximum sleep duration between flushes
        serialize_func: func, serializer before pushing to disk queue
        deserialize_func: func, deserializer after popping from disk queue
        cleanup_orphans: bool, if True, periodically cleanup orphaned disk queues.
        debug_mdq: bool, if True, log debug message. Otherwise, skip it
        empty_checker: func to check if the memory queue is empty
        """
        super(MemDiskQueue, self).__init__()
        self.program_name = program_name
        self.mem_q_pusher = mem_q_pusher
        self.mem_q_popper = mem_q_popper
        self.ready_checker = ready_checker
        self.empty_checker = empty_checker
        self.working_dir = working_dir
        self.ready_dir = ready_dir
        make_dir_if_not_exists(self.working_dir)
        make_dir_if_not_exists(self.ready_dir)
        self.persist_mem_q = persist_mem_q
        self.cleanup_orphans = cleanup_orphans
        self.sleep_seconds_init = sleep_seconds_init
        self.sleep_seconds_max = sleep_seconds_max
        self.sleep_seconds = sleep_seconds_init
        self.serialize_func = serialize_func
        self.deserialize_func = deserialize_func
        self.chunksize = chunksize
        self.push_disk_q = get_new_disk_queue(self.working_dir, program_name,
                                              chunksize=self.chunksize)
        self.using_disk_q = False
        self.ready_disk_queue_pathes = []
        self.pop_disk_q = None
        self.flush_timer = None
        self.check_timer = None
        self.push_lock = threading.Lock()
        self.pop_lock = threading.Lock()

        if debug_mdq:
            logger.setLevel(logging.DEBUG)
        elif logger.getEffectiveLevel() <= logging.DEBUG:
            logger.setLevel(logging.INFO)

        logger.info('created MemDiskQueue: %s', self)

    def __repr__(self):
        return '<%s object at %s. disk_queue: %s>' % (
            self.__class__.__name__, hex(id(self)), self.push_disk_q.path
        )

    def __del__(self):
        """Best effort destructor since Python does not guarantee to call it."""
        self.close()

    def close(self):
        """Clean up resources.

        * Deactivate any pending timers.
        * Close current active disk queue and move it to ready dir.
        * If needs to, persiste entries in memory queue to disk.
        """
        logger.info('close up MemDiskQueue. working_dir: %s, ready_dir: %s',
                    self.working_dir, self.ready_dir)
        try:
            if self.persist_mem_q:
                with self.push_lock, self.pop_lock:
                    if self.push_disk_q is not None:
                        while True:
                            try:
                                data = self.mem_q_popper(block=False)
                            except Empty:
                                break
                            try:
                                self.push_disk_q.push(
                                    self.serialize_func(data)
                                )
                            except:
                                logger.exception('skip unserializable data: %s', data)

            with self.push_lock:
                if is_timer_active(self.flush_timer):
                    self.flush_timer.cancel()
                if self.push_disk_q is not None:  # explict check, otherwise it uses len()
                    self.push_disk_q.close()
                    logger.info('close push_disk_q %s (len: %d)',
                                self.push_disk_q.path, len(self.push_disk_q))
                    self.close_push_disk_queue(self.push_disk_q.path)
                    self.push_disk_q = None

            with self.pop_lock:
                if is_timer_active(self.check_timer):
                    self.check_timer.cancel()
                if self.pop_disk_q is not None:  # explict check, otherwise it uses len()
                    self.pop_disk_q.close()
                    logger.info('close pop_disk_q %s (len: %d)',
                                self.pop_disk_q.path, len(self.pop_disk_q))
                    self.pop_disk_q = None
        except:
            logger.exception('close push or pop disk_q failed. Ignore')

    def setup_next_flush(self):
        """Use timer to register next disk queue flush."""
        self.flush_timer = threading.Timer(self.sleep_seconds, self.flush)
        self.flush_timer.daemon = True
        self.flush_timer.start()

    def push(self, data):
        """Try memory queue first, and fallback to disk queue if memory queue is full."""
        with self.push_lock:
            if self.using_disk_q:
                logger.debug('push to disk: %s', data)
                return self.push_disk_q.push(
                    self.serialize_func(data)
                )

        try:
            self.mem_q_pusher(data, block=False)
            logger.debug('push to memory: %s', data)
            self.sleep_seconds = self.sleep_seconds_init
        except Full:
            logger.debug('mem_q is full. Start writing to disk_queue: %s (len: %d)',
                         self.push_disk_q.path, len(self.push_disk_q))
            with self.push_lock:
                self.push_disk_q.push(
                    self.serialize_func(data)
                )
                logger.debug('push to disk')
                if not self.using_disk_q:
                    self.using_disk_q = True
                    self.setup_next_flush()
                    logger.info('Add timer to flush disk_queue in %.1f seconds.',
                                self.sleep_seconds)

    def flush(self):
        """Close current on-disk push queue, move it to ready dir, and open a new one."""
        while True:
            try:
                new_disk_q = get_new_disk_queue(self.working_dir, self.program_name,
                                                chunksize=self.chunksize)
            except AssertionError:
                # we are within the same second as last time we create DiskQueue, sleep.
                time.sleep(1)
            else:
                break
        logger.info('Create new disk_queue: %s', new_disk_q.path)

        curr_disk_q_path = self.push_disk_q.path
        with self.push_lock:
            try:
                self.push_disk_q.close()
                logger.info('Closed old disk_queue: %s (len: %d)',
                            self.push_disk_q.path, len(self.push_disk_q))
            except:
                # TODO(zheng): In theory, we should not get here. For whatever reason,
                # some race condition makes it possible that the file is claimed and
                # moved by other thread. So push_disk_q.close will raise.
                logger.exception('Failed closing disk_queue: %s. Remove and Ignore',
                                 self.push_disk_q.path)
                remove_dir(self.push_disk_q.path)
                curr_disk_q_path = ''  # signal close_push_disk_queue to ignore it

            self.push_disk_q = new_disk_q
            if self.ready_checker():
                self.using_disk_q = False
                logger.info('mem_q is healthy. Stop using disk_queue: %s (len: %d)',
                            self.push_disk_q.path, len(self.push_disk_q))
            else:
                self.setup_next_flush()
                logger.info('Add timer to flush disk_queue %s in %.1f seconds',
                            self.push_disk_q.path, self.sleep_seconds)

        self.sleep_seconds = min(
            self.sleep_seconds_max,
            self.sleep_seconds * (1.0 + random.random())
        )

        self.close_push_disk_queue(curr_disk_q_path)

    def close_push_disk_queue(self, disk_q_path):
        """Move the given disk queue to ready dir, and append it to the ready queue list."""
        assert os.path.isdir(self.ready_dir), 'Dir %s does not exist' % self.ready_dir
        try:
            # disk_q_path may already be removed if its empty when being closed
            if os.path.exists(disk_q_path) and os.path.isdir(disk_q_path):
                move_dir(disk_q_path, self.ready_dir)
                ready_q_path = os.path.join(self.ready_dir, os.path.basename(disk_q_path))
                self.ready_disk_queue_pathes.append(ready_q_path)
                logger.info('Add new ready disk_queue: %s. ready_disk_queues: %s',
                            ready_q_path, self.ready_disk_queue_pathes)
            else:
                logger.info('Skip moving non-existent disk_queue file: %s. It may be empty '
                            'and removed while being closed', disk_q_path)
        except:
            logger.exception('Failed close_push_disk_queue on: %s. Ignore', disk_q_path)

    def pop(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).

        When pop, the memory queue has priority over the disk queues. Only when
        the memory queue is empty, the disk queues can be popped.
        """
        try:
            return self._pop()
        except Empty:
            logger.debug('no entry can be found in mem_disk_q.')
            if not block:
                raise Empty
            elif timeout is None:
                # block on memory queue
                return self.mem_q_popper(block=True)
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time.time() + timeout
                while True:
                    remaining = endtime - time.time()
                    if remaining <= 0.0:
                        raise Empty
                    try:
                        return self.mem_q_popper(block=True, timeout=remaining)
                    except Empty:
                        pass

    def pop_nowait(self):
        return self.pop(block=False)

    def _pop(self):
        """Try memory queue first, and fallback to disk queue if memory queue is empty.

        It is non-blocking.
        """
        try:
            return self.mem_q_popper(block=False)
        except Empty:
            logger.debug('mem_q is empty.')
            while True:
                with self.pop_lock:
                    if self.pop_disk_q is None and self.ready_disk_queue_pathes:
                        try:
                            fp = self.ready_disk_queue_pathes.pop(0)
                            self.pop_disk_q = FifoDiskQueue(fp)
                        except:
                            logger.exception('Failed reading from disk_q: %s. Remove and Ignore',
                                             fp)
                            self.pop_disk_q = None
                            remove_dir(fp)
                            continue
                        logger.info('Start reading from disk_queue: %s (len: %d)',
                                    self.pop_disk_q.path, len(self.pop_disk_q))

                    if self.pop_disk_q is not None:  # explict check, otherwise it uses len()
                        logger.debug('pop from disk')
                        try:
                            disk_data = self.pop_disk_q.pop()
                        except:
                            logger.exception('Failed popping from disk_q: %s. Remove and Ignore',
                                             self.pop_disk_q.path)
                            remove_dir(self.pop_disk_q.path)
                            self.pop_disk_q = None
                            continue

                        if disk_data:
                            return self.deserialize_func(disk_data)
                        else:
                            try:
                                logger.info('close pop_disk_q %s', self.pop_disk_q.path)
                                self.pop_disk_q.close()
                            except:
                                logger.exception('Failed closing disk_queue: %s. Remove and Ignore',
                                                 self.pop_disk_q.path)
                            remove_dir(self.pop_disk_q.path)
                            self.pop_disk_q = None
                            continue
                    elif self.cleanup_orphans:
                        if not is_timer_active(self.check_timer):
                            logger.info('Add timer to check for ready disk_queue in %.1f seconds',
                                        self.sleep_seconds_max)
                            self.check_timer = threading.Timer(self.sleep_seconds_max,
                                                               self.cleanup_orphan_disk_queues)
                            self.check_timer.daemon = True
                            self.check_timer.start()
                    raise Empty

    def move_orphan_working_disk_queues(self, dead_only=True):
        """Go through working dir and move orphaned disk queues to ready dir.

        The thread who created these queues may be crashed before move them to the
        ready dir, therefore orphaned them. This function will be invoked through
        timer to periodically discover and close them.
        """
        fl = glob.glob(os.path.join(self.working_dir, '*'))
        logger.debug('Check for orphened working disk_queues under: %s. Found: %s',
                     self.working_dir, fl)
        for fp in sorted(fl):
            if check_operation_allowed(fp, dead_only=dead_only):
                new_path = get_new_path_for_orphan(fp, self.ready_dir)
                logger.info('Move new orphened working disk_queue %s to %s', fp, new_path)
                move_dir(fp, new_path)

    def add_orphan_ready_disk_queues(self, dead_only=True):
        """Go through ready dir and append newly discovered queues to ready queue list.

        The thread who created these queues may be crashed before append them to the
        ready queue, therefore orphaned them. This function will be invoked through
        timer to periodically discover and process them.
        """
        fl = glob.glob(os.path.join(self.ready_dir, '%s_*' % self.program_name))
        logger.debug('Check for orphened ready disk_queues under: %s. Found: %s',
                     self.ready_dir, fl)
        if fl:
            with self.pop_lock:
                new_fl = set(fl) - set(self.ready_disk_queue_pathes)
                if self.pop_disk_q is not None:
                    new_fl.discard(self.pop_disk_q.path)
                to_be_added = []
                for fp in sorted(new_fl):
                    if check_operation_allowed(fp,
                                               dead_only=dead_only,
                                               max_idle_seconds=self.sleep_seconds_max * 2):
                        to_be_added.append(fp)
                if to_be_added:
                    self.ready_disk_queue_pathes.extend(to_be_added)
                    logger.info('Add new orphened ready disk_queues: %s', to_be_added)

    def cleanup_orphan_disk_queues(self, dead_only=True):
        """Go through working and ready dirs and clean up orphaned disk queues."""
        self.move_orphan_working_disk_queues(dead_only=dead_only)
        self.add_orphan_ready_disk_queues(dead_only=dead_only)

    def empty(self):
        """Check if both memory and disk queues are empty. Not reliable."""
        return (
            self.empty_checker() and
            (self.push_disk_q is None or len(self.push_disk_q) <= 0) and
            (self.pop_disk_q is None or len(self.pop_disk_q) <= 0) and
            not self.ready_disk_queue_pathes
        )
