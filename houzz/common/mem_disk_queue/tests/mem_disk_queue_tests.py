#!/usr/bin/env python

from __future__ import absolute_import
import glob
import logging
import os
import shutil
import tempfile
import threading
import time
import json
import unittest
from collections import defaultdict
from six.moves.queue import (
    Empty,
    Queue,
)
from mock import patch

from queuelib import FifoDiskQueue

from houzz.common.mem_disk_queue.mem_disk_queue import (
    get_new_disk_queue,
    is_timer_active,
    MemDiskQueue,
)
import six
from six.moves import range


MAX_MEM_QUEUE_SIZE = 10

class MemDiskQueueTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s: %(message)s')

        self.program_name = 'q_test'
        self.working_dir = tempfile.mkdtemp()
        self.ready_dir = tempfile.mkdtemp()
        self.mem_q = Queue(MAX_MEM_QUEUE_SIZE)
        self.buf_q = MemDiskQueue(
            program_name=self.program_name,
            mem_q_pusher=self.mem_q.put,
            mem_q_popper=self.mem_q.get,
            ready_checker=lambda: True,
            empty_checker=self.mem_q.empty,
            working_dir=self.working_dir,
            ready_dir=self.ready_dir,
            persist_mem_q=False,
            chunksize=100,
            sleep_seconds_init=0.1,
            sleep_seconds_max=0.5,
            cleanup_orphans=True,
            debug_mdq=False
        )

    def tearDown(self):
        self.buf_q.close()
        shutil.rmtree(self.working_dir, ignore_errors=True)
        shutil.rmtree(self.ready_dir, ignore_errors=True)

    def test_get_new_disk_queue(self):
        ts = int(time.time())
        disk_q = get_new_disk_queue(self.working_dir, name='xyz', ts=ts)
        dirp, bn = os.path.split(disk_q.path)
        self.assertEqual(dirp, self.working_dir)
        p, t, pid, tn = bn.split('_')
        self.assertIsInstance(disk_q, FifoDiskQueue)
        self.assertEqual(p, 'xyz')
        self.assertEqual(int(t), ts)
        self.assertGreater(int(pid), 0)
        self.assertIn('Thread', tn)

    def _gen_data(self, idx):
        return json.dumps({idx: idx * idx})

    def _verify_data(self, idx, serialized):
        obj = json.loads(serialized)
        obj = dict((int(k), int(v)) for k, v in six.iteritems(obj))
        self.assertDictEqual(obj, {idx: idx * idx})

    def _get_active_disk_queues(self):
        return glob.glob(os.path.join(self.working_dir, '*'))

    def _get_ready_disk_queues(self):
        return glob.glob(os.path.join(self.ready_dir, '*'))

    def test_pure_mem_queue_push_and_pop(self):
        # test push
        for i in range(MAX_MEM_QUEUE_SIZE):
            self.buf_q.push(self._gen_data(i))
        self.assertEqual(self.mem_q.qsize(), MAX_MEM_QUEUE_SIZE)
        self.assertEqual(len(self.buf_q.push_disk_q), 0)

        # test pop
        for i in range(MAX_MEM_QUEUE_SIZE):
            self._verify_data(i, self.buf_q.pop())
        self.assertEqual(self.mem_q.qsize(), 0)
        self.assertEqual(len(self.buf_q.push_disk_q), 0)

        # test close
        self.buf_q.close()
        self.assertEqual(len(self._get_active_disk_queues()), 0)
        # disk_queue is empty, so it will be deleted
        self.assertEqual(len(self._get_ready_disk_queues()), 0)

    def test_disk_queue_push_and_pop(self):
        # test push
        for i in range(MAX_MEM_QUEUE_SIZE + 1):
            self.buf_q.push(self._gen_data(i))
        self.assertEqual(self.mem_q.qsize(), MAX_MEM_QUEUE_SIZE)
        self.assertEqual(len(self.buf_q.push_disk_q), 1)
        self.assertEqual(len(self._get_active_disk_queues()), 1)
        self.assertEqual(len(self._get_ready_disk_queues()), 0)
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 0)
        self.assertTrue(self.buf_q.using_disk_q)

        # wait for flush to finish, so the disk_queue file is moved to ready dir
        time.sleep(self.buf_q.sleep_seconds + 0.1)
        self.assertIsNone(self.buf_q.pop_disk_q)
        self.assertFalse(self.buf_q.using_disk_q)  # since ready_checker report healthy
        self.assertEqual(len(self._get_active_disk_queues()), 1)
        self.assertEqual(len(self._get_ready_disk_queues()), 1)
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 1)

        # test pop
        for i in range(MAX_MEM_QUEUE_SIZE + 1):
            self._verify_data(i, self.buf_q.pop())
        self.assertEqual(self.mem_q.qsize(), 0)
        self.assertEqual(len(self.buf_q.push_disk_q), 0)
        self.assertIsNotNone(self.buf_q.pop_disk_q)
        self.assertEqual(len(self._get_active_disk_queues()), 1)
        self.assertEqual(len(self._get_ready_disk_queues()), 1)
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 0)
        self.assertFalse(is_timer_active(self.buf_q.check_timer))

        # no more data, one more pop
        self.assertRaises(Empty, self.buf_q.pop_nowait)
        self.assertIsNone(self.buf_q.pop_disk_q)
        self.assertEqual(len(self._get_active_disk_queues()), 1)
        self.assertEqual(len(self._get_ready_disk_queues()), 0)
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 0)
        self.assertTrue(is_timer_active(self.buf_q.check_timer))

    @patch('houzz.common.mem_disk_queue.mem_disk_queue.os.path.getmtime')
    @patch('houzz.common.mem_disk_queue.mem_disk_queue.os.getpid')
    def test_cleanup_orphan_disk_queues(self, m_pid, m_mtime):
        m_pid.return_value = 99999
        m_mtime.return_value = 1  # allow all files in ready_dir to be processed

        def _create_ready_file(ts):
            q_obj = get_new_disk_queue(self.ready_dir, self.program_name, ts=ts)
            for i in range(MAX_MEM_QUEUE_SIZE):
                q_obj.push(self._gen_data(i))
            q_obj.close()
            return q_obj.path

        def _create_working_file(ts, cnt=MAX_MEM_QUEUE_SIZE, pid=None):
            q_obj = get_new_disk_queue(self.working_dir, self.program_name, pid=pid, ts=ts)
            if cnt:
                for i in range(cnt):
                    q_obj.push(self._gen_data(i))
                q_obj.close()
            return q_obj.path

        # prepare some ready disk_queue files
        num_qfs = 2
        ts_now = int(time.time())
        qfs = [_create_ready_file(ts_now + i) for i in range(num_qfs)]

        # prepare some orphaned working disk_queue files
        qfs.append(_create_working_file(ts_now + 3, cnt=0, pid=123456789))  # empty q from dead proc
        qfs.append(_create_working_file(ts_now + 4, pid=123456789))  # non-empty q from dead proc
        qfs.append(_create_working_file(ts_now + 5, pid=1))          # non-empty q from alive proc

        self.assertIsNone(self.buf_q.pop_disk_q)
        self.assertFalse(is_timer_active(self.buf_q.check_timer))
        self.assertEqual(len(self._get_ready_disk_queues()), 2)
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 0)

        # first pop should raise
        self.assertRaises(Empty, self.buf_q.pop_nowait)
        self.assertTrue(is_timer_active(self.buf_q.check_timer))

        # wait for check to be done
        time.sleep(self.buf_q.sleep_seconds_max + 0.1)
        self.assertFalse(is_timer_active(self.buf_q.check_timer))
        self.assertEqual(len(self._get_ready_disk_queues()), 4)
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 4)
        # dead proc's pid is replaced with current proc pid
        qfs[-3] = qfs[-3].replace('123456789', '99999')
        qfs[-2] = qfs[-2].replace('123456789', '99999')
        self.assertListEqual(
            [os.path.basename(i).rsplit('_', 1)[0] for i in self.buf_q.ready_disk_queue_pathes],
            [os.path.basename(j).rsplit('_', 1)[0] for j in qfs[:-1]]
        )  # last one ignored

        # pop first disk queueu
        for i in range(MAX_MEM_QUEUE_SIZE):
            self._verify_data(i, self.buf_q.pop())
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 3)
        old_pop_disk_q = self.buf_q.pop_disk_q
        self.assertNotEqual(old_pop_disk_q.path, self.buf_q.ready_disk_queue_pathes[0])

        # check for available ready disk_queues, nothing should change
        self.buf_q.cleanup_orphan_disk_queues()
        self.assertEqual(len(self._get_ready_disk_queues()), 4)
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 3)
        new_pop_disk_q = self.buf_q.pop_disk_q
        self.assertEqual(old_pop_disk_q.path, new_pop_disk_q.path)

        # pop second queue
        for i in range(MAX_MEM_QUEUE_SIZE):
            self._verify_data(i, self.buf_q.pop())
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 2)

        # pop third and fourth queue
        for i in range(MAX_MEM_QUEUE_SIZE):
            self._verify_data(i, self.buf_q.pop())
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 0)

        # another pop should raise
        self.assertRaises(Empty, self.buf_q.pop_nowait)
        self.assertIsNone(self.buf_q.pop_disk_q)

    def test_persist_mem_q(self):
        self.buf_q.persist_mem_q = True

        # add data to mem_q
        for i in range(MAX_MEM_QUEUE_SIZE):
            self.buf_q.push(self._gen_data(i))
        self.assertEqual(self.mem_q.qsize(), MAX_MEM_QUEUE_SIZE)
        self.assertEqual(len(self.buf_q.push_disk_q), 0)
        self.assertEqual(len(self._get_ready_disk_queues()), 0)
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 0)

        # persist mem_q data to disk
        self.buf_q.close()
        self.assertEqual(self.mem_q.qsize(), 0)
        self.assertIsNone(self.buf_q.push_disk_q)
        self.assertEqual(len(self.buf_q.ready_disk_queue_pathes), 1)

        # verify all data is persisted to disk
        disk_paths = self._get_ready_disk_queues()
        self.assertEqual(len(disk_paths), 1)
        disk_queue = FifoDiskQueue(disk_paths[0])
        self.assertEqual(len(disk_queue), MAX_MEM_QUEUE_SIZE)
        for i in range(MAX_MEM_QUEUE_SIZE):
            self._verify_data(i, disk_queue.pop())

    def test_multi_threaded_reader_writer(self):
        num_writer = 7
        num_reader = 13
        num_rows_per_writer = 1000
        total_rows = num_rows_per_writer * num_writer

        num_rows_read = defaultdict(int)  # thread_name => num_rows_read

        # use approximate queue fullness check
        self.buf_q.ready_checker = lambda: not self.mem_q.full()

        def get_total_rows_read():
            return sum(six.itervalues(num_rows_read))

        def writer():
            for i in range(num_rows_per_writer):
                self.buf_q.push(self._gen_data(i))

        def reader():
            thread_name = threading.current_thread().getName()
            while True:
                try:
                    self.buf_q.pop_nowait()
                    num_rows_read[thread_name] += 1
                except Empty:
                    if get_total_rows_read() == total_rows:
                        return
                    else:
                        logging.debug('Read %d rows. Across all threads %d rows. Continue to wait',
                                      num_rows_read[thread_name], get_total_rows_read())
                        time.sleep(0.1)

        threads = []

        time_start = time.time()
        for i in range(num_reader):
            t = threading.Thread(target=reader, name='reader-%d' % i)
            t.start()
            threads.append(t)

        for i in range(num_writer):
            t = threading.Thread(target=writer, name='writer-%d' % i)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
        time_end = time.time()

        logging.info('finished %d rows en/dequeue in %.2f seconds. qps: %d',
                     total_rows,
                     time_end - time_start,
                     total_rows / (time_end - time_start))

        # verify all rows have been enqueued and dequeued
        self.assertEqual(get_total_rows_read(), total_rows)
        self.assertEqual(self.mem_q.qsize(), 0)
        self.assertEqual(len(self.buf_q.push_disk_q), 0)
        self.assertTrue(self.buf_q.pop_disk_q is None or len(self.buf_q.pop_disk_q) == 0)

        self.buf_q.close()
        self.assertEqual(len(self._get_ready_disk_queues()), 0)
