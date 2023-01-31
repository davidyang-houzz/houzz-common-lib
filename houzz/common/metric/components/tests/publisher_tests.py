#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
from six.moves.queue import Queue
from mock import (
    Mock,
    call,
    patch,
)
import copy
import glob
import socket
import tempfile
import time
import unittest
from functools import partial
from queuelib import FifoDiskQueue

from houzz.common.mem_disk_queue.mem_disk_queue import MemDiskQueue
from houzz.common.thrift_gen.metrics.ttypes import MetricType
from houzz.common.metric.components.controller import Controller
from houzz.common.metric.components.publisher import (
    MAX_ALLOWED_METRIC_DELAY_MILLISECONDS,
    Publisher,
    persist_metrics,
)
from houzz.common.metric.common.key_value import (
    Key,
    KeyValue,
    Snapshot,
    Value,
    deserialize_metrics,
    serialize_metrics,
)
from houzz.common.metric.components.tests.agent_test_utils import (
    cleanup_metric_client,
    gen_metric_client,
    verify_internal_metrics,
)
from houzz.common.metric.common.tests.common_test_utils import (
    create_metric,
    cleanup_temp_dir,
)
from houzz.common.metric.streamhist import StreamHist
from six.moves import range


class PublisherTest(unittest.TestCase):
    def setUp(self):
        # initialize metric_client
        self.metric_client = gen_metric_client()

        self.controller = Controller()
        self.controller.reset()

        self.ready_snapshots = Queue(100)
        self.pr = Publisher(self.ready_snapshots,
                            queue_pop_timeout=0.1,
                            adjust_timestamp=True)
        self.controller.register(Controller.PUBLISHER, self.pr)

        self.working_dir = tempfile.mkdtemp()
        self.ready_dir = tempfile.mkdtemp()

    def tearDown(self):
        cleanup_metric_client(self.metric_client)
        if self.pr and self.pr.is_alive():
            self.pr.shutdown()
        cleanup_temp_dir(self.working_dir)
        cleanup_temp_dir(self.ready_dir)

    def test_build_metric_lines_counter(self):
        # with tags
        res = self.pr.build_metric_lines(
            key=Key(service_name='a', metric_name='b', timestamp=1449623400000, tags={'x': 'y'}),
            value=Value(metric_type=MetricType.COUNTER, sum=2.0, count=2, ignore_age=False,
                        histogram_buckets=None)
        )
        self.assertListEqual(['put a.b 1449623400000 2.0 x=y\n',
                              'put a.b_count 1449623400000 2 x=y\n'],
                             res)

        # no tags
        res = self.pr.build_metric_lines(
            key=Key(service_name='a', metric_name='b', timestamp=1449623400000),
            value=Value(metric_type=MetricType.COUNTER, sum=2.0, count=1, ignore_age=False,
                        histogram_buckets=None)
        )
        self.assertListEqual(['put a.b 1449623400000 2.0\n',
                              'put a.b_count 1449623400000 1\n'],
                             res)

        # override timestamp
        res = self.pr.build_metric_lines(
            key=Key(service_name='a', metric_name='b', timestamp=123),
            value=Value(metric_type=MetricType.COUNTER, sum=2.0, count=1, ignore_age=False,
                        histogram_buckets=None),
            override_timestamp=1449623400000
        )
        self.assertListEqual(['put a.b 1449623400000 2.0\n',
                              'put a.b_count 1449623400000 1\n'],
                             res)

        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {"dummy_service.publisher.name_group_sent{'group': 'a.b'}": 6.0},
            ignored_tags=self.metric_client.tags
        )

    def test_build_metric_lines_gauge(self):
        # with tags
        res = self.pr.build_metric_lines(
            key=Key(service_name='a', metric_name='b', timestamp=1449623400000, tags={'x': 'y'}),
            value=Value(metric_type=MetricType.GAUGE, sum=2.0, count=2, ignore_age=False,
                        histogram_buckets=None)
        )
        self.assertListEqual(['put a.b 1449623400000 1.0 x=y\n',
                              'put a.b_count 1449623400000 2 x=y\n'],
                             res)

        # no tags
        res = self.pr.build_metric_lines(
            key=Key(service_name='a', metric_name='b', timestamp=1449623400000),
            value=Value(metric_type=MetricType.GAUGE, sum=2.0, count=2, ignore_age=False,
                        histogram_buckets=None)
        )
        self.assertListEqual(['put a.b 1449623400000 1.0\n',
                              'put a.b_count 1449623400000 2\n'],
                             res)

        # override timestamp
        res = self.pr.build_metric_lines(
            key=Key(service_name='a', metric_name='b', timestamp=123),
            value=Value(metric_type=MetricType.GAUGE, sum=2.0, count=2, ignore_age=False,
                        histogram_buckets=None),
            override_timestamp=1449623400000
        )
        self.assertListEqual(['put a.b 1449623400000 1.0\n',
                              'put a.b_count 1449623400000 2\n'],
                             res)

        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {"dummy_service.publisher.name_group_sent{'group': 'a.b'}": 6.0},
            ignored_tags=self.metric_client.tags
        )

    def test_build_metric_lines_histogram(self):
        histo = StreamHist(maxbins=100, weighted=False, freeze=None)
        histo.update(list(range(1, 201)))

        # with tags
        res = self.pr.build_metric_lines(
            key=Key(service_name='a', metric_name='b', timestamp=1449623400000, tags={'x': 'y'}),
            value=Value(metric_type=MetricType.HISTOGRAM, sum=0.0, count=0, ignore_age=False,
                        histogram_buckets=histo)
        )
        self.assertListEqual(
            ['put a.b.p50 1449623400000 100.5 x=y\n',
             'put a.b.p75 1449623400000 150.5 x=y\n',
             'put a.b.p90 1449623400000 180.5 x=y\n',
             'put a.b.p95 1449623400000 190.5 x=y\n',
             'put a.b.p99 1449623400000 198.5 x=y\n',
             'put a.b.mean 1449623400000 100.5 x=y\n',
             'put a.b.min 1449623400000 1.0 x=y\n',
             'put a.b.max 1449623400000 200.0 x=y\n',
             'put a.b.count 1449623400000 200 x=y\n'],
            res)
        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {"dummy_service.publisher.name_group_sent{'group': 'a.b'}": 9.0},
            ignored_tags=self.metric_client.tags
        )

        # no tags
        res = self.pr.build_metric_lines(
            key=Key(service_name='a', metric_name='b', timestamp=1449623400000),
            value=Value(metric_type=MetricType.HISTOGRAM, sum=0.0, count=0, ignore_age=False,
                        histogram_buckets=histo)
        )
        self.assertListEqual(
            ['put a.b.p50 1449623400000 100.5\n',
             'put a.b.p75 1449623400000 150.5\n',
             'put a.b.p90 1449623400000 180.5\n',
             'put a.b.p95 1449623400000 190.5\n',
             'put a.b.p99 1449623400000 198.5\n',
             'put a.b.mean 1449623400000 100.5\n',
             'put a.b.min 1449623400000 1.0\n',
             'put a.b.max 1449623400000 200.0\n',
             'put a.b.count 1449623400000 200\n'],
            res)
        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {"dummy_service.publisher.name_group_sent{'group': 'a.b'}": 9.0},
            ignored_tags=self.metric_client.tags
        )

        # override timestamp
        res = self.pr.build_metric_lines(
            key=Key(service_name='a', metric_name='b', timestamp=123),
            value=Value(metric_type=MetricType.HISTOGRAM, sum=0.0, count=0, ignore_age=False,
                        histogram_buckets=histo),
            override_timestamp=1449623400000
        )
        self.assertListEqual(
            ['put a.b.p50 1449623400000 100.5\n',
             'put a.b.p75 1449623400000 150.5\n',
             'put a.b.p90 1449623400000 180.5\n',
             'put a.b.p95 1449623400000 190.5\n',
             'put a.b.p99 1449623400000 198.5\n',
             'put a.b.mean 1449623400000 100.5\n',
             'put a.b.min 1449623400000 1.0\n',
             'put a.b.max 1449623400000 200.0\n',
             'put a.b.count 1449623400000 200\n'],
            res)
        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {"dummy_service.publisher.name_group_sent{'group': 'a.b'}": 9.0},
            ignored_tags=self.metric_client.tags
        )

    def test_verify_connection(self):
        self.pr.hosts = [(str(i), 2 * i) for i in range(5)]
        self.pr.current_tsd = 0
        self.pr.reconnect_interval = 0
        self.pr.last_verify = 0

        # conn is broken
        self.controller.reset()
        self.pr.tsd = Mock()
        m_sendall = self.pr.tsd.sendall = Mock()
        m_sendall.side_effect = socket.error

        result = self.pr.verify_connection()

        self.assertFalse(result)
        self.assertEqual(1, m_sendall.call_count)
        self.assertIsNone(self.pr.tsd)
        self.assertEqual(1, self.controller.get_stat('publisher.verify_conn_failed'))
        self.assertIsNone(self.controller.get_stat('publisher.verify_conn_success'))

        # conn is ok
        self.controller.reset()
        self.pr.tsd = Mock()
        m_sendall.side_effect = None
        m_recv = self.pr.tsd.recv = Mock()
        m_recv.side_effect = 'dummy return'

        result = self.pr.verify_connection()

        self.assertTrue(result)
        self.assertEqual(1, m_recv.call_count)
        self.assertIsNone(self.controller.get_stat('publisher.verify_conn_failed'))
        self.assertEqual(1, self.controller.get_stat('publisher.verify_conn_success'))

    @patch('houzz.common.metric.components.publisher.time.time')
    def test_adjust_metric_timestamp(self, m_time):
        curr_ts = 1449623400  # datetime.datetime(2015, 12, 8, 17, 10)
        m_time.return_value = curr_ts

        # data from current minute.
        kv = KeyValue.from_thrift_msg(
            create_metric(curr_ts * 1000, 1.0, 2, metric_name_postfix=1)
        )
        k, v = kv.key, kv.val
        k_orig = copy.deepcopy(k)
        v_orig = copy.deepcopy(v)
        self.assertTrue(self.pr.adjust_metric_timestamp(k, v))
        self.assertEqual(v, v_orig)
        self.assertGreaterEqual(k.timestamp - k_orig.timestamp, 0)
        self.assertLess(k.timestamp - k_orig.timestamp, 30 * 1000)

        # data from previous minute.
        kv = KeyValue.from_thrift_msg(
            create_metric((curr_ts - 60) * 1000, 1.0, 2, metric_name_postfix=1)
        )
        k, v = kv.key, kv.val
        k_orig = copy.deepcopy(k)
        v_orig = copy.deepcopy(v)
        self.assertTrue(self.pr.adjust_metric_timestamp(k, v))
        self.assertEqual(v, v_orig)
        self.assertGreaterEqual(k.timestamp - k_orig.timestamp, 30 * 1000)
        self.assertLess(k.timestamp - k_orig.timestamp, 60 * 1000)

        # Too old metrics will be dropped.
        self.assertIsNone(self.controller.get_stat('publisher.drop_old_metrics'))
        kv = KeyValue.from_thrift_msg(
            create_metric(
                curr_ts * 1000 - MAX_ALLOWED_METRIC_DELAY_MILLISECONDS - 1,
                1.0, 2, metric_name_postfix=1
            )
        )
        k, v = kv.key, kv.val
        k_orig = copy.deepcopy(k)
        v_orig = copy.deepcopy(v)
        self.assertFalse(self.pr.adjust_metric_timestamp(k, v))
        self.assertEqual(k, k_orig)
        self.assertEqual(v, v_orig)
        self.assertEqual(1, self.controller.get_stat('publisher.drop_old_metrics'))

        # If Value.ignore_age is set, the metric will pass thru
        v.ignore_age = True
        v_orig = copy.deepcopy(v)
        self.assertTrue(self.pr.adjust_metric_timestamp(k, v))
        self.assertGreaterEqual(k.timestamp - k_orig.timestamp, 30 * 1000)
        self.assertLess(k.timestamp - k_orig.timestamp, 60 * 1000)
        self.assertEqual(v, v_orig)
        # drop_old_metrics is not increased
        self.assertEqual(1, self.controller.get_stat('publisher.drop_old_metrics'))

    @patch('houzz.common.metric.components.publisher.random.randint')
    @patch('houzz.common.metric.components.publisher.time.time')
    @patch('houzz.common.metric.components.publisher.Publisher.verify_connection')
    def test_publisher_run(self, m_vc, m_time, m_randint):
        m_vc.return_value = True
        m_time.return_value = 1449623400 + 60
        m_randint.return_value = 45000

        self.pr.tsd = Mock()
        self.pr.start()

        snapshot = Snapshot()
        # make 2 good rows, 1 too-old row
        for i in range(2):
            snapshot.accumulate(
                KeyValue.from_thrift_msg(
                    create_metric(1449623400000, i * 1.0, i + 1, metric_name_postfix=i)
                )
            )
        snapshot.accumulate(
            KeyValue.from_thrift_msg(
                create_metric(1449623400 * 1000 - MAX_ALLOWED_METRIC_DELAY_MILLISECONDS,
                              i * 1.0, i + 1, metric_name_postfix=i)
            )
        )

        self.ready_snapshots.put_nowait(snapshot)

        while self.controller.get_stat('publisher.num_metrics_received') < 3:
            time.sleep(0.1)

        self.assertEqual(2, self.controller.get_stat('publisher.num_metrics_sent'))
        self.assertEqual(4, self.controller.get_stat('publisher.num_rows_sent'))
        self.assertEqual(1, self.controller.get_stat('publisher.drop_old_metrics'))
        self.assertListEqual(
            self.pr.tsd.method_calls,
            [call.sendall(
                'put dummy_service.dummy_metric 1449623445000 0.0 tag_0=val_0 tag_1=val_1\n'
                'put dummy_service.dummy_metric_count 1449623445000 1 tag_0=val_0 tag_1=val_1\n'
                'put dummy_service.dummy_metric_1 1449623445000 1.0 tag_0=val_0 tag_1=val_1\n'
                'put dummy_service.dummy_metric_1_count 1449623445000 2 tag_0=val_0 tag_1=val_1\n')]
        )

    @patch('houzz.common.mem_disk_queue.mem_disk_queue.os.path.getmtime')
    def test_persist_metrics(self, m_mtime):
        m_mtime.return_value = 1  # allow all files in ready_dir to be processed

        # TODO(zheng): test histogram
        name = 'test_persist_metrics'

        key_vals = []
        for i in range(2):
            kv = KeyValue.from_thrift_msg(
                create_metric(1449623400000, i * 1.0, i * 2, metric_name_postfix=i)
            )
            key_vals.append((kv.key, kv.val))

        persist_metrics(key_vals,
                        name=name,
                        working_dir=self.working_dir,
                        ready_dir=self.ready_dir)

        fl = glob.glob('%s/*' % self.working_dir)
        self.assertEqual(len(fl), 0)
        fl = glob.glob('%s/*' % self.ready_dir)
        self.assertEqual(len(fl), 1)
        mdq = FifoDiskQueue(fl[0])
        self.assertEqual(len(mdq), 1)

        # validate the right data is persisted
        mem_q = Queue(100)
        mdq = MemDiskQueue(
            program_name=name,
            mem_q_pusher=mem_q.put,
            mem_q_popper=mem_q.get,
            ready_checker=lambda: not mem_q.full(),
            empty_checker=mem_q.empty,
            working_dir=self.working_dir,
            ready_dir=self.ready_dir,
            serialize_func=serialize_metrics,
            deserialize_func=deserialize_metrics,
            debug_mdq=False
        )
        mdq.cleanup_orphan_disk_queues(dead_only=False)
        metrics = mdq.pop_nowait()
        self.assertEqual(len(metrics.metrics), 2)
        for i in range(2):
            self.assertEqual(
                metrics.metrics[i],
                create_metric(1449623400000,
                              i * 1.0, i * 2,
                              metric_name_postfix=i,
                              ignore_age=True)  # need it set to avoid being dropped
            )

    def test_send_data(self):
        def mock_tsd(se):
            m_tsd = Mock()
            self.pr.tsd = m_tsd
            m_tsd.sendall.side_effect = se

        self.pr.hosts = [(str(i), 2 * i) for i in range(5)]
        self.pr.current_tsd = 0

        # conn is good
        self.controller.reset()
        mock_tsd(None)
        self.assertIsNone(self.pr._send_data('dummy_str'))
        self.assertIsNone(self.controller.get_stat('publisher.send_data_failure'))
        self.assertIsNone(self.controller.get_stat('publisher.out_of_tsd_connection'))

        # conn is broken
        self.controller.reset()
        mock_tsd(socket.error)
        self.pr.get_next_connection = Mock()
        self.pr.get_next_connection.side_effect = partial(mock_tsd, socket.error)

        with self.assertRaises(socket.error):
            self.pr._send_data('dummy_str')
        self.assertEqual(3, self.controller.get_stat('publisher.send_data_failure'))

        # conn is broken the first time, then recover
        self.controller.reset()
        mock_tsd(socket.error)
        self.pr.get_next_connection = Mock()
        self.pr.get_next_connection.side_effect = partial(mock_tsd, None)
        self.assertIsNone(self.pr._send_data('dummy_str'))
        self.assertEqual(1, self.controller.get_stat('publisher.send_data_failure'))
