#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from six.moves.queue import Empty
import time
import unittest
from mock import patch

from houzz.common.thrift_gen.metrics.ttypes import (
    Metrics,
    MetricType,
)
from houzz.common.metric.components.aggregator import Aggregator
from houzz.common.metric.components.controller import Controller
from houzz.common.metric.components.snapshoter import (
    Snapshoter,
    generate_client_offset,
    generate_server_offset,
)
from houzz.common.metric.common.key_value import (
    Key,
    KeyValue,
    Snapshot,
    Value,
)

from houzz.common.metric.components.tests.agent_test_utils import (
    cleanup_metric_client,
    gen_metric_client,
    verify_internal_metrics,
)
from houzz.common.metric.common.tests.common_test_utils import (
    METRIC_NAME,
    NUM_TAGS,
    SERVICE_NAME,
    cleanup_mem_disk_queue,
    create_metric,
    create_tags,
    get_mem_disk_queue,
)
from six.moves import range


class SnapshoterTest(unittest.TestCase):
    def setUp(self):
        self.ts = 1449623400

        # initialize metric_client
        self.metric_client = gen_metric_client()

        self.controller = Controller()
        self.controller.reset()

        # initialize receiving queue
        self.buf_q = get_mem_disk_queue(
            name='test_aggregator',
            mem_q_size=5,
            mdq_sleep_init=0.1,
            mdq_sleep_max=0.5
        )

        self.ag = Aggregator(self.buf_q, queue_pop_timeout_seconds=0.1, join_timeout=1.0)
        self.controller.register(Controller.AGGREGATOR, self.ag)

    def tearDown(self):
        cleanup_metric_client(self.metric_client)
        if self.ag and self.ag.is_alive():
            self.ag.shutdown()
        cleanup_mem_disk_queue(self.buf_q)

    def test_generate_offset(self):
        server_offsets = set()
        for i in range(100):
            server_offsets.add(generate_server_offset(30, 40))
        self.assertEqual(len(server_offsets), 1)
        server_offset = server_offsets.pop()
        self.assertGreaterEqual(server_offset, 30)
        self.assertLess(server_offset, 40)

        client_offsets = set()
        for i in range(100):
            client_offsets.add(generate_client_offset(20))
        self.assertGreater(len(client_offsets), 1)
        self.assertGreaterEqual(min(client_offsets), 20)
        self.assertLess(max(client_offsets), server_offset)

    def test_snapshot(self):
        k1 = Key(SERVICE_NAME, METRIC_NAME, self.ts + 10.5, create_tags())
        k2 = Key(SERVICE_NAME, METRIC_NAME + '_1', self.ts + 10.5, create_tags())
        k3 = Key(SERVICE_NAME, METRIC_NAME, self.ts + 10.5, create_tags(num_tags=NUM_TAGS + 1))
        v1 = Value(MetricType.COUNTER, 1.0, 2, False, None)

        kv1 = KeyValue(k1, v1)
        kv2 = KeyValue(k2, v1)
        kv3 = KeyValue(k3, v1)

        ss = Snapshot()
        ss.accumulate(kv1)
        ss.accumulate(kv2)
        ss.accumulate(kv3)
        ss.accumulate(kv1)

        self.assertEqual(3, len(ss))
        self.assertListEqual([k for (k, v) in ss.gen_sorted_key_values()],
                             sorted([k1, k2, k3]))
        self.assertEqual(Value(MetricType.COUNTER, 2.0, 4, False, None), ss._data[k1])
        self.assertEqual(v1, ss._data[k2])
        self.assertEqual(v1, ss._data[k3])

    @patch('houzz.common.metric.components.snapshoter.get_sleep_duration')
    def test_snap_shoter(self, m_gsd):
        m_gsd.return_value = 0.1
        self.ag.start()
        self.assertFalse(self.ag.do_snapshot)

        for i in range(2):
            self.buf_q.push(
                Metrics([create_metric(self.ts + i, i * 1.0, i * 2)])
            )

        sr = Snapshoter(self.ag, 30, join_timeout=1.0)
        self.controller.register(Controller.SNAPSHOTER, sr)
        sr.start()

        # give aggregator sometime to react to the snapshot request
        while not self.controller.get_stat('aggregator.num_snapshot_done'):
            time.sleep(0.1)

        # internal metrics are logged through MetricClient
        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictContainsSubset,
            {'dummy_service.aggregator.num_snapshot_done': 1.0,
             'dummy_service.aggregator.num_metrics_received': 2.0},
            ignored_tags=self.metric_client.tags
        )
        self.assertGreaterEqual(
            self.controller.get_stat('snapshoter.num_snapshot_requests'), 1)

        # all metrics have been consumed
        self.assertRaises(Empty, self.buf_q.pop_nowait)

        # all aggregated into one key-val
        ss = self.ag.ready_snapshots.get_nowait()
        k1 = Key(SERVICE_NAME, METRIC_NAME, self.ts + 10.5, create_tags())
        self.assertEqual(1, len(ss))
        self.assertEqual(Value(MetricType.COUNTER, 1.0, 2, False, None), ss._data[k1])

        sr.shutdown()
