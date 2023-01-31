#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from six.moves.queue import Empty
import time
import unittest

from houzz.common.thrift_gen.metrics.ttypes import (
    Bucket,
    Metrics,
    MetricType,
)
from houzz.common.metric.components.aggregator import Aggregator
from houzz.common.metric.components.controller import Controller
from houzz.common.metric.common.key_value import (
    Key,
    Value,
)

from houzz.common.metric.components.tests.agent_test_utils import (
    cleanup_metric_client,
    gen_metric_client,
    verify_internal_metrics,
)
from houzz.common.metric.common.tests.common_test_utils import (
    METRIC_NAME,
    SERVICE_NAME,
    cleanup_mem_disk_queue,
    create_metric,
    create_tags,
    get_mem_disk_queue,
)
from houzz.common.metric.streamhist import StreamHist
from six.moves import range


class AggregatorTest(unittest.TestCase):
    def setUp(self):
        self.ts = 1449623400

        # initialize metric_client
        self.metric_client = gen_metric_client(mem_q_size=20)

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

    def test_aggregator_counter(self):
        self.ag.start()

        for i in range(4):
            # append one bad metric in each Metrics, will be skipped
            self.buf_q.push(Metrics([create_metric(self.ts + i, i * 1.0, i * 2),
                                     create_metric(self.ts + i, sum=None, count=None)]))
        # append one non-Metrics type in memeory queue, will be popped but dropped
        self.buf_q.push('bad')

        for i in range(4, 10):
            # append one bad metric in each Metrics, will be skipped
            self.buf_q.push(Metrics([create_metric(self.ts + i, i * 1.0, i * 2),
                                     create_metric(self.ts + i, sum=None, count=None)]))
        # append one non-Metrics type in disk queue, will not be popped successfully
        self.buf_q.push('bad')

        # add one bad metric with too many tags
        self.buf_q.push(Metrics([
            create_metric(int(time.time()), 100, 200, tags=dict((str(i), str(i)) for i in range(7)))
        ]))

        # wait till aggregator sometime to consume all metrics
        while self.ag.num_metrics_received < 21:
            time.sleep(0.1)

        self.ag.do_snapshot = True
        while self.ag.do_snapshot:
            time.sleep(0.1)

        # all metrics have been consumed
        self.assertRaises(Empty, self.buf_q.pop_nowait)

        self.assertEqual(len(self.ag.snapshot), 0)

        # all aggregated into one key-val
        ss = self.ag.ready_snapshots.get_nowait()
        self.assertRaises(Empty, self.ag.ready_snapshots.get_nowait)

        k1 = Key(SERVICE_NAME, METRIC_NAME, self.ts + 10.5, create_tags())
        self.assertEqual(1, len(ss))
        self.assertEqual(Value(MetricType.COUNTER, 45.0, 90, False, None), ss._data[k1])

        # internal metrics are logged through MetricClient
        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {'dummy_service.aggregator.num_snapshot_done': 1.0,
             'dummy_service.aggregator.num_metrics_received': 21.0,
             'dummy_service.aggregator.num_metrics_sent': 1.0,
             'dummy_service.aggregator.failed_metric_pop': 1.0,
             'dummy_service.aggregator.bad_metric_skipped': 11.0,
             'dummy_service.aggregator.skipped_unknown_data_in_queue': 1.0},
            ignored_tags=self.metric_client.tags
        )

    def test_aggregator_histogram(self):
        self.ag.start()

        for i in range(1, 201):
            self.buf_q.push(Metrics([
                create_metric(self.ts + i, 0.0, 0,
                              type=MetricType.HISTOGRAM,
                              histogram_buckets=[Bucket(i, 1)])
            ]))

        # wait till aggregator sometime to consume all metrics
        while self.ag.num_metrics_received < 200:
            time.sleep(0.1)

        self.ag.do_snapshot = True
        while self.ag.do_snapshot:
            time.sleep(0.1)

        # all metrics have been consumed
        self.assertRaises(Empty, self.buf_q.pop_nowait)

        self.assertEqual(len(self.ag.snapshot), 0)

        # all aggregated into one key-val
        ss = self.ag.ready_snapshots.get_nowait()
        self.assertRaises(Empty, self.ag.ready_snapshots.get_nowait)

        k1 = Key(SERVICE_NAME, METRIC_NAME, self.ts + 10.5, create_tags())
        self.assertEqual(1, len(ss))
        self.assertEqual(
            Value(MetricType.HISTOGRAM, 0.0, 0, False,
                  StreamHist(100).update(list(range(1, 201)))),
            ss._data[k1]
        )

        # internal metrics are logged through MetricClient
        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {'dummy_service.aggregator.num_snapshot_done': 1.0,
             'dummy_service.aggregator.num_metrics_received': 200.0,
             'dummy_service.aggregator.num_metrics_sent': 1.0},
            ignored_tags=self.metric_client.tags
        )
