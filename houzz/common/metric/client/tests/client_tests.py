#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
import os
import tempfile
import time
import unittest

from houzz.common.thrift_gen.metrics.ttypes import MetricType
from houzz.common.metric.client.client import (
    MetricClientFactory,
)
from houzz.common.metric.client.pusher import (
    MetricPusherFactory,
    close_thrift_client,
    get_thrift_client,
)

from houzz.common.metric.components.tests.agent_test_utils import (
    call_agent_server,
    cleanup_metric_client,
    start_agent_server,
    get_agent_stat,
    gen_metric_client,
)
from houzz.common.metric.common.tests.common_test_utils import (
    cleanup_temp_dir,
    create_metric,
    create_tags,
)
from six.moves import range

uds_name = 'client_test_uds'

uds_dir = None
client_metric_client = None
thrift_client = None
server_process = None

def get_uds_path():
    global uds_dir
    if not uds_dir:
        uds_dir = tempfile.mkdtemp()
    return os.path.join(uds_dir, uds_name)

def setup_module(_module):
    # server side
    # start the agent server in a separate process
    global server_process

    uds = get_uds_path()
    logging.info('in client_tests, use uds: %s', uds)
    server_process = start_agent_server(
        disable_snapshot=False,  # start snapshoter, and force snapshot through API
        uds=uds,
        mem_q_size=10
    )

    # wait till agent server is up
    while not os.path.exists(uds):
        time.sleep(0.1)

    # client side
    global thrift_client
    thrift_client = get_thrift_client(uds=uds)

    metric_client = gen_metric_client()
    MetricPusherFactory._reset_metric_pusher(
        metric_queue=metric_client.client_queue,
        collector_uds=uds,
        queue_pop_timeout=0.1,
        ready_queue_pop_timeout=0.1,
        disable_snapshot=True,  # manually control when to snapshot
        shutdown_timeout=0.5,
        max_push_chunk_size=2
    )


def teardown_module(_module):
    # server side
    call_agent_server(thrift_client, 'shutdown', join_timeout=0.1)
    server_process.terminate()
    while server_process.is_alive():
        time.sleep(0.1)

    # client side
    MetricPusherFactory._get_metric_pusher().shutdown()
    cleanup_metric_client(MetricClientFactory._get_metric_client())
    cleanup_temp_dir(uds_dir)
    close_thrift_client(thrift_client)


class MetricClientTest(unittest.TestCase):
    def setUp(self):
        self.ts = 1449623400 * 1000
        self.metric = create_metric(ts=self.ts, sum=1.0, count=2)
        self.metric_pusher = MetricPusherFactory._get_metric_pusher()
        self.metric_client = MetricClientFactory._get_metric_client()

    def tearDown(self):
        # reset client counters
        self.metric_pusher.aggregator.num_metrics_received = 0
        self.metric_pusher.num_pushed_metrics = 0
        # reset server counters and stdout holder
        call_agent_server(thrift_client, 'reset_stats')
        call_agent_server(thrift_client, 'reset_dryrun_out')

    def _test_log_metrics(self, metric_type, metric_name, metric_vals,
                          expected_received, expected_metrics_sent, expected_rows_sent,
                          expected_put,
                          service_name=None, tags=None):
        # client side
        for metric_val in metric_vals:
            if metric_type == MetricType.COUNTER:
                self.metric_client.log_counter(metric_name, metric_val, timestamp=self.ts,
                                               service_name=service_name, tags=tags)
            elif metric_type == MetricType.GAUGE:
                self.metric_client.log_gauge(metric_name, metric_val, timestamp=self.ts,
                                             service_name=service_name, tags=tags)
            else:
                self.assertTrue(False, 'not supported metric_type: %s' % metric_type)

        while self.metric_pusher.aggregator.num_metrics_received != len(metric_vals):
            # allow all lines to be received by client-side aggregator
            logging.debug('client num_metrics_received: %s',
                          self.metric_pusher.aggregator.num_metrics_received)
            time.sleep(0.1)

        while not self.metric_pusher.num_pushed_metrics:
            self.metric_pusher.aggregator.do_snapshot = True  # force to flush
            logging.debug('client num_pushed_metrics: %s',
                          self.metric_pusher.num_pushed_metrics)
            time.sleep(0.1)

        self.assertEqual(self.metric_pusher.num_pushed_metrics, expected_received)

        # server side
        while not get_agent_stat(thrift_client, 'publisher.num_rows_sent'):
            call_agent_server(thrift_client, 'snapshot')
            logging.debug('server wait for lines to be published')
            time.sleep(0.1)

        self.assertEqual(get_agent_stat(thrift_client, 'publisher.num_metrics_received'),
                         expected_received)
        self.assertEqual(get_agent_stat(thrift_client, 'publisher.num_metrics_sent'),
                         expected_metrics_sent)
        self.assertEqual(get_agent_stat(thrift_client, 'publisher.num_rows_sent'),
                         expected_rows_sent)
        self.assertEqual(call_agent_server(thrift_client, 'get_dryrun_out'), expected_put)

    def test_log_counter_single(self):
        self._test_log_metrics(
            MetricType.COUNTER,
            'metric.counter',
            [10],
            1, 1, 2,
            'put dummy_service.metric.counter 1449623400000 10.0 tag_0=val_0 tag_1=val_1\n'
            'put dummy_service.metric.counter_count 1449623400000 1 tag_0=val_0 tag_1=val_1\n'
        )

    def test_log_counter_multi(self):
        self._test_log_metrics(
            MetricType.COUNTER,
            'metric.counter',
            [10, 20],
            1, 1, 2,
            'put dummy_service.metric.counter 1449623400000 30.0 tag_0=val_0 tag_1=val_1\n'
            'put dummy_service.metric.counter_count 1449623400000 2 tag_0=val_0 tag_1=val_1\n',
            tags=create_tags(),
        )

    def test_log_counter_multi_service_name_and_tags(self):
        # also test service_name and tags overriding
        self._test_log_metrics(
            MetricType.COUNTER,
            'metric.counter',
            [10, 20],
            1, 1, 2,
            'put metric.counter 1449623400000 30.0 a=b tag_0=val_0 tag_1=val_1\n'
            'put metric.counter_count 1449623400000 2 a=b tag_0=val_0 tag_1=val_1\n',
            service_name='',
            tags={'a': 'b'}
        )

    def test_log_gauge_single(self):
        self._test_log_metrics(
            MetricType.GAUGE,
            'metric.gauge',
            [10],
            1, 1, 2,
            'put dummy_service.metric.gauge 1449623400000 10.0 tag_0=val_0 tag_1=val_1\n'
            'put dummy_service.metric.gauge_count 1449623400000 1 tag_0=val_0 tag_1=val_1\n'
        )

    def test_log_gauge_multi(self):
        # For gauge metrics, take average of the sum when aggregates
        self._test_log_metrics(
            MetricType.GAUGE,
            'metric.gauge',
            [10, 20],
            1, 1, 2,
            'put dummy_service.metric.gauge 1449623400000 15.0 tag_0=val_0 tag_1=val_1\n'
            'put dummy_service.metric.gauge_count 1449623400000 2 tag_0=val_0 tag_1=val_1\n'
        )

    def test_log_gauge_multi_service_name_and_tags(self):
        self._test_log_metrics(
            MetricType.GAUGE,
            'metric.gauge',
            [10, 20],
            1, 1, 2,
            'put x.metric.gauge 1449623400000 15.0 a=b tag_0=val_0 tag_1=val_1\n'
            'put x.metric.gauge_count 1449623400000 2 a=b tag_0=val_0 tag_1=val_1\n',
            service_name='x',
            tags={'a': 'b'}
        )

    def test_pusher_chunkify(self):
        num_metrics = 3
        # client side
        for i in range(num_metrics):
            self.metric_client.log_counter('metric.counter_%d' % i, i, timestamp=self.ts)

        while self.metric_pusher.aggregator.num_metrics_received != num_metrics:
            # allow all lines to be received by client-side aggregator
            logging.debug('client num_metrics_received: %s',
                          self.metric_pusher.aggregator.num_metrics_received)
            time.sleep(0.1)

        while self.metric_pusher.num_pushed_metrics != num_metrics:
            self.metric_pusher.aggregator.do_snapshot = True  # force to flush
            logging.debug('client num_pushed_metrics: %s',
                          self.metric_pusher.num_pushed_metrics)
            time.sleep(0.1)

        # server side
        while not get_agent_stat(thrift_client, 'publisher.num_rows_sent'):
            call_agent_server(thrift_client, 'snapshot')
            logging.debug('server wait for lines to be published')
            time.sleep(0.1)

        self.assertEqual(get_agent_stat(thrift_client, 'publisher.num_metrics_received'),
                         num_metrics)
        self.assertEqual(get_agent_stat(thrift_client, 'publisher.num_metrics_sent'),
                         num_metrics)
        self.assertEqual(get_agent_stat(thrift_client, 'publisher.num_rows_sent'),
                         2 * num_metrics)
        self.assertEqual(
            call_agent_server(thrift_client, 'get_dryrun_out'),
            'put dummy_service.metric.counter_0 1449623400000 0.0 tag_0=val_0 tag_1=val_1\n'
            'put dummy_service.metric.counter_0_count 1449623400000 1 tag_0=val_0 tag_1=val_1\n'
            'put dummy_service.metric.counter_1 1449623400000 1.0 tag_0=val_0 tag_1=val_1\n'
            'put dummy_service.metric.counter_1_count 1449623400000 1 tag_0=val_0 tag_1=val_1\n'
            'put dummy_service.metric.counter_2 1449623400000 2.0 tag_0=val_0 tag_1=val_1\n'
            'put dummy_service.metric.counter_2_count 1449623400000 1 tag_0=val_0 tag_1=val_1\n'
        )
