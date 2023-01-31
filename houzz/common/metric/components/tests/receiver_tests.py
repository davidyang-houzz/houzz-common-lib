#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from six.moves.queue import (
    Empty,
    Full,
)

import itertools
import logging
import time
import unittest
from mock import patch


from houzz.common.metric.components.controller import Controller
from houzz.common.metric.components.receiver import CollectHandler

from houzz.common.metric.components.tests.agent_test_utils import (
    cleanup_metric_client,
    gen_metric_client,
    verify_internal_metrics,
)
from houzz.common.metric.common.tests.common_test_utils import (
    cleanup_mem_disk_queue,
    create_metric,
    get_mem_disk_queue,
)
from houzz.common.thrift_gen.metrics.ttypes import Metrics
from six.moves import range


class ReceiverTest(unittest.TestCase):
    def setUp(self):
        # initialize metric_client
        self.metric_client = gen_metric_client()

        self.read_q = get_mem_disk_queue(
            name='receiver_tests',
            mem_q_size=5,
            mdq_sleep_init=0.1,
            mdq_sleep_max=0.2
        )
        self.controller = Controller()
        self.controller.reset()
        self.handler = CollectHandler(controller=self.controller, read_q=self.read_q)

    def tearDown(self):
        cleanup_metric_client(self.metric_client)
        if self.controller:
            self.controller.shutdown()
        if self.handler:
            cleanup_mem_disk_queue(self.handler.read_q)

    def test_check_metric_to_ignore(self):
        handler = self.handler

        handler.ignored_name_prefixes = ['dummy_service.dummy_metric']
        m = create_metric(int(time.time()), 1, 1)
        self.assertTrue(handler.check_metric_to_ignore(m))
        m = create_metric(int(time.time()), 1, 1, service_name_postfix=1)
        self.assertFalse(handler.check_metric_to_ignore(m))

        handler.ignored_name_prefixes = ['non_match_sn', 'dummy_service.dummy_metric_1']
        m = create_metric(int(time.time()), 1, 1)
        self.assertFalse(handler.check_metric_to_ignore(m))
        m = create_metric(int(time.time()), 1, 1, metric_name_postfix=1)
        self.assertTrue(handler.check_metric_to_ignore(m))

    def test_handler(self):
        handler = self.handler
        handler.ignored_name_prefixes = ['dummy_service.dummy_metric_1']

        for i in range(10):
            handler.log_metrics([
                create_metric(int(time.time()), i * 1.0, i * 2)
            ])
        # add one bad metric with bad key/val
        handler.log_metrics([create_metric(int(time.time()), None, None)])
        # add one bad metric with too many tags
        handler.log_metrics([
            create_metric(int(time.time()), 10, 20, tags=dict((str(i), str(i)) for i in range(7)))
        ])
        # add one good metric without tags
        m = create_metric(int(time.time()), 10, 20)
        m.tags = None
        handler.log_metrics([m])

        # add one bad metric with unicode in metric name
        m = create_metric(int(time.time()), 10, 20, metric_name_postfix=u"中国")
        handler.log_metrics([m])
        # add one metric to be ignored
        m = create_metric(int(time.time()), 10, 20, metric_name_postfix=1)
        handler.log_metrics([m])

        # let entries on disk queue to be discovered
        while not self.handler.read_q.ready_disk_queue_pathes:
            time.sleep(0.1)

        # all queued up successfully, except the unicode one, and the ignored one
        for i in range(10):
            m = handler.read_q.pop_nowait()
            logging.debug('%02d: %s', i, m)
            self.assertIsInstance(m, Metrics)
            self.assertEqual(len(m.metrics), 1)
            self.assertEqual(i * 1.0, m.metrics[0].sum)
            self.assertEqual(i * 2, m.metrics[0].count)
        for i in range(3):
            m = handler.read_q.pop_nowait()
            logging.debug('%02d: %s', i, m)
        self.assertRaises(Empty, handler.read_q.pop_nowait)

        # internal metrics are logged through MetricClient
        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {'dummy_service.handler.metrics_received': 15.0,
             'dummy_service.handler.non_ascii_metrics_dropped': 1.0,
             "dummy_service.handler.received_by_name_group{'group': 'dummy_service.dummy_metric'}":
             13.0,
             "dummy_service.handler.metrics_ignored{'group': 'dummy_service.dummy_metric_0'}":
             1.0,
             'dummy_service.handler.metrics_sent': 13.0},
            ignored_tags=self.metric_client.tags
        )

        self.assertEqual(self.controller.get_stat('handler.metrics_received'), 15)
        self.assertEqual(self.controller.get_stat('handler.non_ascii_metrics_dropped'), 1)
        self.assertEqual(self.controller.get_stat('handler.metrics_ignored'), 1)
        self.assertEqual(self.controller.get_stat('handler.metrics_sent'), 13)

        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_received'),
            {'handler.metrics_received': 15}
        )
        self.assertDictEqual(
            self.handler.get_stat('handler.non_ascii_metrics_dropped'),
            {'handler.non_ascii_metrics_dropped': 1}
        )
        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_ignored'),
            {'handler.metrics_ignored': 1}
        )
        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_sent'),
            {'handler.metrics_sent': 13}
        )

        # reset stats counters
        self.controller.reset_stats()
        self.assertEqual(self.controller.get_stat('handler.metrics_received'), None)
        self.assertEqual(self.controller.get_stat('handler.metrics_ignored'), None)
        self.assertEqual(self.controller.get_stat('handler.non_ascii_metrics_dropped'), None)

        # simulate queuing error
        with patch('houzz.common.metric.client.utils.MemDiskQueue.push') as m_push:
            # NOTE: this mock will also force the MetricClient drop the internal metrics
            m_push.side_effect = Full
            handler.log_metrics([create_metric(int(time.time()), 1.0, 2)])

        self.assertEqual(self.controller.get_stat('handler.metrics_received'), 1)
        self.assertEqual(self.controller.get_stat('handler.metrics_dropped'), 1)

        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_received'),
            {'handler.metrics_received': 1}
        )
        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_dropped'),
            {'handler.metrics_dropped': 1}
        )

    ##################################
    # Tests for new sampling feature #
    ##################################

    def test_no_sampling(self):
        handler = CollectHandler(controller=self.controller, read_q=self.read_q)
        handler.ignored_name_prefixes = ['dummy_service.dummy_metric_1']

        start = time.time()
        for i in range(10000):
            handler.log_metrics([
                create_metric(int(time.time()), i * 1.0, i * 2,
                              metric_name_postfix=i, allow_zero=True)
            ])

        # let entries on disk queue to be discovered
        while not self.handler.read_q.ready_disk_queue_pathes:
            time.sleep(0.1)

        duration = time.time() - start
        logging.debug("no sampling duration: " + str(duration) + "s")

        # all queued up successfully, except the first (zeroth to be exact) one ignored
        for i in range(10000):
            if str(i).startswith('1'):
                continue
            m = handler.read_q.pop()
            logging.debug('%02d: %s', i, m)
            self.assertIsInstance(m, Metrics)
            self.assertEqual(len(m.metrics), 1)
            self.assertEqual(i * 1.0, m.metrics[0].sum)
            self.assertEqual(i * 2, m.metrics[0].count)
        self.assertRaises(Empty, handler.read_q.pop_nowait)

        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {'dummy_service.handler.metrics_received': 10000.0,
             "dummy_service.handler.received_by_name_group{'group': 'dummy_service.dummy_metric_0'}"
             : 8889.0,
             "dummy_service.handler.metrics_ignored{'group': 'dummy_service.dummy_metric_0'}":
             1111.0,
             'dummy_service.handler.metrics_sent': 8889.0},
            ignored_tags=self.metric_client.tags
        )

        self.assertEqual(self.controller.get_stat('handler.metrics_received'), 10000)
        self.assertEqual(self.controller.get_stat('handler.metrics_ignored'), 1111)
        self.assertEqual(self.controller.get_stat('handler.metrics_sent'), 8889)

        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_received'),
            {'handler.metrics_received': 10000}
        )
        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_ignored'),
            {'handler.metrics_ignored': 1111}
        )
        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_sent'),
            {'handler.metrics_sent': 8889}
        )

    def test_sampling_no_metric_dropped(self):
        handler = self.handler

        start = time.time()
        for i in range(10000):
            handler.log_metrics([
                create_metric(int(time.time()), i * 1.0, i * 2)
            ])

        # let entries on disk queue to be discovered
        while not self.handler.read_q.ready_disk_queue_pathes:
            time.sleep(0.1)

        duration = time.time() - start
        logging.debug("sampling duration: " + str(duration) + "s")

        # all queued up successfully, no metric should be dropped
        for i in range(10000):
            m = handler.read_q.pop()
            logging.debug('%02d: %s', i, m)
            self.assertIsInstance(m, Metrics)
            self.assertEqual(len(m.metrics), 1)
            self.assertEqual(i * 1.0, m.metrics[0].sum)
            self.assertEqual(i * 2, m.metrics[0].count)
        self.assertRaises(Empty, handler.read_q.pop_nowait)

        self.assertEqual(self.controller.get_stat('handler.metrics_received'), 10000)
        self.assertEqual(self.controller.get_stat('handler.metrics_sent'), 10000)

        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_received'),
            {'handler.metrics_received': 10000}
        )
        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_sent'),
            {'handler.metrics_sent': 10000}
        )

    def test_bad_sample_rate(self):
        handler = CollectHandler(controller=self.controller, read_q=self.read_q,
                                 name_group_sample_rate=-1)
        for i in range(10):
            handler.log_metrics([
                create_metric(int(time.time()), i * 1.0, i * 2)
            ])

        # let entries on disk queue to be discovered
        while not self.handler.read_q.ready_disk_queue_pathes:
            time.sleep(0.1)

        for i in range(10):
            m = handler.read_q.pop_nowait()
            logging.debug('%02d: %s', i, m)
            self.assertIsInstance(m, Metrics)
            self.assertEqual(len(m.metrics), 1)
            self.assertEqual(i * 1.0, m.metrics[0].sum)
            self.assertEqual(i * 2, m.metrics[0].count)
        self.assertRaises(Empty, handler.read_q.pop_nowait)

        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {'dummy_service.handler.metrics_received': 10,
             "dummy_service.handler.received_by_name_group{'group': 'dummy_service.dummy_metric'}":
             10.0,
             'dummy_service.handler.metrics_sent': 10.0},
            ignored_tags=self.metric_client.tags
        )

        self.assertEqual(self.controller.get_stat('handler.metrics_received'), 10)
        self.assertEqual(self.controller.get_stat('handler.metrics_sent'), 10)

        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_received'),
            {'handler.metrics_received': 10}
        )
        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_sent'),
            {'handler.metrics_sent': 10}
        )

    @unittest.skip("need to be fixed")
    @patch('houzz.common.metric.components.receiver.get_name_group_for_stats')
    def test_sampling(self, m_sub):
        handler = CollectHandler(controller=self.controller, read_q=self.read_q,
                                 name_group_sample_rate=33)
        handler.ignored_name_prefixes = ['dummy_service.dummy_metric_1']
        m_sub.return_value = 'dummy_service.dummy_metric_0'

        with patch('houzz.common.metric.components.receiver.random.random') as m_rnd:
            m_rnd.side_effect = itertools.cycle([0, 0.99, 0.99])
            for i in range(90):
                handler.log_metrics([
                    create_metric(int(time.time()), i * 1.0, i * 2,
                                  metric_name_postfix=j, allow_zero=True)
                    for j in range(10)
                ])

        # re.sub is called 30*10=300 times to report name_group_metrics and
        # called another 60*1=60 times to report ignored metrics from sampled out messages
        self.assertEqual(m_sub.call_count, 360)

        # let entries on disk queue to be discovered
        while not self.handler.read_q.ready_disk_queue_pathes:
            time.sleep(0.1)

        # only one metric in each message should be ignored, in total 90 ignored.
        for i in range(90):
            m = handler.read_q.pop_nowait()
            logging.debug('%02d: %s', i, m)
            self.assertIsInstance(m, Metrics)
            self.assertEqual(len(m.metrics), 9)
            self.assertEqual(i * 1.0, m.metrics[0].sum)
            self.assertEqual(i * 2, m.metrics[0].count)
        self.assertRaises(Empty, handler.read_q.pop_nowait)

        # internal metrics are logged through MetricClient
        verify_internal_metrics(
            self.metric_client.client_queue,
            self.assertDictEqual,
            {'dummy_service.handler.metrics_received': 900.0,
             "dummy_service.handler.received_by_name_group{'group': 'dummy_service.dummy_metric_0'}"
             : 810.0,
             "dummy_service.handler.metrics_ignored{'group': 'dummy_service.dummy_metric_0'}":
             90.0,
             'dummy_service.handler.metrics_sent': 810.0},
            ignored_tags=self.metric_client.tags
        )

        self.assertEqual(self.controller.get_stat('handler.metrics_received'), 900)
        self.assertEqual(self.controller.get_stat('handler.metrics_ignored'), 90)
        self.assertEqual(self.controller.get_stat('handler.metrics_sent'), 810)

        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_received'),
            {'handler.metrics_received': 900}
        )
        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_ignored'),
            {'handler.metrics_ignored': 90}
        )
        self.assertDictEqual(
            self.handler.get_stat('handler.metrics_sent'),
            {'handler.metrics_sent': 810}
        )
