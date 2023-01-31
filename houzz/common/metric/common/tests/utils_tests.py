#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import time
import unittest
from datetime import datetime
from mock import patch

from houzz.common.metric.common.utils import (
    chunkify,
    datetime_to_mtimestamp,
    get_name_group_for_stats,
    get_sleep_duration,
    merge_tags,
    normalize_timestamp_to_minute,
    validate_metric_in_ascii,
)

from houzz.common.thrift_gen.metrics.ttypes import Metric
from six.moves import range


class UtilsTest(unittest.TestCase):

    def test_get_name_group_for_stats(self):
        self.assertEqual(get_name_group_for_stats('metric', 'aggregator.num_metrics_sent'),
                         'metric.aggregator')
        self.assertEqual(get_name_group_for_stats('epn', 'nl_1017_nlpv'),
                         'epn.nl_0_nlpv')
        self.assertEqual(get_name_group_for_stats('test', 'redis_connect_10379_123.max_time'),
                         'test.redis_connect_0_0')
        self.assertEqual(get_name_group_for_stats(None, 'aggregator.num_metrics_sent'),
                         '.aggregator')
        self.assertEqual(get_name_group_for_stats('metric', None),
                         'metric.')

    @patch('houzz.common.metric.common.utils.datetime')
    def test_get_sleep_duration(self, m_datetime):
        m_datetime.now.return_value = datetime(2015, 11, 16, 1, 2, 3, 500000)
        # catch current minute
        self.assertAlmostEqual(
            11.5,  # 15 - 3.5
            get_sleep_duration(15)
        )
        # missed current minute
        self.assertAlmostEqual(
            58.5,  # 60 - 3.5 + 2
            get_sleep_duration(2)
        )

    def test_normalize_timestamp_to_minute(self):
        ts = datetime_to_mtimestamp(
            datetime(2015, 12, 9, 1, 10, 50, 698152)
        )
        expected = datetime_to_mtimestamp(
            datetime(2015, 12, 9, 1, 10)
        )  # 1449623400000
        self.assertEqual(normalize_timestamp_to_minute(ts), expected)

    def test_merge_tags(self):
        self.assertDictEqual(merge_tags(None, None), {})
        self.assertDictEqual(merge_tags({1: 2}, None), {1: 2})
        self.assertDictEqual(merge_tags(None, {1: 2}), {1: 2})
        self.assertDictEqual(merge_tags({1: 2, 2: 3}, {1: 'a', 3: 4}), {1: 'a', 2: 3, 3: 4})

    def test_chunkify(self):
        def verify_result(src, size_per_chunk, expected):
            self.assertListEqual(
                list(chunkify(src, size_per_chunk)), expected
            )

        self.assertRaises(chunkify(range(3), 0))
        verify_result(range(3), 1, [[0], [1], [2]])
        verify_result(range(3), 2, [[0, 1], [2]])
        verify_result(range(3), 3, [[0, 1, 2]])
        verify_result(range(3), 4, [[0, 1, 2]])

    def test_validate_metric_in_ascii(self):
        # default good metric
        m = Metric(
            name='abc',
            type=1,
            timestamp=int(time.time()) * 1000,
            service_name='def',
            tags={'tag': 'ghi'},
            sum=1,
            count=1,
            annotation='jkl',
            ignore_age=False,
            histogram_buckets={}
        )

        self.assertTrue(validate_metric_in_ascii(m))
        for k in ('service_name', 'name', 'annotation'):
            old = getattr(m, k)
            setattr(m, k, u'中国')
            self.assertFalse(validate_metric_in_ascii(m))
            setattr(m, k, old)
        self.assertTrue(validate_metric_in_ascii(m))

        m.tags['tag'] = u'中国'
        self.assertFalse(validate_metric_in_ascii(m))
        m.tags['tag'] = 'ghi'
        self.assertTrue(validate_metric_in_ascii(m))
        m.tags[u'中国'] = 'ghi'
        self.assertFalse(validate_metric_in_ascii(m))
