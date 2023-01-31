#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import unittest

from houzz.common.thrift_gen.metrics.ttypes import (
    Bucket,
    Metric,
    Metrics,
    MetricType,
)
from houzz.common.metric.common.key_value import (
    Key,
    KeyValue,
    Value,
    deserialize,
    deserialize_keyvalue,
    deserialize_metrics,
    gen_metric_from_key_value,
    serialize,
    serialize_metrics,
)
from houzz.common.metric.common.tests.common_test_utils import (
    METRIC_NAME,
    SERVICE_NAME,
    create_metric,
    create_tags,
)
from houzz.common.metric.streamhist import StreamHist
from six.moves import range


class KeyValueTest(unittest.TestCase):
    def setUp(self):
        self.ts = 1449623400 * 1000
        self.metric = create_metric(ts=self.ts, sum=1.0, count=2)
        self.metric_has_histogram = create_metric(
            ts=self.ts, sum=1.0, count=2,
            histogram_buckets=[Bucket(i, i + 1) for i in range(2)])
        self.metric_no_tag = create_metric(ts=self.ts, sum=1.0, count=2, tags={})
        self.metric_too_many_tag = create_metric(ts=self.ts, sum=1.0, count=2,
                                                 tags=dict((i, i) for i in range(7)))
        self.histo_1 = StreamHist(maxbins=100, weighted=False, freeze=None)
        for i in range(2):
            self.histo_1.update(i, i + 1)
        self.histo_2 = StreamHist(maxbins=100, weighted=False, freeze=None)
        for i in range(2, 4):
            self.histo_2.update(i, i + 1)

    def tearDown(self):
        pass

    def test_key(self):
        # shift 10.5 seconds
        ke = Key(SERVICE_NAME, METRIC_NAME, self.ts + 10.5, create_tags(num_tags=6))
        kc = Key.from_thrift_msg(create_metric(ts=self.ts, sum=1.0, count=2,
                                               tags=create_tags(num_tags=6)))

        self.assertEqual('{"mn":"dummy_metric","sn":"dummy_service",'
                         '"t":{"tag_0":"val_0","tag_1":"val_1","tag_2":"val_2","tag_3":"val_3",'
                         '"tag_4":"val_4","tag_5":"val_5"},"ts":1449623400000}',
                         str(ke))
        self.assertEqual(str(ke), str(kc))
        self.assertEqual(hash(ke), hash(kc))
        self.assertEqual(ke, kc)

        self.assertDictEqual(
            {'mn': 'dummy_metric',
             'sn': 'dummy_service',
             'ts': 1449623400000,
             't': {'tag_0': 'val_0',
                   'tag_1': 'val_1',
                   'tag_2': 'val_2',
                   'tag_3': 'val_3',
                   'tag_4': 'val_4',
                   'tag_5': 'val_5',
                   }},
            ke.to_json(),
        )

        json_obj = ke.to_json()
        self.assertEqual(ke, Key.from_json(json_obj))

    def test_key_too_many_tags(self):
        with self.assertRaisesRegexp(ValueError, 'max_num_tags_allowed is 6, 7 given'):
            Key.from_thrift_msg(self.metric_too_many_tag)

    def test_key_no_tag(self):
        ke = Key(SERVICE_NAME, METRIC_NAME, self.ts + 10.5)
        kc = Key.from_thrift_msg(self.metric_no_tag)

        self.assertEqual('{"mn":"dummy_metric","sn":"dummy_service","t":{},"ts":1449623400000}',
                         str(ke))
        self.assertEqual(str(ke), str(kc))
        self.assertEqual(hash(ke), hash(kc))
        self.assertEqual(ke, kc)

        self.assertDictEqual(
            {'mn': 'dummy_metric',
             'sn': 'dummy_service',
             'ts': 1449623400000,
             't': {}},
            ke.to_json(),
        )

        json_obj = ke.to_json()
        self.assertEqual(ke, Key.from_json(json_obj))

    def test_value_no_histogram(self):
        v1 = Value(MetricType.COUNTER, 1.0, 2, False, None)
        vc = Value.from_thrift_msg(self.metric)

        self.assertEqual(v1, vc)

        self.assertDictEqual(
            {'mt': 1,
             's': 1.0,
             'c': 2,
             'i': False,
             'h': None,
             },
            v1.to_json(),
        )

        json_obj = v1.to_json()
        self.assertEqual(v1, Value.from_json(json_obj))

        v2 = Value(MetricType.COUNTER, 2.0, 3, True, None)
        v2.merge(v1)
        self.assertEqual(
            Value(MetricType.COUNTER, 3.0, 5, True, None),
            v2
        )
        self.assertEqual(
            Value(MetricType.COUNTER, 1.0, 2, False, None),
            v1
        )

    def test_value_has_histogram(self):
        v1 = Value(MetricType.COUNTER, 1.0, 2, False, self.histo_1)
        vc = Value.from_thrift_msg(self.metric_has_histogram)

        self.assertEqual(v1, vc)

        self.assertDictEqual(
            {'mt': 1,
             's': 1.0,
             'c': 2,
             'i': False,
             'h': {'bins': [{'count': 1, 'mean': 0}, {'count': 2, 'mean': 1}],
                   'info': {'freeze': None,
                            'min': 0,
                            'max': 1,
                            'total': 3,
                            'maxbins': 100,
                            'missing_count': 0,
                            'weighted': False}},
             },
            v1.to_json(),
        )

        json_obj = v1.to_json()
        self.assertEqual(v1, Value.from_json(json_obj))

        v2 = Value(MetricType.COUNTER, 2.0, 3, True, self.histo_2)
        v2.merge(v1)
        self.assertEqual(
            Value(MetricType.COUNTER, 3.0, 5, True, self.histo_1 + self.histo_2),
            v2
        )
        self.assertEqual(
            Value(MetricType.COUNTER, 1.0, 2, False, self.histo_1),
            v1
        )

    def test_key_value(self):
        k1 = Key(SERVICE_NAME, METRIC_NAME, self.ts + 10.5, create_tags())
        v1 = Value(MetricType.COUNTER, 1.0, 2, False, None)
        kve = KeyValue(k1, v1)
        kvc = KeyValue.from_thrift_msg(self.metric)

        self.assertEqual(kve, kvc)

        self.assertDictEqual(
            {'k': {'mn': 'dummy_metric',
                   'sn': 'dummy_service',
                   'ts': 1449623400000,
                   't': {'tag_0': 'val_0', 'tag_1': 'val_1'}},
             'v': {'c': 2, 'mt': 1, 's': 1.0, 'i': False,
                   'h': None,
                   }},
            kve.to_json(),
        )

        json_obj = kve.to_json()
        self.assertEqual(kve, KeyValue.from_json(json_obj))

    def test_gen_metric_from_key_value(self):
        k1 = Key(SERVICE_NAME, METRIC_NAME, self.ts + 10.5, create_tags())
        v1 = Value(MetricType.COUNTER, 1.0, 2, False, self.histo_1)
        m = gen_metric_from_key_value(k1, v1)
        expected = Metric(
            name='dummy_metric',
            type=1,
            timestamp=self.ts,
            service_name='dummy_service',
            tags={'tag_0': 'val_0', 'tag_1': 'val_1'},
            sum=1.0,
            count=2,
            histogram_buckets=[Bucket(i, i + 1) for i in range(2)],
            annotation=None,
            ignore_age=False,
        )
        self.assertEqual(expected, m)

    def test_keyvalue_serialization(self):
        k1 = Key(SERVICE_NAME, METRIC_NAME, self.ts + 10.5, create_tags())
        v1 = Value(MetricType.COUNTER, 1.0, 2, False, self.histo_1)
        kve = KeyValue(k1, v1)

        self.assertEqual(
            kve,
            deserialize(KeyValue, serialize(kve))
        )

        self.assertEqual(
            kve,
            deserialize_keyvalue(serialize(kve))
        )

    def test_metrics_serialization(self):
        metrics = Metrics([self.metric, self.metric_has_histogram, self.metric_no_tag])
        sm = serialize_metrics(metrics)
        dm = deserialize_metrics(sm)

        self.assertIsInstance(sm, str)
        self.assertEqual(len(dm.metrics), 3)
        self.assertEqual(metrics, dm)
