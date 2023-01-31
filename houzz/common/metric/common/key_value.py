#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from functools import partial
import logging
import json

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

from houzz.common.thrift_gen.metrics.ttypes import (
    Bucket,
    Metric,
    Metrics,
)
from houzz.common.metric.common.utils import (
    normalize_timestamp_to_minute,
)
from houzz.common.metric.streamhist import StreamHist
import six

# Since Prometheus could handle more labels, we increase the tag # to 10
MAX_NUM_TAGS_ALLOWED = 10
# To get 99%, we need 100 bins
MAX_NUM_HISTOGRAM_BINS = 100

logger = logging.getLogger('metric.key_value')


class Key(object):

    """Key within metric snapshot, Need to be sortable and hashable."""

    SERVICE_NAME = 'sn'
    METRIC_NAME = 'mn'
    TIMESTAMP = 'ts'
    TAGS = 't'

    FIELD_LIST = sorted([SERVICE_NAME, METRIC_NAME, TIMESTAMP, TAGS])

    def __init__(self, service_name, metric_name, timestamp, tags=None):
        """Constructor. Align the metric timestamp to the minute mark.

        NOTE: timestamp is in milliseconds.
        """
        if tags:
            assert isinstance(tags, dict), 'tags need to be dict'
            if len(tags) > MAX_NUM_TAGS_ALLOWED:
                raise ValueError(
                    'max_num_tags_allowed is %d, %d given' % (
                        MAX_NUM_TAGS_ALLOWED, len(tags)
                    )
                )

        self.service_name = service_name
        self.metric_name = metric_name
        self.timestamp = normalize_timestamp_to_minute(timestamp)
        self.tags = tags if tags else {}

    @classmethod
    def from_thrift_msg(cls, metric_msg):
        return Key(
            metric_msg.service_name,
            metric_msg.name,
            metric_msg.timestamp,
            metric_msg.tags
        )

    def __str__(self):
        return serialize(self, sort_keys=True)

    def __lt__(self, other):
        return str(self) < str(other)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return str(self) == str(other)

    def to_json(self):
        return {
            self.SERVICE_NAME: self.service_name,
            self.METRIC_NAME: self.metric_name,
            self.TIMESTAMP: self.timestamp,
            self.TAGS: self.tags,
        }

    @classmethod
    def from_json(cls, json_obj):
        assert (isinstance(json_obj, dict) and
                sorted(json_obj) == cls.FIELD_LIST), repr(json_obj)
        return Key(
            json_obj[cls.SERVICE_NAME],
            json_obj[cls.METRIC_NAME],
            json_obj[cls.TIMESTAMP],
            json_obj[cls.TAGS]
        )


class Value(object):

    """Value within metric snapshot."""

    METRIC_TYPE = 'mt'
    SUM = 's'
    COUNT = 'c'
    IGNORE_AGE = 'i'
    HISTOGRAM_BUCKETS = 'h'

    FIELD_LIST = sorted([METRIC_TYPE, SUM, COUNT, IGNORE_AGE, HISTOGRAM_BUCKETS])

    def __init__(self, metric_type, sum, count, ignore_age, histogram_buckets):
        assert isinstance(sum, six.integer_types + (float,)) and \
               isinstance(count, six.integer_types + (float,))
        self.metric_type = metric_type
        self.sum = sum
        self.count = count
        self.ignore_age = ignore_age
        if not histogram_buckets:
            # optimization: use None instead of empty histogram object since
            # most metrics are not histogram metrics
            self.histogram_buckets = None
        elif isinstance(histogram_buckets, list):
            # list of Metric Buckets
            assert isinstance(histogram_buckets[0], Bucket)
            self.histogram_buckets = StreamHist(maxbins=MAX_NUM_HISTOGRAM_BINS)
            for b in histogram_buckets:
                self.histogram_buckets.insert(b.value, b.count)
            self.histogram_buckets.trim()
        elif isinstance(histogram_buckets, StreamHist):
            # make a deep copy
            self.histogram_buckets = histogram_buckets.copy()
        else:
            raise ValueError('wrong type for histogram_buckets')

    def __eq__(self, other):
        return (
            self.metric_type == other.metric_type and
            self.sum == other.sum and
            self.count == other.count and
            self.ignore_age == other.ignore_age and
            str(self.histogram_buckets) == str(other.histogram_buckets)
        )

    def __str__(self):
        return serialize(self)

    @classmethod
    def from_thrift_msg(cls, metric_msg):
        return Value(
            metric_msg.type,
            metric_msg.sum,
            metric_msg.count,
            metric_msg.ignore_age,
            metric_msg.histogram_buckets
        )

    @classmethod
    def copy(cls, other):
        return Value(other.metric_type, other.sum, other.count, other.ignore_age,
                     other.histogram_buckets)

    def merge(self, other, key_str=""):
        if self.metric_type != other.metric_type:
            # NOTE: if you ever change metric_type for a given metric, you better
            # rename it, or you will lose some stats.
            logger.error('IGNORE merging incompatible metric %s types %s and %s',
                         key_str, self.metric_type, other.metric_type)
            return
        self.sum += other.sum
        self.count += other.count
        # use OR maybe over generous. Because of timestamp in the key, this should
        # be for rare cases.
        self.ignore_age |= other.ignore_age
        if self.histogram_buckets and other.histogram_buckets:
            self.histogram_buckets.merge(other.histogram_buckets)
        elif other.histogram_buckets:
            self.histogram_buckets = other.histogram_buckets.copy()

    def to_json(self):
        return {
            self.METRIC_TYPE: self.metric_type,
            self.SUM: self.sum,
            self.COUNT: self.count,
            self.IGNORE_AGE: self.ignore_age,
            self.HISTOGRAM_BUCKETS: (self.histogram_buckets.to_dict() if
                                     isinstance(self.histogram_buckets, StreamHist) else None),
        }

    @classmethod
    def from_json(cls, json_obj):
        assert (isinstance(json_obj, dict) and
                sorted(json_obj) == cls.FIELD_LIST), repr(json_obj)
        histogram_buckets = StreamHist.from_dict(
            json_obj[cls.HISTOGRAM_BUCKETS]) if json_obj[cls.HISTOGRAM_BUCKETS] else None
        return Value(
            json_obj[cls.METRIC_TYPE],
            json_obj[cls.SUM],
            json_obj[cls.COUNT],
            json_obj[cls.IGNORE_AGE],
            histogram_buckets
        )


class KeyValue(object):

    """Serializable object to hold Key and Value objects."""

    KEY = 'k'
    VALUE = 'v'

    FIELD_LIST = sorted([KEY, VALUE])

    def __init__(self, key, val):
        self.key = key
        self.val = val

    def __eq__(self, other):
        return (
            self.key == other.key and
            self.val == other.val
        )

    def __str__(self):
        return serialize(self)

    @classmethod
    def from_thrift_msg(cls, metric_msg):
        return KeyValue(
            key=Key.from_thrift_msg(metric_msg),
            val=Value.from_thrift_msg(metric_msg),
        )

    def to_json(self):
        return {
            self.KEY: self.key.to_json(),
            self.VALUE: self.val.to_json()
        }

    @classmethod
    def from_json(cls, json_obj):
        assert (isinstance(json_obj, dict) and
                sorted(json_obj) == cls.FIELD_LIST), repr(json_obj)
        return KeyValue(
            key=Key.from_json(json_obj[cls.KEY]),
            val=Value.from_json(json_obj[cls.VALUE])
        )


def gen_metric_from_key_value(key, val):
    if not val.histogram_buckets:
        histogram_buckets = None
    else:
        # TODO(zheng): add more thrift fields to store min/max info
        histogram_buckets = [Bucket(value=b.value, count=b.count) for b in
                             val.histogram_buckets.bins]
    return Metric(
        name=key.metric_name,
        type=val.metric_type,
        timestamp=key.timestamp,
        service_name=key.service_name,
        tags=key.tags,
        sum=val.sum,
        count=val.count,
        histogram_buckets=histogram_buckets,
        ignore_age=val.ignore_age,
    )


def serialize(obj, sort_keys=False):
    """Serialize obj to str.

    NOTE: sort_keys will use much more CPU than unsorted_keys.
          Only set it if necessary, ie, use as dict key.

    Args:
        obj (object): object to be serialized
        sort_keys (bool): if the key should be sorted in the output
    Returns:
        type: str
    """
    if isinstance(obj, six.string_types):
        return obj
    return json.dumps(obj.to_json(), sort_keys=sort_keys, separators=(',', ':'))


def deserialize(cls, json_str):
    if isinstance(json_str, cls):
        return json_str
    return cls.from_json(json.loads(json_str))


def deserialize_keyvalue(json_str):
    return partial(deserialize, KeyValue)(json_str)


def serialize_metrics(obj):
    if isinstance(obj, six.string_types):
        return obj
    trans_out = TTransport.TMemoryBuffer()
    proto_out = TBinaryProtocol.TBinaryProtocolAccelerated(trans_out)
    obj.write(proto_out)
    return trans_out.getvalue()


def deserialize_metrics(m_str):
    if isinstance(m_str, Metrics):
        return m_str
    trans_in = TTransport.TMemoryBuffer(m_str)
    proto_in = TBinaryProtocol.TBinaryProtocolAccelerated(trans_in)
    metrics = Metrics()
    metrics.read(proto_in)
    return metrics


class Snapshot(object):

    """Holds the periodic aggregated results to be published.

    NOTE: Not thread-safe. Client is reponsible to guarantee
          one snapshot is handled by one thread only.
    """

    def __init__(self):
        self._data = {}  # str(Key) => Value
        self.timestamp = None

    def __len__(self):
        return len(self._data)

    def __str__(self):
        return ',\n'.join(
            serialize(KeyValue(deserialize(Key, k), v)) for k, v in six.iteritems(self._data)
        )

    def accumulate(self, key_val):
        """Assume accessed by single thread."""
        assert isinstance(key_val, KeyValue), 'wrong type to accumulate - %s' % (type(key_val))
        key_str = str(key_val.key)
        if key_str in self._data:
            #logger.debug('merging: %s %s', key_val.key, key_val.val)
            self._data[key_str].merge(key_val.val, key_str)
        else:
            #logger.debug('adding: %s %s', key_val.key, key_val.val)
            self._data[key_str] = key_val.val

    def gen_key_values(self):
        """Generator to yield (key, val) stored in the snapshot."""
        for k in six.iterkeys(self._data):
            yield (deserialize(Key, k), self._data[k])

    def gen_sorted_key_values(self):
        """Generator to yield sorted (key, val) stored in the snapshot."""
        for k in sorted(six.iterkeys(self._data)):
            yield (deserialize(Key, k), self._data[k])
