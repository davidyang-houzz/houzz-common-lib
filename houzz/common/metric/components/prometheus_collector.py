#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
import six
import re
import json
import time
import random
from houzz.common.metric.components.component import Component
from houzz.common.thrift_gen.metrics.ttypes import (
    MetricType,
)
from houzz.common.metric.common.utils import (
    get_name_group_for_stats,
)
from prometheus_client.core import (
    GaugeMetricFamily,
)
from houzz.common.metric.common.key_value import (
    Key,
    Snapshot,
)
logger = logging.getLogger('metric.prom_collector')
# List of percentiles tracked by the histogram metrics
HISTOGRAM_PERCENTILES = [0.5, 0.9, 0.95]
# Allow 5 mins delay for Prometheus
MAX_ALLOWED_METRIC_DELAY_SECONDS_PROMETHEUS = 5 * 60

class PrometheusCollector(Component):
    def __init__(self, **kwargs):
        super(PrometheusCollector, self).__init__(**kwargs)
        self.gauge_dict = {}
        self.snapshot = Snapshot()

    def snapshot_accumulate(self, key_val):
        if len(self.snapshot) < 100000:
            self.snapshot.accumulate(key_val)
        else:
            logger.exception('Max snapshot limit hit. Skip metric key:%s, value:%s', key_val.key, key_val.val)

    def get_gauge_dict(self):
        return self.gauge_dict

    def set_gauge_dict(self, k, g):
        self.gauge_dict[k] = g

    def deserialize(self, cls, json_str):
        if isinstance(json_str, cls):
            return json_str
        return cls.from_json(json.loads(json_str))

    def gen_sorted_key_values(self, data):
            """Generator to yield sorted (key, val) stored in the snapshot."""
            for k in sorted(six.iterkeys(data)):
                yield (self.deserialize(Key, k), data[k])

    def collect(self):
        # Make a dict copy
        snapshot_copy = self.snapshot._data.copy()
        self.snapshot = Snapshot()
        if not len(snapshot_copy):
            return
        try:
            for key, val in self.gen_sorted_key_values(snapshot_copy):
                try:
                    self.build_metric_lines(key=key, value=val)
                except:
                    logger.exception('Skip metric failed to build metric line. key:%s, value:%s', key, val)
        except:
            logger.exception('Failed to process snapshot. Ignore')
        gauge_copy = self.gauge_dict.copy()
        # Clear the dict right away to avoid RuntimeError: dictionary changed size during iteration
        self.gauge_dict.clear()
        local_dict = {}
        # Interate thru copy dict to yield data to Prometheus request
        for k in gauge_copy:
            g = gauge_copy[k]
            if g.get_name_label_str() not in local_dict:
                local_dict[g.get_name_label_str()] = []
            local_dict[g.get_name_label_str()].append([g.get_name(), g.get_labels(), g.get_label_values(), g.get_value()])
            if k == 'payment_service_get_payment_methods_success':
                logger.info('payment_service_get_payment_methods_success' + g.get_value())
        logger.info('PrometheusCollector: %s', len(local_dict))
#         logger.info(local_dict)
        for k in local_dict:
            gmf = GaugeMetricFamily(local_dict[k][0][0], local_dict[k][0][0], labels=local_dict[k][0][1])
            for data in local_dict[k]:
                # g = GaugeMetricFamily(prometheus_metric_name, 'Help text', value=7, labels=label_names)
                # add_metric(label_values, value, timestamp=None): // timestamp in seconds
                gmf.add_metric(data[2], data[3])
            yield gmf

    def build_metric_lines(self, key, value):
        if key.service_name:
            metric_name = '%s.%s' % (key.service_name, key.metric_name)
        else:
            metric_name = key.metric_name
        metric_name.replace('"', '')
        label_names = []
        label_values = []
        if key.tags:
            # CINF-659 Check whether a tag name starts with number. If so, try to fix it and push metrics to TSDB for alerting
            r = re.compile("^[0-9]?$")
            num_list = sorted(list(filter(r.match, key.tags.keys())))
            if len(num_list) > 0:
                tags_dict = {}
                for i in range(0, len(num_list), 2):
                    tags_dict[key.tags[str(i)]] = key.tags[str(i+1)]
                for k in key.tags.keys():
                    if k not in num_list:
                        tags_dict[k] = key.tags[k]
                label_original = sorted(tags_dict.keys())
                label_names = [k.replace('"', '') for k in sorted(tags_dict.keys())]
                label_values = [tags_dict[k].replace('"', '') for k in label_original]
                logger.info('prometheus_collector_encoding_error: %s - %s', metric_name, ''.join(key.tags.keys()))
                self.report_counter_metric('prometheus_collector_encoding_error', 1, {'metric_name': metric_name})
            else:
                label_original = sorted(key.tags.keys())
                label_names = [k.replace('"', '') for k in sorted(key.tags.keys())]
                label_values = [key.tags[k].replace('"', '') for k in label_original]

        def add_line(mn, val, cnt=1, metric_type = None):
            # Remove any string between [xxxx] or (xxxx)
            # prometheus_metric_name = re.sub("[\(\[].*?[\)\]]", "", mn)
            # Replace ()[]{}/?=.-| with underscore(_)
            prometheus_metric_name = re.sub("[\(\[\)\]\{\}/\?=\.|-]", "_", mn)
            # Concatenate all underscores with just one underscore
            prometheus_metric_name = re.sub(r'[_]+', '_', prometheus_metric_name)
            prometheus_key = prometheus_metric_name + ' '.join(label_names) + ' '.join(label_values)
            curr_ts = int(time.time())
            metric_ts = key.timestamp/1000.000
            time_delta = curr_ts - metric_ts
            # Don't process metrics older than MAX_ALLOWED_METRIC_DELAY_SECONDS_PROMETHEUS (default 5 mins)
            if time_delta > MAX_ALLOWED_METRIC_DELAY_SECONDS_PROMETHEUS:
                self.report_gauge_metric('prometheus_collector_drop_old_prometheus_metrics_time', time_delta)
#                 logger.warning('Dropping: %s, delta: %d', prometheus_metric_name, time_delta)
                return
            if prometheus_key not in self.get_gauge_dict():
                # logger.info('prometheus_metric_name: ' + prometheus_metric_name)
                # logger.info('label_name: ' + ' '.join(label_names))
                if (len(self.get_gauge_dict()) < 100000):
                    g = CustomGaugeMetric(prometheus_metric_name, value=val, count=cnt, labels=label_names, label_values=label_values, ts=metric_ts, metric_type=metric_type)
                    self.set_gauge_dict(prometheus_key, g)
                else:
                    logger.warning('Dropping: %s', prometheus_metric_name)
            else:
                g = self.get_gauge_dict()[prometheus_key]
                g.add_value_and_count(val, cnt)
                # Don't need this line anymore since we move it to yield part in CustomCollector class
                # g.add_metric(labels=label_values, value=val)

        if value.metric_type == MetricType.COUNTER:
            # The sum of count within 30sec interval. This metric is suitable for sum(sum_over_time()) aggregation.
            add_line(metric_name, value.sum, cnt=1, metric_type=MetricType.COUNTER)
            if random.random() < 0.01:
                # The avg of count within 30 sec interval. This metric is suitable for avg(avg_over_time()) or quantile query.
                add_line(metric_name + '_avg', value.sum)
        elif value.metric_type == MetricType.GAUGE:
            # take average for gauge type
            if value.count:
                add_line(metric_name, value.sum / value.count, cnt=value.count)
            # always output an additional metric line for value.count, so later we can
            # do weighted average on gauge metrics when aggregate them.
            add_line(metric_name + '_count', value.count, cnt=1, metric_type=MetricType.COUNTER)
        elif value.metric_type == MetricType.HISTOGRAM:
            try:
                hb = value.histogram_buckets
                for p, v in zip(HISTOGRAM_PERCENTILES, hb.quantiles(*HISTOGRAM_PERCENTILES)):
                    add_line('%s.p%s' % (metric_name, int(p * 100)), v)
                # Required for giving counts for every bucket
                add_line(metric_name + '_count', hb.count(), cnt=1, metric_type=MetricType.COUNTER)
                # We sample max and mean by sampling
                if random.random() < 0.01:
                    add_line(metric_name + '_max', hb.max())
                    add_line(metric_name + '_mean', hb.mean())
            except:
                self.report_counter_metric('prometheus_collector_failed_histogram', 1)
                raise
        else:
            logger.error('Skip unknown metric type %s', value.metric_type)


class CustomGaugeMetric(object):
    def __init__(self, name, value, count, labels, label_values, ts, metric_type = None):
        self.name = name
        self.value = value
        self.labels = labels
        self.label_values = label_values
        self.count = count
        self.metric_type = metric_type
        self.ts = ts
        self.name_label_str = name + ''.join(labels)

    def add_value(self, value):
        self.value = self.value + value
        self.count = self.count + 1

    def add_value_and_count(self, value, count):
        self.value = self.value + value
        self.count = self.count + count

    def get_name(self):
        return self.name

    def get_value(self):
        suffix = "_count"
        # If it's count related metric, we return the sum of the all count values; otherwise we return the average data
        if self.metric_type == MetricType.COUNTER:
            return self.value
        else:
            return self.value / self.count

    def get_labels(self):
        return self.labels

    def get_label_values(self):
        return self.label_values

    def get_timestamp(self):
        return self.ts

    def get_name_label_str(self):
        return self.name_label_str
