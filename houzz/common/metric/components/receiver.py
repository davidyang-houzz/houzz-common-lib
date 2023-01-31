#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
import random

from houzz.common.metric.client import MetricClientFactory
from houzz.common.metric.common.utils import (
    export_metric,
    get_name_group_for_stats,
    merge_tags,
    validate_metric_in_ascii
)
from prometheus_client.core import (
    REGISTRY
)
from prometheus_client import (
    start_http_server
)
from houzz.common.shutdown_handler import add_shutdown_handler
from houzz.common.thrift_gen.metrics.ttypes import (
    Metric,
    Metrics,
    MetricType
)
from houzz.common.metric.common.key_value import (
    KeyValue
)
logger = logging.getLogger('metric.receiver')

class CollectHandler(object):
    """Handler to receive pushed metrics and store them to PrometheusCollector."""

    def __init__(self, controller, prometheus_collector, tags=None, ignored_name_prefixes=None,
                 name_group_sample_rate=100):
        """Constructor.

        controller (Controller): answer stats queries
        tags (dict): tags to be applied on all metrics
        ignored_name_prefixes (list(str)): drop if metric's final name (service name +
                                           metric_name has matching prefix in the list.
        name_group_sample_rate: integer, the rate in percent for sampling received
                                metrics by name group, tune this for expected cpu performance.

        NOTE: TNonblockingServer only create one CollectHandler object and shared
              it across all the threads (default 10). So be careful about thread-safety.
              Current implementation depends on the thread-safeness of Controller.
              If you ever need to maintain stateful counters, add locking.

              CollectHandler is very similar to Component, except its not subclass of
              threading.Thread.
        """
        super(CollectHandler, self).__init__()

        self.controller = controller
        # NOTE: Avoid using init_metric_client because we want to delay starting the pusher
        #       thread until the agent server is up.
        self.metric_client = MetricClientFactory._get_metric_client()

        self.tags = tags if tags else {}
        self.ignored_name_prefixes = ignored_name_prefixes if ignored_name_prefixes else []
        # set sampling rate to given if sampling rate is valid, otherwise don't sample
        # this is intended to improve cpu performance.
        if isinstance(name_group_sample_rate, int) and 0 <= name_group_sample_rate < 100:
            self.name_group_sample_rate = name_group_sample_rate
        else:
            self.name_group_sample_rate = 100

        self.prometheus_collector = prometheus_collector
        logger.info('receiver.name_group_sample_rate: {}'.format(self.name_group_sample_rate))
        REGISTRY.register(self.prometheus_collector)
        start_http_server(6319) # Start Prometheus server

    def report_metric(self, metric_type, metric_name, val, tags=None, publish=True):
        """Track metric through controller, and conditionally export through publisher.

        Args:
            metric_type (MetricType): metric type
            metric_name (str): metric name
            val (float): metric value
            tags (dict): if not None, the new tags to be added to the metric
            publish (bool): if True, export through publisher
        """
        export_metric(self, metric_type, metric_name, val, tags=tags, publish=publish)

    def report_counter_metric(self, metric_name, val, tags=None, publish=True):
        self.report_metric(MetricType.COUNTER, metric_name, val, tags, publish)

    def report_gauge_metric(self, metric_name, val, tags=None, publish=True):
        self.report_metric(MetricType.GAUGE, metric_name, val, tags, publish)

    def report_histogram_metric(self, metric_name, val, tags=None, publish=True):
        self.report_metric(MetricType.HISTOGRAM, metric_name, val, tags, publish)

    def check_metric_to_ignore(self, metric):
        """Check if the metric naming match any ignored_name_prefixes.

        Args:
            metric (Metric): metric to check

        Returns:
            bool: True if the metric should be ignored.
        """
        sn = '' if metric.service_name is None else metric.service_name
        mn = '' if metric.name is None else metric.name
        fn = '%s.%s' % (sn, mn)
        for prefix in self.ignored_name_prefixes:
            if fn.startswith(prefix):
                return True
        return False

    def _log_metrics(self, metrics):
        """Log the received metrics.

        Args:
            metrics (list(Metric)): metrics to log
        Returns:
            int: number of metrics sent to backend
        """
        logger.debug('metrics received: %s', metrics)
        if not metrics:
            return 0

        validated_metrics = []
        report_name_group = (self.name_group_sample_rate == 100 or
                             random.random() < self.name_group_sample_rate / 100.0)
        try:
            for m in metrics:
                if validate_metric_in_ascii(m):
                    if not self.check_metric_to_ignore(m):
                        m.tags = merge_tags(m.tags, self.tags)
                        tag_len = len(m.tags)
                        if m.service_name:
                            metric_name = '%s.%s' % (m.service_name, m.name)
                        else:
                            metric_name = m.name
                        try:
                            # We will check if ValueError: max_num_tags_allowed hits the max (10)
                            key_val = KeyValue.from_thrift_msg(m)
                            try:
                                # We build snapshot for Prometheus
                                self.prometheus_collector.snapshot_accumulate(key_val)
                                # For debugging purpose
                                # logger.info('key:%s, value:%s', key_val.key, key_val.val)
                            except:
                                logger.exception(
                                    'Skip metric failed to build snapshot: %s',m)
                                self.report_counter_metric(
                                    'handler.snapshot_failed', 1,
                                    tags={'metric_name': metric_name, 'tag_len': tag_len}
                                )
                            validated_metrics.append(m)
                            if report_name_group:
                                self.report_counter_metric(
                                    'handler.received_by_name_group', 100 / self.name_group_sample_rate,
                                    tags={'group': get_name_group_for_stats(m.service_name, m.name)}
                                )
                        except:
                            logger.exception(
                                'Skip metric failed to read from thrift message: %s',m)
                            self.report_counter_metric(
                                'handler.failed_from_thrift_msg', 1,
                                tags={'metric_name': metric_name, 'tag_len': tag_len}
                            )
                    else:
                        self.report_counter_metric(
                            'handler.metrics_ignored', 1,
                            tags={'group': get_name_group_for_stats(m.service_name, m.name)}
                        )
                else:
                    logger.warn('skip non-ascii metric: %s', m)
                    self.report_counter_metric('handler.non_ascii_metrics_dropped', 1)
        except Exception as e:
            # this should happen rarely, so logger should not be a concern
            logger.error('failed to push metric with exception: %s', e)
            self.report_counter_metric('handler.metrics_dropped', len(metrics))
            return 0

        num_metrics = len(validated_metrics)
        self.report_counter_metric('handler.metrics_sent', num_metrics)
        logger.debug('CollectHandler sent %d rows', num_metrics)

        return num_metrics

    def log_metrics_oneway(self, metrics):
        """Endpoint to log the received metrics without returning result to client.

        Args:
            metrics (list(Metric)): metrics to log
        """
        self.report_counter_metric('handler.metrics_received_oneway', len(metrics))
        self._log_metrics(metrics)

    def log_metrics(self, metrics):
        """Endpoint to log the received metrics and return result to client.

        Args:
            metrics (list(Metric)): metrics to log
        Returns:
            bool: True iff all metrics are logged successfully
        """
        self.report_counter_metric('handler.metrics_received', len(metrics))
        num_metrics_sent = self._log_metrics(metrics)
        return num_metrics_sent == len(metrics)

    def get_stat(self, stat_name):
        """Endpoint to check stat about the agent.

        Args:
            names (str): stat name to poll
        Returns:
            result (dict(string => float)): polled stat. Only contains existing stats
        """
        stat_val = self.controller.get_stat(stat_name)
        return {} if stat_val is None else {stat_name: stat_val}

    def get_all_stats(self):
        """Endpoint to check all stats about the agent.

        Returns:
            result (dict(string => float)): polled stats.
        """
        return self.controller.get_all_stats()

    def reset_stats(self):
        """Endpoint to reset all agent stats.

        Returns:
            Success (bool): if the call is successful
        """
        return self.controller.reset_stats()

    def snapshot(self):
        """Endpoint to force a snapshot of the collected metrics.

        Returns:
            Success (bool): if the call is successful
        """
        return self.controller.snapshot()

    def shutdown(self, join_timeout):
        """Endpoint to shutdown the agent gracefully.

        Args:
            join_timeout (float): join timeout in seconds
        Returns:
            Success (bool): if the call is successful
        """
        return self.controller.shutdown(join_timeout)

    def get_dryrun_out(self):
        """Endpoint to get publisher's output in dryrun mode.

        NOTE: Only intended to be used in unittests.

        Returns:
            out (str): printout, empty if it is not dryrun mode or not using StringIO for output
        """
        return self.controller.get_dryrun_out()

    def reset_dryrun_out(self):
        """Endpoint to reset publisher's output buffer in dryrun mode.

        NOTE: Only intended to be used in unittests.

        Returns:
            Success (bool): True if the StringIO buffer is truncated
        """
        return self.controller.reset_dryrun_out()
