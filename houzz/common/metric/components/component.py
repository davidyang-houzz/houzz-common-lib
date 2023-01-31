#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
import threading

from houzz.common.thrift_gen.metrics.ttypes import MetricType
from houzz.common.metric.client import MetricClientFactory
from houzz.common.metric.common.utils import export_metric

logger = logging.getLogger('metric.component')


class Component(threading.Thread):

    """Base class for all components running in the metric agent."""

    def __init__(self, join_timeout=None, **kwargs):
        super(Component, self).__init__()
        self.setDaemon(True)
        self.quit_event = threading.Event()
        self.join_timeout = join_timeout
        # NOTE: Avoid using init_metric_client because we may want to delay starting the pusher
        #       thread until the agent server is up.
        self.metric_client = MetricClientFactory._get_metric_client(**kwargs)
        self.controller = None  # will be set when registering with controller

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

    def shutdown(self, join_timeout=None):
        logger.info('shutting down %s', self.name)
        self.quit_event.set()
        timeout = join_timeout if join_timeout else self.join_timeout
        self.join(timeout)

    def run(self):
        raise NotImplementedError
