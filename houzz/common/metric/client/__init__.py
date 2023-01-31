#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import threading

from houzz.common.metric.client.client import (
    DummyMetricClient,
    MetricClientFactory
)

__lock = threading.RLock()

def init_metric_client(**kwargs):
    # NOTE: Import MetricPusherFactory inside to prevent circular dependency.
    # Pusher depends on Aggregator and Snapshoter, which are derived from
    # Component. Component includes Client to track its own metrics. Therefore,
    # modules outside metric/client will trigger the circular import if the
    # MetricPusherFactory import is on the module level. There is no reason
    # for Pusher to be known outside the common/metric module. Caller should
    # always use init_metric_client.
    from houzz.common.metric.client.pusher import MetricPusherFactory

    # Allows caller to disable metric related setup and operations. This voids
    # the need to setup metric agent server, and eases local testing.
    if kwargs.get('disable_metrics', False):
        return DummyMetricClient()

    with __lock:
        client = MetricClientFactory._get_metric_client(**kwargs)

        # In multiprocess env, both client_queue and pusher thread need to be reset.
        if MetricPusherFactory._need_reinit():
            MetricClientFactory._reset_client_queue(**kwargs)

        kwargs['metric_queue'] = client.client_queue
        MetricPusherFactory._get_metric_pusher(**kwargs)
        return client
