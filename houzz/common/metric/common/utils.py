#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import copy
from datetime import datetime
from functools import wraps
import logging
import os
import re
import socket
import stat

from houzz.common.thrift_gen.metrics.ttypes import MetricType
import six

logger = logging.getLogger('metric.utils')

NUMBER_PATTERN = re.compile(r'\d+')
def get_name_group_for_stats(service_name, metric_name):
    """Combine service_name and first component of the metric_name for stats purpose.

    Args:
        service_name (str): service name
        metric_name (str): metric name
    Returns:
        str: name for grouping
    """
    if service_name is None:
        service_name = ''
    if metric_name is None:
        metric_name = ''
    return '%s.%s' % (service_name, re.sub(NUMBER_PATTERN, '0', metric_name.split('.', 1)[0]))


def datetime_to_mtimestamp(dt):
    """Return timestamp in milliseconds.

    Args:
        dt (datetime.datetime): datetime obj
    Returns:
        int: timestamp in milliseconds
    """
    return (dt - datetime(1970, 1, 1)).total_seconds() * 1000

def _normalize_int_timestamp_to_minute(ts):
    """Normalize integer timestamp to the minute mark.

    Result is cached to improve the performance.

    ts(int): source timestamp in milliseconds
    """
    return ts // 60000 * 60000


def normalize_timestamp_to_minute(ts):
    """Align timestamp to the minute mark using floor operation.

    Result is cached to improve the performance.

    ts(int or float): source timestamp in milliseconds
    """
    return _normalize_int_timestamp_to_minute(int(ts))


def get_sleep_duration(offset):
    """Calculate how many seconds to sleep before next action.

    Assume action is done every minute.

    Args:
        offset (float): offset from the full minute mark, in seconds
    Returns:
        type: float, seconds to sleep
    """
    curr = datetime.now()
    curr_seconds = curr.second + curr.microsecond * 1e-6
    if curr_seconds >= offset:
        return 60 - curr_seconds + offset
    else:
        return offset - curr_seconds


def update_socket_permission(socket_path):
    """Make the socket group and world writable."""
    logger.info('change permission on %s', socket_path)
    st = os.stat(socket_path)
    os.chmod(socket_path, st.st_mode | stat.S_IWGRP | stat.S_IWOTH)


def export_metric(obj, metric_type, metric_name, val, tags=None, publish=True):
    """Track metric through controller, and conditionally export through publisher.

    Args:
        obj (object): contains controller and metric_client member variables
        metric_type (MetricType): metric type
        metric_name (str): metric name
        val (float): metric value
        tags (dict): if not None, the new tags to be added to the metric
        publish (bool): if True, export through publisher
    """
    if not getattr(obj, 'controller', None):
        # This obj is not a server component (controlled by controller), ex, the
        # aggregator used within MetricClient. Skip exporting the metrics.
        return

    # we need to set metric.ignore_age if we are in the process of shutdown
    in_shutdown = not obj.controller.alive

    #logger.debug('export_metric: %s - %s', metric_name, val)
    if metric_type == MetricType.COUNTER:
        obj.controller.increment_stat(metric_name, val)
        if publish:
            obj.metric_client.log_counter(metric_name, val, tags=tags, in_shutdown=in_shutdown)
    elif metric_type == MetricType.GAUGE:
        obj.controller.set_stat(metric_name, val)
        if publish:
            obj.metric_client.log_gauge(metric_name, val, tags=tags, in_shutdown=in_shutdown)
    elif metric_type == MetricType.HISTOGRAM:
        # TODO(zheng): how to track histogram stat in controller?
        obj.controller.set_stat(metric_name, val)
        if publish:
            obj.metric_client.log_histogram(metric_name, val, tags=tags, in_shutdown=in_shutdown)
    else:
        raise ValueError('metric type %s is not supported' % metric_type)


def log_func_call(func):
    """Decorator logs the entry and exit points of the function call with parameters."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug('Entering function: %s, args: %s, kwargs: %s',
                     func.__name__, args, kwargs)
        f_result = func(*args, **kwargs)
        logger.debug('Exiting function: %s, result: %r',
                     func.__name__, f_result)
        return f_result

    return wrapper


def merge_tags(tags, tags_update):
    """Generate a new tag set from tags, with update from tags_update."""
    if not isinstance(tags, dict):
        tags = {}
    if not isinstance(tags_update, dict):
        tags_update = {}

    if not tags:
        return tags_update
    if not tags_update:
        return tags

    host_tag = tags.get('host', None)
    new_tags = copy.copy(tags)
    new_tags.update(tags_update)

    if host_tag:
        new_tags['host'] = host_tag
    return new_tags


def validate_metric_in_ascii(metric):
    """Make sure only ASCII used in the metric."""
    def in_ascii(s):
        try:
            s.encode('ascii')
        except UnicodeEncodeError:
            return False
        return True

    for k in ('service_name', 'name', 'annotation'):
        v = getattr(metric, k, None)
        if v and not in_ascii(v):
            return False
    if metric.tags:
        if not isinstance(metric.tags, dict):
            return False
        for k, v in six.iteritems(metric.tags):
            if not (in_ascii(k) and in_ascii(v)):
                return False
    return True


def add_host_tag(tags, override=True):
    """Include FQDN of current host to the tag set."""
    assert isinstance(tags, dict), 'tags need to be dict type but is %s' % type(tags)
    if override or 'host' not in tags:
        tags['host'] = socket.getfqdn()


def chunkify(src, size_per_chunk):
    """Generator to yield chunks with max size of size_per_chunk.

    This chunkify the src consecutively.

    Args:
        src (iterable): data to be chunked
        size_per_chunk (int): max size per chunk
    """
    assert size_per_chunk > 0, 'size_per_chunk need to be positive integer'
    result = []
    for idx, item in enumerate(src):
        result.append(item)
        if (idx + 1) % size_per_chunk == 0:
            yield result
            result = []
    if result:
        yield result
