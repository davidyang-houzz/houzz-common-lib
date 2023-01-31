#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from six.moves.queue import Queue
from collections import defaultdict
import shutil
import tempfile

from houzz.common.mem_disk_queue.mem_disk_queue import MemDiskQueue
from houzz.common.thrift_gen.metrics.ttypes import (
    Metric,
    MetricType,
)
from houzz.common.metric.common.key_value import (
    deserialize_metrics,
    serialize_metrics,
)
from six.moves import range

SERVICE_NAME = 'dummy_service'
METRIC_NAME = 'dummy_metric'
TAG_KEY_PREFIX = 'tag_'
TAG_VAL_PREFIX = 'val_'
NUM_TAGS = 2


def create_tags(num_tags=NUM_TAGS):
    return {
        '%s%s' % (TAG_KEY_PREFIX, i): '%s%s' % (TAG_VAL_PREFIX, i) for i in range(num_tags)
    }


def create_metric(ts, sum, count,
                  service_name_postfix='', metric_name_postfix='',
                  type=MetricType.COUNTER,
                  tags=None, ignore_age=False, histogram_buckets=None, allow_zero=False):
    if tags is None:
        tags = create_tags()

    # If allow_zero is true, postfix "0" is allowed, otherwise, postfix "0" is ignored
    sn = SERVICE_NAME
    if service_name_postfix or (allow_zero and service_name_postfix is not ''):
        sn = '%s_%s' % (sn, service_name_postfix)
    mn = METRIC_NAME
    if metric_name_postfix or (allow_zero and metric_name_postfix is not ''):
        mn = '%s_%s' % (mn, metric_name_postfix)

    return Metric(
        name=mn,
        type=type,
        timestamp=ts,
        service_name=sn,
        tags=tags,
        sum=sum,
        count=count,
        ignore_age=ignore_age,
        histogram_buckets=histogram_buckets
    )


def get_mem_disk_queue(name,
                       mem_q_size=10,
                       mdq_sleep_init=0.1,
                       mdq_sleep_max=0.2):
    working_dir = tempfile.mkdtemp()
    ready_dir = tempfile.mkdtemp()
    q = Queue(mem_q_size)
    mdq = MemDiskQueue(
        program_name=name,
        mem_q_pusher=q.put,
        mem_q_popper=q.get,
        ready_checker=lambda: not q.full(),
        empty_checker=q.empty,
        working_dir=working_dir,
        ready_dir=ready_dir,
        sleep_seconds_init=mdq_sleep_init,
        sleep_seconds_max=mdq_sleep_max,
        persist_mem_q=False,
        serialize_func=serialize_metrics,
        deserialize_func=deserialize_metrics
    )
    return mdq


def cleanup_temp_dir(dir_path, ignore_errors=True):
    shutil.rmtree(dir_path, ignore_errors=True)


def cleanup_mem_disk_queue(mdq):
    mdq.close()
    cleanup_temp_dir(mdq.working_dir)
    cleanup_temp_dir(mdq.ready_dir)


def aggregate_key_values(kvs, ignored_tags=None):
    if ignored_tags is None:
        ignored_tags = {}
    res = defaultdict(float)
    for kv in kvs:
        agg_key = '%s.%s' % (kv.key.service_name, kv.key.metric_name)
        if kv.key.tags:
            tags = dict((i, kv.key.tags[i]) for i in kv.key.tags if i not in ignored_tags)
        else:
            tags = None
        if tags:
            agg_key = '%s%s' % (agg_key, tags)
        res[agg_key] += kv.val.sum
    return res
