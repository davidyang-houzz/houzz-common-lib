#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import copy
import logging
import threading
import time
import random

from houzz.common.metric.client.utils import create_mem_disk_queue
from houzz.common.shutdown_handler import add_shutdown_handler
from houzz.common.thrift_gen.metrics.ttypes import (
    Bucket,
    Metric,
    Metrics,
    MetricType,
)

METRIC_CLIENT_DEFAULT_SERVICE_NAME = 'default'

##################################################################################
#  Service should initialize MetricClient at the beginning of startup, with the  #
#  desired service_name and tags. Due to the singleton nature, the same service  #
#  name and tags will be used by all callers within the same process.            #
##################################################################################

logger = logging.getLogger('metric.client.client')


class DummyMetricClient(object):

    """Dummy client that do nothing, to ease local testing."""

    def __init__(self,
                 service_name=None,
                 tags=None,
                 client_queue=None,
                 **kwargs):
        super(DummyMetricClient, self).__init__()
        self.service_name = service_name or METRIC_CLIENT_DEFAULT_SERVICE_NAME
        self.tags = tags if tags else {}
        self.alive = True
        logger.warn('dummy queue is created. All metrics will be dropped.')

    def set_tags(self, new_tags):
        pass

    def _log_metric(self, key, val, metric_type,
                    timestamp=None, service_name=None, tags=None, in_shutdown=False):
        return True

    def log_counter(self, key, val,
                    timestamp=None, service_name=None, tags=None, in_shutdown=False):
        return True

    def log_gauge(self, key, val,
                  timestamp=None, service_name=None, tags=None, in_shutdown=False):
        return True

    def log_histogram(self, key, val,
                      timestamp=None, service_name=None, tags=None, in_shutdown=False):
        return True

    def shutdown(self):
        self.alive = False


class MetricClient(object):

    """Python client for metric logging.

    NOTE: Depends on MemDiskQueue's thread-safeness.
    """

    def __init__(self,
                 service_name=None,
                 tags=None,
                 client_queue=None,
                 sample_rate=100,
                 **kwargs):
        """Constructor.

        service_name (str): service name for the caller, will be used as prefix all metric names
        tags (dict): tags to be applied on all metrics
        client_queue (MemDiskQueue): if provided, use it instead of creating new MemDiskQueue
        """
        super(MetricClient, self).__init__()
        self.service_name = service_name or METRIC_CLIENT_DEFAULT_SERVICE_NAME
        self.tags = tags if tags else {}
        self.alive = True
        self.sample_rate = sample_rate

        if client_queue:
            self.client_queue = client_queue
        else:
            self.client_queue = create_mem_disk_queue(**kwargs)
        logger.info('metric queue for MetricClient: %s', self.client_queue)

    def set_tags(self, new_tags):
        """Allow user to update the client's default tags.

        This is helpful when the whole set of fixed tags (attached to each metric)
        are not known at the time when the metric client is initialized.
        """
        self.tags = new_tags

    def _log_metric(self, key, val, metric_type,
                    timestamp=None, service_name=None, tags=None, in_shutdown=False):
        """Push metrics to MemDiskQueue.

        Args:
            key (str): metric key
            val (float): metric value
            timestamp (int): milliseconds, if None, use int(time.time()) * 1000
            service_name (str): if provided, override the default service_name. Specify
                                empty string to use key as the sole and full metric identifier.
                                NOTE: None and empty string are treated differently.
            tags (dict): if provided, update the default tags.
            in_shutdown (bool): if True, set ignore_age on the metric
        """
        try:
            # Added additional arg to do sampling at client side
            is_sampled = (self.sample_rate == 100 or random.random() < self.sample_rate / 100.0)
            if not is_sampled:
                return False
            # we need to preserve the metric if either the service or the client
            # in the process of shutting down.
            in_shutdown |= not self.alive
            if timestamp:
                assert timestamp > 1000000000000, 'timestamp needs to be in milliseconds'
                ts = int(timestamp)
            else:
                ts = int(time.time()) * 1000  # align to seconds mark
            sn = service_name if service_name is not None else self.service_name
            if not tags:
                final_tags = self.tags
            else:
                final_tags = copy.copy(self.tags)
                final_tags.update(tags)

            if metric_type == MetricType.HISTOGRAM:
                sum = 0.0
                count = 0
                histogram_buckets = [Bucket(value=float(val), count=1)]
            else:
                sum = float(val)
                count = 1
                histogram_buckets = None

            metric = Metric(
                name=key,
                type=metric_type,
                timestamp=ts,
                service_name=sn,
                tags=final_tags,
                sum=sum,
                count=count,
                histogram_buckets=histogram_buckets,
                ignore_age=in_shutdown
            )

            #logger.debug('_log_metric: %s', metric)
            self.client_queue.push(Metrics([metric]))
        except Exception as e:
            # TODO(zheng): push some counter to self.client_queue
            logger.exception('failed to log_metric with exception: %s.'
                         '{key:%s, val:%s, mt:%s, ts:%s, sn:%s, tags:%s, sd:%s}', e,
                         key, val, metric_type, timestamp, service_name, tags, in_shutdown)
            return False
        return True

    def log_counter(self, key, val,
                    timestamp=None, service_name=None, tags=None, in_shutdown=False):
        """Log counter type metric.

        See detailed parameter documentation in :func:`_log_metric`.
        """
        return self._log_metric(key, val, MetricType.COUNTER, timestamp, service_name, tags,
                                in_shutdown)

    def log_gauge(self, key, val,
                  timestamp=None, service_name=None, tags=None, in_shutdown=False):
        """Log gauge type metric.

        See detailed parameter documentation in :func:`_log_metric`.
        """
        return self._log_metric(key, val, MetricType.GAUGE, timestamp, service_name, tags,
                                in_shutdown)

    def log_histogram(self, key, val,
                      timestamp=None, service_name=None, tags=None, in_shutdown=False):
        """Log histogram type metric.

        See detailed parameter documentation in :func:`_log_metric`.
        """
        return self._log_metric(key, val, MetricType.HISTOGRAM, timestamp, service_name, tags,
                                in_shutdown)

    def shutdown(self):
        self.alive = False
        self.client_queue.close()


class MetricClientFactory(object):

    """Factory to generate Singleton MetricClient."""

    __lock = threading.RLock()
    __metric_client = None

    @classmethod
    def _reset_metric_client(cls, **kwargs):
        """Create a new MetricClient, and clean up current MetricClient if it exists.

        NOTE: used for unittest.
              Do NOT use in multiprocess env because client_queue is still needed by parent.
        """
        with cls.__lock:
            if cls.__metric_client:
                cls.__metric_client.client_queue.close()
                cls.__metric_client = None
            return cls._get_metric_client(**kwargs)

    @classmethod
    def _reset_client_queue(cls, **kwargs):
        """Create new MemDiskQueue for the existing metric client.

        This is to handle the multiprocess env, where child process inherits the metric client
        created in the parent process. The client queue should be reset because the naming of
        the queue ties to the process id.
        """
        client = cls._get_metric_client(**kwargs)
        with cls.__lock:
            logger.info('Recreate MemDiskQueue for existing MetricClient %s.', client)
            client.client_queue = create_mem_disk_queue(**kwargs)

    @classmethod
    def _get_metric_client(cls, **kwargs):
        with cls.__lock:
            if not cls.__metric_client:
                if 'metric_client' in kwargs:
                    # used for dependency injection in unittests
                    logger.info('Use existing MetricClient.')
                    cls.__metric_client = kwargs['metric_client']
                else:
                    logger.info('Create new MetricClient.')
                    cls.__metric_client = MetricClient(**kwargs)
                # register cleanup
                add_shutdown_handler(cls.__metric_client.shutdown)
            return cls.__metric_client
