#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
import os
import random
import socket
import threading
import time
from six.moves.queue import Empty

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from houzz.common.metric.client.utils import create_mem_disk_queue
from houzz.common.metric.common.ratelimiting import RateLimiting
from houzz.common.metric.common.utils import chunkify
from houzz.common.metric.common.key_value import (
    gen_metric_from_key_value,
)
from houzz.common.metric.components.aggregator import (
    QUEUE_POP_TIMEOUT_SECONDS,
    MAX_NUM_READY_SNAPSHOTS,
    Aggregator,
)
from houzz.common.metric.components.snapshoter import (
    Snapshoter,
    generate_client_offset,
)
from houzz.common.shutdown_handler import add_shutdown_handler
from houzz.common.thrift_gen.metrics import MetricCollector
from houzz.common.thrift_gen.metrics.ttypes import Metrics

# Timeout for thrift calls
THRIFT_CLIENT_TIMEOUT_SECONDS = 5
# Timeout when check for ready snapshots in MetricPusher.
READY_SNAPSHOTS_QUEUE_POP_TIMEOUT_SECONDS = 0.5
# Timeout when shut down MetricPusher
PUSHER_SHUTDOWN_TIMEOUT_SECONDS = 5
# Maximum number of trials before failing the thrift call
MAX_GET_THRIFT_CLIENT_TRIAL = 3
# Maximum number of metrics to push to metric server in on shot
MAX_NUM_METRICS_PUSH_CHUNK = 2000
# Default rate limit for pushing out metrics: 1 chunk per 0.2 seconds
PUSH_RATE_LIMIT_CALLS = 1
PUSH_RATE_LIMIT_PERIOD_SECONDS = 0.2

logger = logging.getLogger('metric.client.pusher')


def get_thrift_client(host=None, port=None, uds=None):
    if uds:
        socket = TSocket.TSocket(unix_socket=uds)
    else:
        socket = TSocket.TSocket(host, port)
    socket.setTimeout(THRIFT_CLIENT_TIMEOUT_SECONDS * 1000)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = MetricCollector.Client(protocol)
    return client


def close_thrift_client(thrift_client):
    try:
        thrift_client._iprot.trans.close()
    except (TTransport.TTransportException, socket.error) as e:
        logger.exception('ignore exception when close thrift client: %s', e)


class MetricPusher(threading.Thread):

    """Thread to push metrics to Collector periodically."""

    def __init__(self, metric_queue,
                 collector_host=None,
                 collector_port=None,
                 collector_uds=None,
                 offset=None,
                 queue_pop_timeout=QUEUE_POP_TIMEOUT_SECONDS,
                 num_ready_snapshots=MAX_NUM_READY_SNAPSHOTS,
                 disable_snapshot=False,
                 ready_queue_pop_timeout=READY_SNAPSHOTS_QUEUE_POP_TIMEOUT_SECONDS,
                 shutdown_timeout=PUSHER_SHUTDOWN_TIMEOUT_SECONDS,
                 max_thrift_call_trial=MAX_GET_THRIFT_CLIENT_TRIAL,
                 max_push_chunk_size=MAX_NUM_METRICS_PUSH_CHUNK,
                 rate_limit_calls=PUSH_RATE_LIMIT_CALLS,
                 rate_limit_period_seconds=PUSH_RATE_LIMIT_PERIOD_SECONDS,
                 sleep_seconds_init=5.0,
                 sleep_seconds_max=30.0,
                 **kwargs):
        super(MetricPusher, self).__init__()
        self.daemon = True
        self.name = kwargs.get('name', 'MetricPusher')

        self.metric_queue = metric_queue  # MemDiskQueue
        self.aggregator = Aggregator(metric_queue,
                                     queue_pop_timeout_seconds=queue_pop_timeout,
                                     max_num_ready_snapshots=num_ready_snapshots,
                                     name='MetricPusher.Aggregator')
        self.ready_snapshots = self.aggregator.ready_snapshots
        self.aggregator.start()

        self.disable_snapshot = disable_snapshot
        if disable_snapshot:
            self.snapshoter = None
        else:
            offset = offset if offset else generate_client_offset()
            self.snapshoter = Snapshoter(self.aggregator,
                                         offset=offset,
                                         name='MetricPusher.Snapshoter')
            self.snapshoter.start()

        self.queue_pop_timeout = queue_pop_timeout
        self.ready_queue_pop_timeout = ready_queue_pop_timeout
        self.shutdown_timeout = shutdown_timeout
        self.max_thrift_call_trial = max_thrift_call_trial
        self.max_push_chunk_size = max_push_chunk_size

        self.collector_host = collector_host
        self.collector_port = collector_port
        self.collector_uds = collector_uds
        self.black_hole = False
        if not (bool(collector_uds) ^ bool(collector_host and collector_port)):
            logger.error('None of unix_domain_socket or (host, port) has been specified.'
                         'Metrics will be silently DROPPED!')
            self.black_hole = True
        self.use_uds = bool(collector_uds)
        if self.use_uds and not os.path.exists(collector_uds):
            self.use_uds = False
            self.black_hole = True
            logger.error('unix_domain_socket %s does not exist. Metrics will be silently DROPPED!' %
                         collector_uds)

        self._client = None  # cached thrift client
        self.alive = True
        self.num_pushed_metrics = 0
        self.push_rate_limiter = RateLimiting(rate_limit_calls, rate_limit_period_seconds)
        self.sleep_seconds_init = sleep_seconds_init
        self.sleep_seconds_max = sleep_seconds_max
        self.sleep_seconds = sleep_seconds_init

    @property
    def client(self):
        tries = 0
        while True:
            tries += 1
            if self._client and self._client._iprot.trans.isOpen():
                return self._client

            if not self._client:
                self._client = get_thrift_client(self.collector_host,
                                                 self.collector_port,
                                                 self.collector_uds)
            try:
                if not self._client._iprot.trans.isOpen():
                    self._client._iprot.trans.open()
                return self._client
            except (TTransport.TTransportException, socket.error) as e:
                logger.exception('exception in trial no.%d get_client: %s', tries, e)
                self.close_client()
                if tries >= self.max_thrift_call_trial:
                    # raise
                    self.close_client_and_exit()
                else:
                    time.sleep(self.sleep_seconds)
                    self.sleep_seconds = min(
                        self.sleep_seconds_max,
                        self.sleep_seconds * (2.0 + random.random())
                    )

    def close_client(self):
        if self._client:
            close_thrift_client(self._client)
            self._client = None

    def close_client_and_exit(self):
        self.close_client()
        os._exit(1)

    def push_metrics(self, metrics):
        if self.black_hole:
            logger.warn('DROP %d metrics due to unconfigured metric server location', len(metrics))
            return False

        try:
            with self.push_rate_limiter:
                return self.client.log_metrics(metrics)
        except Exception as e:
            # most likely the metric agent server is down, put the metrics back
            # to metric_queue so it can be retried later.
            logger.exception(
                'Put %d metrics back to MemDiskQueue after exception in push_metrics: %s',
                len(metrics), e
            )
            for m in metrics:
                m.ignore_age = True  # It may take a while for the agent server to be back.
            self.metric_queue.push(Metrics(metrics))
            self.close_client()
            return False

    def process_snapshot(self, block=True, timeout=None):
        try:
            snapshot = self.ready_snapshots.get(block, timeout)
        except Empty:
            return False

        if not len(snapshot):
            return False

        # TODO(zheng): Do we need to chunkify messages in snapshot if there are too many rows?
        #              100K rows at once seem to be ok on local vagrant test.

        metrics = [
            gen_metric_from_key_value(key, val) for key, val in snapshot.gen_sorted_key_values()
        ]
        logger.info('MetricPusher collected %d metrics from a new snapshot', len(metrics))
        logger.debug('metrics in snapshot: %s', metrics)
        for chunk in chunkify(metrics, self.max_push_chunk_size):
            if self.push_metrics(chunk):
                # TODO(zheng): push out internal metrics, ex, self.num_pushed_metrics and
                # self.aggregator.num_metrics_received.
                self.num_pushed_metrics += len(chunk)
                logger.info('MetricPusher pushed chunk size: %d', len(chunk))
            logger.info('MetricPusher accumulated num_pushed_metrics: %d', self.num_pushed_metrics)
        return True

    def run(self):
        while self.alive:
            self.process_snapshot(block=True, timeout=self.ready_queue_pop_timeout)

        if self.aggregator and self.aggregator.is_alive():
            self.aggregator.shutdown()

        # in case there are outstanding snapshots
        while self.process_snapshot(block=False):
            logger.info('processed 1 snapshot before stopping.')

        # cleanup
        self.close_client()

    def shutdown(self):
        logger.info('shutting down MetricPusher')
        self.alive = False
        if self.snapshoter and self.snapshoter.is_alive():
            self.snapshoter.shutdown(self.shutdown_timeout)
            logger.info('Snapshoter is down')
        logger.info('waitting for pusher to be down in %s seconds', self.shutdown_timeout)
        self.join(self.shutdown_timeout)


class MetricPusherFactory(object):

    """Factory to generate Singleton MetricPusher."""

    __lock = threading.RLock()
    # The singleton implementation below only applies to the same process. In multiprocessed code,
    # or code explictily call fork, the singleton pusher object will be duplicated, but its thread
    # aspect is not. Its is_alive call returns False, but it will raise if being asked to start
    # again. Therefore we have to store pusher object per process, and re-initialize after forking.
    __metric_pushers = {}  # process id => pusher object

    @classmethod
    def _need_init(cls):
        """Check if the pusher needs to be initialized.
        """
        curr_pid = os.getpid()
        return not (curr_pid in cls.__metric_pushers and cls.__metric_pushers[curr_pid].is_alive())

    @classmethod
    def _need_reinit(cls):
        """Check if the existing pusher needs to be re-initialized.

        This is to handle the multiprocess env, where pusher object exists but not runnable.
        Return True *IFF* the push object exists and it is NOT runnable.
        """
        curr_pid = os.getpid()
        if curr_pid in cls.__metric_pushers and not cls.__metric_pushers[curr_pid].is_alive():
            return True
        return False

    @classmethod
    def _reset_metric_pusher(cls, **kwargs):
        """Create a new MetricPusher, and clean up current MetricPusher if it exists.

        NOTE: used for unittest.
        """
        curr_pid = os.getpid()
        with cls.__lock:
            if cls._need_reinit():
                logger.info('Reset existing MetricPusher for process %d.', curr_pid)
                try:
                    cls.__metric_pushers[curr_pid].shutdown()
                except:
                    logging.exception('Failed to shutdown current pusher. Ignore.')
                del cls.__metric_pushers[curr_pid]
            return cls._get_metric_pusher(**kwargs)

    @classmethod
    def _get_metric_pusher(cls, **kwargs):
        curr_pid = os.getpid()
        with cls.__lock:
            if cls._need_init():
                logger.info('creating new MetricPusher for process %d.', curr_pid)

                if 'metric_queue' in kwargs:
                    metric_queue = kwargs.pop('metric_queue')
                else:
                    metric_queue = create_mem_disk_queue(**kwargs)
                logger.info('using MetricQueue: %s', metric_queue)

                pusher = MetricPusher(
                    metric_queue=metric_queue,
                    **kwargs
                )
                pusher.start()
                # register cleanup
                add_shutdown_handler(pusher.shutdown)
                cls.__metric_pushers[curr_pid] = pusher
            else:
                logger.info('Use existing MetricPusher for process %d.', curr_pid)
            return cls.__metric_pushers[curr_pid]
