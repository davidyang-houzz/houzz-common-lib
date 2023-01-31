#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from six.moves.queue import (
    Empty,
    Full,
    Queue,
)
import logging
import time

from houzz.common.metric.components.component import Component
from houzz.common.metric.common.key_value import (
    KeyValue,
    Snapshot,
)
from houzz.common.thrift_gen.metrics.ttypes import Metrics

# Timeout when pop from the MemDiskQueue populated by the CollectHandler
QUEUE_POP_TIMEOUT_SECONDS = 0.5
# queue capacity of ready_snapshots, which are consumed by Publisher
MAX_NUM_READY_SNAPSHOTS = 1000

logger = logging.getLogger('metric.aggregator')


class Aggregator(Component):

    """Aggregate the incoming metrics to be published to central storage.

    NOTE: There should be just one aggregator.
    """

    def __init__(self, read_q,
                 queue_pop_timeout_seconds=QUEUE_POP_TIMEOUT_SECONDS,
                 max_num_ready_snapshots=MAX_NUM_READY_SNAPSHOTS,
                 **kwargs):
        """Constructor.

        Args:
            read_q (MemDiskQueue): queue holds incoming metrics
        """
        if 'join_timeout' in kwargs:
            join_timeout = kwargs['join_timeout']
        else:
            join_timeout = None
        kwargs['join_timeout'] = join_timeout

        super(Aggregator, self).__init__(**kwargs)
        self.name = kwargs.get('name', 'Aggregator')

        self.read_q = read_q
        self.queue_pop_timeout_seconds = queue_pop_timeout_seconds
        self.snapshot = Snapshot()
        self.ready_snapshots = Queue(max_num_ready_snapshots)
        self.do_snapshot = False
        self.num_metrics_received = 0

    def _publish_snapshot(self):
        if len(self.snapshot):
            # only need to rollover if there is some data
            logger.info('publishing %d metrics from snapshot', len(self.snapshot))

            self.report_counter_metric('aggregator.num_metrics_received',
                                       self.num_metrics_received)
            self.num_metrics_received = 0
            self.snapshot.timestamp = int(time.time()) * 1000  # milliseconds
            try:
                self.ready_snapshots.put_nowait(self.snapshot)
            except Full:
                self.report_counter_metric('aggregator.drop_snapshot', 1)
                logger.error('dropping snapshot with %d items, '
                             'due to full queue',
                             len(self.snapshot))
            else:
                self.report_counter_metric('aggregator.num_snapshot_done', 1)
                self.report_counter_metric('aggregator.num_metrics_sent',
                                           len(self.snapshot))
                logger.debug('added 1 snapshot. Ready snapshots (approximate): %d',
                             self.ready_snapshots.qsize())
            self.snapshot = Snapshot()
        else:
            logger.debug('nothing to be published')
        self.do_snapshot = False

    def run(self):
        while not self.quit_event.is_set():
            # check if we need rollover the snapshot
            if self.do_snapshot:
                self._publish_snapshot()

            try:
                metrics = self.read_q.pop(True, self.queue_pop_timeout_seconds)
                #logger.debug('aggregator popped out: %s', metrics)
            except Empty:
                continue
            except:
                # NOTE: this could happen when the data is not a Metrics object, and
                # stored on disk of the MemDiskQueue. The deserialization will fail.
                logger.exception('failed to pop from read_queue')
                self.report_counter_metric('aggregator.failed_metric_pop', 1)
                continue

            if not isinstance(metrics, Metrics):
                logger.warn('Ignore unknown data from queue. type: %s', type(metrics))
                self.report_counter_metric('aggregator.skipped_unknown_data_in_queue', 1)
                continue
            self.num_metrics_received += len(metrics.metrics)
            for metric in metrics.metrics:
                try:
                    key_val = KeyValue.from_thrift_msg(metric)
                    self.snapshot.accumulate(key_val)
                except:
                    # NOTE: this could happen when the data is not a Metric object, and
                    # stored in memory of the MemDiskQueue, therefore is not filtered
                    # out by the deserialization when pop, but will fail when is converted
                    # to KeyValue.
                    logger.exception('skip aggregating problemetic metric: %s', metric)
                    self.report_counter_metric('aggregator.bad_metric_skipped', 1)

        # In case we have some stats have not been flushed yet
        logger.info('cleaning up before exiting')
        if len(self.snapshot):
            self._publish_snapshot()
