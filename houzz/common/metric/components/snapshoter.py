#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
import random
import socket

from houzz.common.metric.components.component import Component
from houzz.common.metric.common.utils import get_sleep_duration

# const for setting up the snapshot offset mark within the minute
OFFSET_CLIENT_LOWER_BOUND = 20.0
OFFSET_SERVER_LOWER_BOUND = 30.0
OFFSET_SERVER_UPPER_BOUND = 40.0
MIN_CLIENT_SERVER_OFFSET_GAP = 2.0

logger = logging.getLogger('metric.snapshoter')


def generate_offset(lower, upper, seed=None):
    """Generate offset from the minute mark.

    Args:
        lower (float): lower bound
        upper (float): upper bound
        seed (float or func): random generator seed
    Returns:
        float: offset
    """
    if callable(seed):
        seed = seed()
    random.seed(seed)
    return lower + (upper - lower) * random.random()


def generate_server_offset(lower=OFFSET_SERVER_LOWER_BOUND, upper=OFFSET_SERVER_UPPER_BOUND):
    """Generate offset for snapshoter in agent server.

    NOTE: local hostname is used as deterministic random seed.

    Args:
        lower (float): lower bound
        upper (float): upper bound
    Returns:
        float: offset
    """
    return generate_offset(lower, upper, socket.gethostname())


def generate_client_offset(lower=OFFSET_CLIENT_LOWER_BOUND):
    """Generate offset for snapshoter in metric client.

    Note: The upper bound is defined by the agent server offset. This is to reduce
          the average latency of datapoints in TSDB.

    Args:
        lower (float): lower bound
    Returns:
        float: offset
    """
    return generate_offset(lower, generate_server_offset() - MIN_CLIENT_SERVER_OFFSET_GAP)


class Snapshoter(Component):

    """Make snapshot periodically.

    NOTE: There should be just one snapshoter.
    """

    def __init__(self, aggregator, offset,
                 **kwargs):
        """Constructor.

        Args:
            aggregator (Aggregator): from whom to take snapshot
            offset (int): snapshot time from the minute mark
        """
        super(Snapshoter, self).__init__(**kwargs)
        self.name = kwargs.get('name', 'Snapshoter')

        self.aggregator = aggregator
        self.offset = offset

    def snapshot(self):
        """Force aggregator to start a new snapshot."""
        logger.debug('trigger aggregator to snapshot')
        self.report_counter_metric('snapshoter.num_snapshot_requests', 1)
        self.aggregator.do_snapshot = True

    def run(self):
        """Periodically notify aggregator to start a new snapshot."""
        logger.info('Snapshoter sets offset as %.3f from minute mark', self.offset)
        while not self.quit_event.is_set():
            time_to_offset = get_sleep_duration(self.offset)
            self.quit_event.wait(time_to_offset)
            self.snapshot()
            logger.debug('trigger periodic snapshot')
