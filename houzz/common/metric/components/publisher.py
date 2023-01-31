#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
import random
import socket
import sys
import time
import copy
import six
import re
from six.moves.queue import Empty
from collections import OrderedDict
from houzz.common.metric.client.utils import create_mem_disk_queue
from houzz.common.metric.common.ratelimiting import RateLimiting
from houzz.common.metric.components.component import Component
from houzz.common.metric.common.config import (
    AGENT_MEM_DISK_QUEUE_WORKING_DIR,
    AGENT_MEM_DISK_QUEUE_READY_DIR,
    AGENT_MEM_DISK_QUEUE_PROGRAM_NAME
)
from houzz.common.metric.common.key_value import gen_metric_from_key_value
from houzz.common.metric.common.utils import (
    chunkify,
    get_name_group_for_stats,
)
from houzz.common.thrift_gen.metrics.ttypes import (
    Metrics,
    MetricType,
)

from six.moves import zip

# Timeout when check for ready snapshots.
READY_SNAPSHOTS_QUEUE_POP_TIMEOUT_SECONDS = 0.5
# Number of seconds after which the connection to the TSD hostname reconnects
# itself. A value of 0 means stay connected, and never reconnect.
TSD_RECONNECT_INTERVAL = 0
# Number of seconds after connection idling to verify the socket liveness
TSD_SOCKET_VERIFICATION_INTERVAL = 60
# Allow metric to arrive at most 7 days late
MAX_ALLOWED_METRIC_DELAY_MILLISECONDS = 7 * 86400 * 1000

# Maximum number of metrics to publish to TSD in on shot
MAX_NUM_METRICS_PUBLISH_CHUNK = 2000
# Maximum number of trials before failing the TSD data send
MAX_TSD_SEND_TRIAL = 3
# Maximum number of seconds before retry failed TSD connections
MAX_CONNECTION_RETRY_DELAY_SECONDS = 15
# List of percentiles tracked by the histogram metrics
HISTOGRAM_PERCENTILES = [0.5, 0.95]
# Default rate limit for publishing metrics: 1 chunk per 0.2 seconds
# PUBLISH_RATE_LIMIT_CALLS = 4 & PUBLISH_RATE_LIMIT_PERIOD_SECONDS = 3 meaning running 4 times 3 secs
PUBLISH_RATE_LIMIT_CALLS = 3
PUBLISH_RATE_LIMIT_PERIOD_SECONDS = 3

logger = logging.getLogger('metric.publisher')

def persist_metrics(key_vals,
                    name=AGENT_MEM_DISK_QUEUE_PROGRAM_NAME,
                    working_dir=AGENT_MEM_DISK_QUEUE_WORKING_DIR,
                    ready_dir=AGENT_MEM_DISK_QUEUE_READY_DIR,
                    debug_mdq=False):
    """Persist metrics to disk through MemDiskQueue.

    Mostly to handle the case when TSD is down.

    Args:
        key_vals (list((Key, Value))): metric KeyValue to persist
        name (str): MemDiskQueue filename prefix
        working_dir (str): MemDiskQueue working dir
        ready_dir (str): MemDiskQueue ready dir
    """
    mdq = create_mem_disk_queue(
        mem_queue_size=1,  # all metrics are combined into one Metrics object
        program_name=name,
        working_dir=working_dir,
        ready_dir=ready_dir,
        sleep_seconds_init=3600,  # essentially disable flush on this queue
        sleep_seconds_max=3600,
        persist_mem_q=True,
        cleanup_orphans=False,
        debug_mdq=debug_mdq
    )
    for key, val in key_vals:
        val.ignore_age = True  # It may take a while for TSD to be back.
    metrics = Metrics([gen_metric_from_key_value(key, val) for key, val in key_vals])
    mdq.push(metrics)
    mdq.close()


class Publisher(Component):
    """Publish received metrics to the tcollector periodically.

    NOTE: There should be just one publisher.
    """
    def __init__(self, ready_snapshots,
                 hosts=None, dryrun=False,
                 queue_pop_timeout=READY_SNAPSHOTS_QUEUE_POP_TIMEOUT_SECONDS,
                 reconnect_interval=TSD_RECONNECT_INTERVAL,
                 verify_interval=TSD_SOCKET_VERIFICATION_INTERVAL,
                 dryrun_out=sys.stdout,
                 adjust_timestamp=True,
                 max_metric_delay=MAX_ALLOWED_METRIC_DELAY_MILLISECONDS,
                 max_publish_chunk_size=MAX_NUM_METRICS_PUBLISH_CHUNK,
                 max_send_trial=MAX_TSD_SEND_TRIAL,
                 max_conn_retry_delay=MAX_CONNECTION_RETRY_DELAY_SECONDS,
                 debug_mdq=False,
                 rate_limit_calls=PUBLISH_RATE_LIMIT_CALLS,
                 rate_limit_period_seconds=PUBLISH_RATE_LIMIT_PERIOD_SECONDS,
                 tsdb_stop_metrics=[],
                 name_group_sample_rate=100,
                 **kwargs):
        super(Publisher, self).__init__(**kwargs)
        self.name = kwargs.get('name', 'Publisher')

        self.ready_snapshots = ready_snapshots  # Queue of ready Snapshots
        self.metrics = []  # assume we only run one publisher thread
        self.metric_lines = []  # assume we only run one publisher thread
        self.queue_pop_timeout = queue_pop_timeout

        self.dryrun = dryrun
        self.hosts = hosts if hosts else []  # A list of (host, port) pairs.
        # Randomize hosts to help even out the load.
        random.shuffle(self.hosts)
        self.current_tsd = -1  # Index in self.hosts where we're at.
        self.host = None  # The current TSD host we've selected.
        self.port = None  # The port of the current TSD.
        self.tsd = None   # The socket connected to the aforementioned TSD.
        self.last_verify = 0
        self.reconnect_interval = reconnect_interval
        self.verify_interval = verify_interval
        self.dryrun_out = dryrun_out  # for unittesting
        self.time_reconnect = 0  # if reconnect_interval > 0, used to track the time.
        self.adjust_timestamp = adjust_timestamp
        self.max_metric_delay = max_metric_delay
        self.max_publish_chunk_size = max_publish_chunk_size
        self.max_send_trial = max_send_trial
        self.max_conn_retry_delay = max_conn_retry_delay
        self.debug_mdq = debug_mdq
        self.try_delay = 1
        self.pub_rate_limiter = RateLimiting(rate_limit_calls, rate_limit_period_seconds)
        # set sampling rate to given if sampling rate is valid, otherwise don't sample
        # this is intended to improve cpu performance.
        if isinstance(name_group_sample_rate, int) and 0 <= name_group_sample_rate < 100:
            self.name_group_sample_rate = name_group_sample_rate
        else:
            self.name_group_sample_rate = 100
        self.tsdb_keep_metrics = []

    def create_connection(self):
        """Create TSD connection to current host and port.

        Returns:
            bool: True if a connection is created.
        """
        if self.dryrun:
            return True

        self.tsd = None
        try:
            addresses = socket.getaddrinfo(self.host, self.port,
                                           socket.AF_UNSPEC,
                                           socket.SOCK_STREAM, 0)
        except Exception as e:
            # Don't croak on transient DNS resolution issues.
            if e[0] in (socket.EAI_AGAIN, socket.EAI_NONAME,
                        socket.EAI_NODATA):
                self.report_counter_metric('publisher.sock_dns_error', 1)
                logger.info('DNS resolution failure: %s: %s', self.host, e)
            return False

        for family, socktype, proto, canonname, sockaddr in addresses:
            try:
                self.tsd = socket.socket(family, socktype, proto)
                self.tsd.settimeout(30)
                self.tsd.connect(sockaddr)
                # if we get here it is connected
                logger.info('Connection to %s was successful', str(sockaddr))
                break
            except socket.error as e:
                logger.warning('Connection attempt failed to %s:%d: %s',
                               self.host, self.port, e)
            if self.tsd:
                self.tsd.close()
                self.tsd = None

        if not self.tsd:
            logger.error('Failed to connect to %s:%d', self.host, self.port)
            return False
        return True

    def verify_connection(self):
        """Periodically verify that our connection to the TSD is OK.

        Returns:
            bool: True if TSD is alive and working.
        """
        if self.dryrun:
            return True

        if not self.tsd:
            return False

        # if the last verification was less than the verify_interval, don't re-verify
        if self.last_verify > time.time() - self.verify_interval:
            return True

        # in case reconnect is activated, check if it's time to reconnect
        if (self.reconnect_interval > 0 and
                self.time_reconnect < time.time() - self.reconnect_interval):
            # closing the connection and indicating that we need to reconnect.
            try:
                self.tsd.close()
            except:
                pass    # not handling that
            self.time_reconnect = time.time()
            self.tsd = None
            assert self.current_tsd >= 0
            self.current_tsd -= 1  # force it to reconnect to the same host
            return False

        # we use the version command as it is very low effort for the TSD
        # to respond
        logger.debug('verifying our TSD connection is alive')
        try:
            # TODO Remove this once all migrated to python3
            if six.PY2:
                self.tsd.sendall('version\n')
            else:
                self.tsd.sendall(b'version\n')
        except socket.error:
            self.tsd = None
            self.report_counter_metric('publisher.verify_conn_failed', 1)
            return False

        bufsize = 4096
        resp_buf = ''
        while not self.quit_event.is_set():
            # try to read as much data as we can.  at some point this is going
            # to block, but we have set the timeout low when we made the
            # connection
            try:
                buf = self.tsd.recv(bufsize)
                # TODO Remove this once all migrated to python3
                if not six.PY2:
                    buf = buf.decode()
                resp_buf += buf
            except socket.error:
                self.tsd = None
                self.report_counter_metric('publisher.verify_conn_failed', 1)
                return False

            # If we don't get a response to the `version' request, the TSD
            # must be dead or overloaded.
            if not resp_buf:
                self.tsd = None
                self.report_counter_metric('publisher.verify_conn_failed', 1)
                return False

            # Woah, the TSD has a lot of things to tell us...  Let's make
            # sure we read everything it sent us by looping once more.
            if len(buf) == bufsize:
                continue

            break  # TSD is alive.

        # if we get here, we assume the connection is good
        self.last_verify = time.time()
        self.report_counter_metric('publisher.verify_conn_success', 1)
        return True

    def sleep_between_trials(self):
        # increase the try delay by some amount and some random value,
        # in case the TSD is down for a while.  delay at most
        # approximately self.max_conn_retry_delay seconds.
        self.try_delay *= 1 + random.random()
        if self.try_delay > self.max_conn_retry_delay:
            self.try_delay *= 0.5
        logger.info('Publisher blocking %0.2f seconds', self.try_delay)
        self.quit_event.wait(self.try_delay)

    def get_next_connection(self):
        """Get next good TSD connection. Raise if run out hosts to try.

        Raises:
            IOError: if run out of TSD hosts to try
        """
        if self.dryrun:
            return

        self.report_counter_metric('publisher.get_tsd_connection', 1)
        self.current_tsd += 1
        while self.current_tsd < len(self.hosts):
            self.host, self.port = self.hosts[self.current_tsd]
            if self.create_connection():
                return
            self.current_tsd += 1

        logger.info('No more healthy hosts, will try all the hosts again after shuffle')
        random.shuffle(self.hosts)
        self.tsd = None
        self.current_tsd = -1
        self.report_counter_metric('publisher.out_of_tsd_connection', 1)
        self.sleep_between_trials()
        raise IOError('No good TSD connection')

    def _send_data(self, data):
        """Send data to TSD with retries.

        Args:
            data (str): text to be pushed to TSD
        Raises:
            Exception: raise after send failed max_send_trial times or run out of TSD hosts to try
        """
        tries = 0
        while True:
            tries += 1
            with self.pub_rate_limiter:
                if not self.tsd:
                    try:
                        self.get_next_connection()
                    except:
                        raise
                try:
                    # TODO Remove this once all migrated to python3
                    if six.PY2:
                        self.tsd.sendall(data)
                    else:
                        self.tsd.sendall(data.encode())
                    break
                except Exception as e:
                    self.report_counter_metric('publisher.send_data_failure', 1)
                    logger.exception('failed _send_data trial no.%d', tries)

                    try:
                        self.tsd.close()
                    except:
                        pass
                    self.tsd = None
                    self.last_verify = time.time() - self.verify_interval

                    if (isinstance(e, UnicodeError) or
                            tries >= self.max_send_trial or
                            self.quit_event.is_set()):
                        raise

    def send_data(self):
        """Sends outstanding data in self.sendq to the TSD in one operation.

        Args:
            num_metrics_sent (int): number of metrics sent successfully.
                                    one metric may generate mulitple rows.
        """

        # batch all the metrics lines
        out = ''.join(ml for ml in self.metric_lines)

        metrics_sent = 0
        try:
            logger.debug('metric lines: %s', self.metric_lines)

            if self.dryrun:
                self.dryrun_out.write(out)
                self.dryrun_out.flush()
            else:
                self._send_data(out)
            self.report_counter_metric('publisher.num_rows_sent',
                                       len(self.metric_lines))

            logger.info('sent out %d metric lines', len(self.metric_lines))

            # treat it as connection verified
            self.last_verify = time.time()

            metrics_sent = len(self.metrics)
        except UnicodeError:
            logger.error('Skip persisting metrics for Unicode exception')
        except:
            logger.info('persist %s metrics on disk after send_data failures', len(self.metrics))
            persist_metrics(self.metrics, debug_mdq=self.debug_mdq)
            self.report_counter_metric('publisher.persist_metrics',
                                       len(self.metrics))
        finally:
            self.metrics[:] = []
            self.metric_lines[:] = []

        return metrics_sent

        # TODO(zheng): collect error msg by calling self.tsd.recv periodically.
        # FIXME: we should be reading the result at some point to drain
        # the packets out of the kernel's queue

    def adjust_metric_timestamp(self, key, val):
        """Adjust timestamp in the metric Key to avoid duplicate write.

        OpenTSDB will report conflict or take the last write, if multiple
        writes happen on the same (metric_key, timestamp, tags). To support
        delayed metrics, we add a random offset milliseconds to the key.timestamp,
        which is already aligned to the minute mark in milliseconds. Majority
        of the duplications happen because we take snapshot in the middle of
        the minute, therefore, the aggregated data always contain data from
        previous minute and current minute. We check the metric timestamp and
        current time. If they are not in the same minute, we make sure after the
        adjustment, the new timestamp is in the first half of the minute. If
        they are in the same minute, we place it in the second half of the minute.
        Assume we always use host in tags, and we are doing per minute snapshot,
        this should provide enough headroom.

        Args:
            key (Key): will be modified if the key has been seen before.
            val (Value): contains ignore_age setting per metric.
        Returns:
            bool: True if the metric should be published. False if it
                  should be dropped because it is too old.
        """
        def get_minute_from_milsecs(mil_ts):
            return (mil_ts / 60000) % 60

        if not self.adjust_timestamp:
            return True

        curr_mil_ts = int(time.time()) * 1000

        if (not val.ignore_age and curr_mil_ts - key.timestamp > self.max_metric_delay):
            self.report_counter_metric('publisher.drop_old_metrics', 1)
            return False

        curr_minute = get_minute_from_milsecs(curr_mil_ts)
        event_minute = get_minute_from_milsecs(key.timestamp)

        if curr_minute > event_minute:
            # previous minute data in the snapshot(therefore happened in the 2nd half of the
            # minute), or delayed data
            key.timestamp += random.randint(30000, 59999)
        else:
            # current minute data in the snapshot(therefore happened in the 1st half of the minute)
            key.timestamp += random.randint(0, 29999)

        return True

    def _process_snapshot(self, snapshot):
        if not len(snapshot):
            return
        self.report_counter_metric('publisher.num_metrics_received', len(snapshot))

        metrics_sent = 0
        try:
            for chunk in chunkify(snapshot.gen_sorted_key_values(),
                                  self.max_publish_chunk_size):
                for key, val in chunk:
                    if not self.adjust_metric_timestamp(key, val):
                        # drop this unfit metric
                        continue

                    try:
                        self.metric_lines.extend(
                            self.build_metric_lines(key=key, value=val)
                        )
                        self.metrics.append((key, val))
                    except:
                        logger.exception(
                            'Skip metric failed to build metric line. key:%s, value:%s',
                            key, val)

                metrics_sent += self.send_data()
        except:
            logger.exception('Failed to process snapshot. Ignore')
            self.report_counter_metric('publisher.dropped_metrics', len(snapshot) - metrics_sent)
            self.report_counter_metric('publisher.failed_process_snapshot', 1)
        self.report_counter_metric('publisher.num_metrics_sent', metrics_sent)
        logger.info('Publisher sent %d out of %d metrics received from a new snapshot',
                    metrics_sent, len(snapshot))

    def build_metric_lines(self, key, value, override_timestamp=None):
        """Build the put lines to send to TSD.

        Args:
            key (Key): metric key
            value (Value): metric value
            override_timestamp (int): if provided, use it instead of the ts in metric key
        Returns:
            [str]: list of the formatted lines to be sent
        """
        timestamp = key.timestamp if not override_timestamp else override_timestamp
        if key.service_name:
            metric_name = '%s.%s' % (key.service_name, key.metric_name)
        else:
            metric_name = key.metric_name

        label_names = []
        label_values = []
        tag_line = ''
        if key.tags:
            tag_line = ' ' + ' '.join('%s=%s' % (k, key.tags[k]) for k in sorted(key.tags.keys()))
        line_tmpl = 'put %%s %d %%s%s\n' % (timestamp, tag_line)
        lines = []

        def add_line(mn, val, metric_type = None):
            if isinstance(val, six.integer_types + (float,)):
                # Check if mn is in the TSDB stop write list, configured in ConfigMap
                # STG_METRIC_CONFIG: &STG_METRIC_CONFIG
                #     <<: *METRIC_CONFIG
                #     TSDB_STOP_METRIC_CONFIG: ['perf.node.products']
                tsdb_logging = False
                if len(self.tsdb_keep_metrics) != 0:
                    for tsdb_keep_metric in self.tsdb_keep_metrics:
                        if tsdb_keep_metric in mn:
                            tsdb_logging = True

                if tsdb_logging:
                    lines.append(line_tmpl % (mn, val))
                    self.report_counter_metric(
                        'publisher.name_group_sent', 1,
                        tags={'group': get_name_group_for_stats(key.service_name, key.metric_name)}
                    )

        if value.metric_type == MetricType.COUNTER:
            add_line(metric_name, value.sum)
            # always output an additional metric line for value.count, so later we can
            # do weighted average on gauge metrics when aggregate them.
            add_line(metric_name + '_count', value.count, metric_type=MetricType.COUNTER)
        elif value.metric_type == MetricType.GAUGE:
            # take average for gauge type
            if value.count:
                add_line(metric_name, value.sum / value.count)
            # always output an additional metric line for value.count, so later we can
            # do weighted average on gauge metrics when aggregate them.
            add_line(metric_name + '_count', value.count, metric_type=MetricType.COUNTER)
        elif value.metric_type == MetricType.HISTOGRAM:
            try:
                hb = value.histogram_buckets
                for p, v in zip(HISTOGRAM_PERCENTILES, hb.quantiles(*HISTOGRAM_PERCENTILES)):
                    add_line('%s.p%s' % (metric_name, int(p * 100)), v)
                # Required for giving counts for every bucket
                add_line(metric_name + '.count', hb.count(), metric_type=MetricType.COUNTER)
                # We sample max and mean by sampling
                if random.random() < self.name_group_sample_rate / 100.0:
                    add_line(metric_name + '.max', hb.max())
                    add_line(metric_name + '.mean', hb.mean())
            except:
                self.report_counter_metric('publisher.failed_histogram', 1)
                raise
        else:
            self.report_counter_metric('publisher.unknown_metric_type', 1)
            logger.error('Skip unknown metric type %s', value.metric_type)

        return lines

    def run(self):
        while not self.quit_event.is_set():
            # TODO(zheng): do this in a separate thread
            if not self.verify_connection():
                try:
                    self.get_next_connection()
                except:
                    # we don't have a good conn, persist data and retry later
                    pass

            try:
                snapshot = self.ready_snapshots.get(True, self.queue_pop_timeout)
            except Empty:
                continue

            self._process_snapshot(snapshot)

        # aggregator may have made one last snapshot before it exits
        while True:
            try:
                snapshot = self.ready_snapshots.get_nowait()
            except Empty:
                break
            logger.info('Processing 1 snapshot after being instructed to shut down')
            self._process_snapshot(snapshot)

