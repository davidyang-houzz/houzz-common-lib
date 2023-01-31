from __future__ import absolute_import
from __future__ import print_function
import logging

from contextlib import contextmanager
import os
import time
from datetime import datetime
import threading
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

from houzz.common.thrift_gen.log_record import LogCollector
from houzz.common.thrift_gen.log_record.ttypes import PerfRecord, CounterType

from houzz.common.metric.client import init_metric_client

_Per_Monitor = None

logger = logging.getLogger(__name__)


def get_metric_name_from_perf_name(perf_name):
    return perf_name.replace('|', '.')


class PerfMonitor(object):
    @staticmethod
    def set_instance(host=None, port=None, disable_metrics=False,
                     metric_client=None, metric_uds=None, service_name=None):
        global _Per_Monitor
        _Per_Monitor = PerfMonitor(
            host=host,
            port=port,
            disable_metrics=disable_metrics,
            metric_client=metric_client,
            metric_uds=metric_uds,
            service_name=service_name
        )
        return _Per_Monitor

    @staticmethod
    def get_instance():
        if _Per_Monitor is None:
            PerfMonitor.set_instance()
        return _Per_Monitor

    @staticmethod
    def has_instance():
        return _Per_Monitor is not None

    def __init__(self, host=None, port=None, disable_metrics=False,
                 metric_client=None, metric_uds=None, service_name=None):
        '''When PerfMonitor is initialized through the PerfModule, metric_client should
           have been already setup by the MetricModule. Some codes use PerfMonitor directly,
           they should pass in metric_uds, so the metric_client can be initialized here.
           Or, they can explicitly disable it by setting the disable_metrics flag.
        '''
        self.host = host
        self.port = port
        self.disable_metrics = disable_metrics
        self.service_name = service_name
        self.metric_uds = metric_uds
        if metric_client:
            self.metric_client = metric_client
            if self.service_name:
                self.metric_client.service_name = self.service_name
        else:
            self.metric_client = init_metric_client(
                disable_metrics=self.disable_metrics,
                service_name=self.service_name,
                collector_uds=self.metric_uds
            )
        self.client = None
        self.retryTime = None
        self.transport = None
        self.retryPeriod = None
        #
        # Exponential backoff parameters.
        #
        self.retryStart = 1.0
        self.retryMax = 30.0
        self.retryFactor = 2.0

        # lock
        self.lock = threading.Lock()
        # HACK: to support PerfMonitor being used in multi-processed env.
        # The PerfMonitor may be created in the parent process. The child
        # process need to re-initialize it make sure all necessary threads
        # are properly created and started.
        self.metric_client_inited = {}  # process id => bool

    def create_client(self):
        now = time.time()
        if self.retryTime is None:
            attempt = True
        else:
            attempt = (now >= self.retryTime)
        if attempt:
            try:
                socket = TSocket.TSocket(
                    self.host, self.port
                ) if self.port else TSocket.TSocket(unix_socket=self.host)
                self.transport = TTransport.TFramedTransport(socket)
                protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
                self.client = LogCollector.Client(protocol)
                self.transport.open()
                self.retryTime = None  # next time, no delay before trying
            except TTransport.TTransportException as err:
                print(err)
                # Creation failed, so set the retry time and return.
                if self.retryTime is None:
                    self.retryPeriod = self.retryStart
                else:
                    self.retryPeriod *= self.retryFactor
                    if self.retryPeriod > self.retryMax:
                        self.retryPeriod = self.retryMax
                self.retryTime = now + self.retryPeriod

    @contextmanager
    def get_perf(self, perf_name=None, log_dur=True):
        counter = PerfCounter()
        start_time = datetime.utcnow()
        try:
            yield counter
        finally:
            with self.lock:
                if self.client is None and self.host:
                    self.create_client()
                # make sure metric client is properly initialized for this proc
                pid = os.getpid()
                if pid not in self.metric_client_inited:
                    self.metric_client = init_metric_client(
                        disable_metrics=self.disable_metrics,
                        service_name=self.service_name,
                        collector_uds=self.metric_uds
                    )
                    self.metric_client_inited[pid] = 1

            if counter.start_time is None:
                counter.dur_sec.append((datetime.utcnow() - start_time).total_seconds())
            if not log_dur:
                counter.dur_sec = []

            recs = []
            if counter.total:
                recs.append(
                    PerfRecord(
                        perf_name, CounterType.TOTAL, times=counter.total, period=counter.dur_sec
                    )
                )
                logger.debug('name:%s, total:%s, dur:%s', perf_name, counter.total, counter.dur_sec)
            if counter.error:
                recs.append(PerfRecord(perf_name, CounterType.ERR, times=counter.error))
                logger.debug('name:%s, err:%s', perf_name, counter.error)

            with self.lock:
                if self.client:
                    try:
                        self.client.perf(recs)
                    except (OSError, TTransport.TTransportException) as err:
                        print(err)
                        self.client = None  # so we can call createSocket next time
                        self.transport.close()
                    except Exception as err:
                        print(err)
                        self.client = None  # so we can call createSocket next time
                        self.transport.close()

                if self.metric_client:
                    metric_prefix = get_metric_name_from_perf_name(perf_name)
                    self.metric_client.log_counter('%s.called' % metric_prefix, counter.total)
                    self.metric_client.log_counter('%s.error' % metric_prefix, counter.error)
                    for dur in counter.dur_sec:
                        self.metric_client.log_gauge('%s.duration' % metric_prefix, dur)


class PerfCounter(object):
    def __init__(self):
        self._total = 0
        self._error = 0
        self.start_time = None
        self._dur_sec = []

    def count_total(self):
        self._total += 1

    def start_period(self):
        self.start_time = datetime.utcnow()

    def end_period(self):
        dur_sec = (datetime.utcnow() - self.start_time).total_seconds()
        self._dur_sec.append(dur_sec)
        self.start_time = datetime.utcnow()

    @property
    def dur_sec(self):
        return self._dur_sec

    @dur_sec.setter
    def dur_sec(self, value):
        self._dur_sec = value

    @property
    def total(self):
        return self._total

    def count_error(self):
        self._error += 1

    @property
    def error(self):
        return self._error


def get_perf_count(name, log_dur=True):
    def wrapper(func):
        def call(*args, **kwargs):
            if _Per_Monitor is None:
                return func(*args, **kwargs)
            with _Per_Monitor.get_perf(name, log_dur=log_dur) as counter:
                try:
                    counter.start_period()
                    counter.count_total()
                    return func(*args, **kwargs)
                except:
                    counter.count_error()
                    raise
                finally:
                    counter.end_period()

        return call

    return wrapper


def get_perf_count_without_dur(name):
    return get_perf_count(name, log_dur=False)
