#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from six.moves.queue import Empty
from StringIO import StringIO
import logging
import multiprocessing
import os
import signal
import sys

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.server import TNonblockingServer

from houzz.common.thrift_gen.metrics import MetricCollector
from houzz.common.thrift_gen.metrics.ttypes import Metrics
from houzz.common.metric.components.aggregator import Aggregator
from houzz.common.metric.components.controller import Controller
from houzz.common.metric.components.publisher import Publisher
from houzz.common.metric.components.receiver import CollectHandler
from houzz.common.metric.components.snapshoter import (
    Snapshoter,
    generate_server_offset,
)
from houzz.common.metric.common.key_value import KeyValue
from houzz.common.metric.common.utils import update_socket_permission
from houzz.common.metric.client import MetricClientFactory

from houzz.common.metric.common.tests.common_test_utils import (
    aggregate_key_values,
    create_tags,
    cleanup_mem_disk_queue,
    get_mem_disk_queue,
)

SERVICE_NAME = 'dummy_service'
METRIC_NAME = 'dummy_metric'
TAG_KEY_PREFIX = 'tag_'
TAG_VAL_PREFIX = 'val_'
NUM_TAGS = 2


def gen_metric_client(mem_q_size=10, mdq_sleep_init=0.1, mdq_sleep_max=0.2):
    """Create new MetricClient with injected MemDiskQueue to control cleanup.

    NOTE: py.test run all tests in the same process. To avoid all test cases
    using the same MetricClient object, we explicitly reset it here so each
    individual test has a clean slate to begin with.
    """
    client_q = get_mem_disk_queue(
        name='metric_client_python',
        mem_q_size=mem_q_size,
        mdq_sleep_init=mdq_sleep_init,
        mdq_sleep_max=mdq_sleep_max
    )
    return MetricClientFactory._reset_metric_client(
        service_name=SERVICE_NAME,
        tags=create_tags(),
        client_queue=client_q
    )


def cleanup_metric_client(metric_client):
    cleanup_mem_disk_queue(metric_client.client_queue)


def _start_agent_server(disable_snapshot=False, host=None, port=None, uds=None, tags=None,
                        mem_q_size=10, mdq_sleep_init=0.1, mdq_sleep_max=0.2, ready_q_timeout=0.1,
                        ready_q_size=100, **_kwargs):
    # initialize metric_client to track the server metrics
    metric_client = gen_metric_client()

    controller = Controller()
    controller.reset()

    read_q = get_mem_disk_queue(
        name='test_utils',
        mem_q_size=mem_q_size,
        mdq_sleep_init=mdq_sleep_init,
        mdq_sleep_max=mdq_sleep_max
    )

    handler = CollectHandler(controller=controller, tags=tags, read_q=read_q)
    processor = MetricCollector.Processor(handler)

    aggregator = Aggregator(read_q,
                            queue_pop_timeout_seconds=ready_q_timeout,
                            max_num_ready_snapshots=ready_q_size,
                            name='collector')
    controller.register(Controller.AGGREGATOR, aggregator)
    aggregator.start()

    if not disable_snapshot:
        snapshoter = Snapshoter(aggregator, offset=generate_server_offset())
        controller.register(Controller.SNAPSHOTER, snapshoter)
        snapshoter.start()

    publisher = Publisher(aggregator.ready_snapshots,
                          dryrun=True,
                          queue_pop_timeout=ready_q_timeout,
                          dryrun_out=StringIO(),
                          adjust_timestamp=False)
    controller.register(Controller.PUBLISHER, publisher)
    publisher.start()

    if host and port:
        transport = TSocket.TServerSocket(host=host, port=port)
    elif uds:
        if os.path.exists(uds):
            os.unlink(uds)
        transport = TSocket.TServerSocket(unix_socket=uds)
    else:
        raise ValueError(
            'Either unix_domain_socket or (listen_host, listen_port) has to be specified'
        )

    tfactory = TBinaryProtocol.TBinaryProtocolFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TNonblockingServer.TNonblockingServer(processor, transport, tfactory, pfactory)

    # register cleanup when being terminated by parent process
    def cleanup(num, frame):
        logging.info('received signal %s', num)
        server.close()
        cleanup_metric_client(metric_client)
        cleanup_mem_disk_queue(handler.read_q)
        sys.exit()
    signal.signal(signal.SIGTERM, cleanup)

    server.prepare()
    if uds:
        update_socket_permission(uds)
    server.serve()


def start_agent_server(disable_snapshot=False, host=None, port=None, uds=None, tags=None,
                       mem_q_size=10, mdq_sleep_init=0.1, mdq_sleep_max=0.2, ready_q_timeout=0.1,
                       ready_q_size=100):
    """Start a metric agent server in a separate process.

    NOTE: We use different process to allow client and server both have their own
          MetricClient (singleton within the process).
    """
    sub_proc = multiprocessing.Process(target=_start_agent_server,
                                       name='test_agent_server',
                                       kwargs=locals())
    sub_proc.daemon = True
    sub_proc.start()
    return sub_proc


def call_agent_server(thrift_client, func_name, *args, **kwargs):
    if not thrift_client._iprot.trans.isOpen():
        thrift_client._iprot.trans.open()
    return getattr(thrift_client, func_name)(*args, **kwargs)


def get_agent_stat(thrift_client, stat_name, default_val=0):
    return call_agent_server(thrift_client, 'get_stat', stat_name).get(stat_name, default_val)


def verify_internal_metrics(metric_queue, assert_func, expected, ignored_tags=None):
    kvs = []
    while True:
        try:
            metrics = metric_queue.pop_nowait()
            logging.debug('pop out: %s', metrics)
            assert isinstance(metrics, Metrics)
            for m in metrics.metrics:
                kvs.append(KeyValue.from_thrift_msg(m))
        except Empty:
            break
    received = aggregate_key_values(kvs, ignored_tags)
    logging.debug('aggregated from queue: %s', received)
    assert_func(expected, received)
