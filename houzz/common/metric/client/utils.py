#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from six.moves.queue import Queue
import logging
import os

from queuelib import FifoDiskQueue

from houzz.common.mem_disk_queue.mem_disk_queue import MemDiskQueue
from houzz.common.metric.common.config import (
    CLIENT_MEM_DISK_QUEUE_WORKING_DIR,
    CLIENT_MEM_DISK_QUEUE_READY_DIR,
    CLIENT_MEM_DISK_QUEUE_PROGRAM_NAME,
    CLIENT_MEM_DISK_QUEUE_SLEEP_SECONDS_INIT,
    CLIENT_MEM_DISK_QUEUE_SLEEP_SECONDS_MAX,
    CLIENT_MEM_QUEUE_SIZE,
)
from houzz.common.metric.common.key_value import (
    deserialize_metrics,
    serialize_metrics,
)

logger = logging.getLogger('metric.utils')


def create_mem_disk_queue(mem_queue_size=CLIENT_MEM_QUEUE_SIZE,
                          program_name=CLIENT_MEM_DISK_QUEUE_PROGRAM_NAME,
                          working_dir=CLIENT_MEM_DISK_QUEUE_WORKING_DIR,
                          ready_dir=CLIENT_MEM_DISK_QUEUE_READY_DIR,
                          sleep_seconds_init=CLIENT_MEM_DISK_QUEUE_SLEEP_SECONDS_INIT,
                          sleep_seconds_max=CLIENT_MEM_DISK_QUEUE_SLEEP_SECONDS_MAX,
                          persist_mem_q=True,
                          cleanup_orphans=False,
                          debug_mdq=False,
                          **kwargs):
    mem_q = Queue(mem_queue_size)
    mdq = MemDiskQueue(
        program_name=program_name,
        mem_q_pusher=mem_q.put,
        mem_q_popper=mem_q.get,
        ready_checker=lambda: not mem_q.full(),
        empty_checker=mem_q.empty,
        working_dir=working_dir,
        ready_dir=ready_dir,
        sleep_seconds_init=sleep_seconds_init,
        sleep_seconds_max=sleep_seconds_max,
        persist_mem_q=persist_mem_q,
        serialize_func=serialize_metrics,
        deserialize_func=deserialize_metrics,
        cleanup_orphans=cleanup_orphans,
        debug_mdq=debug_mdq
    )
    return mdq


def iter_mem_disk_queue_items(fp, deserialize_func=deserialize_metrics):
    """Generator to yield items from the given on disk MemDiskQueue.

    Args:
        fp (str): file path to the MemDiskQueue
        deserialize_func (func): func to deserialize data in the queue
    """
    assert os.path.exists(fp), 'MemDiskQueue path %s does NOT exist' % fp
    try:
        fdq = FifoDiskQueue(fp)
    except:
        logger.exception('failed to open MemDiskQueue %s.', fp)
        return

    while True:
        try:
            data = fdq.pop()
        except:
            logger.exception('failed to pop from MemDiskQueue %s. Stop', fp)
            break
        if data is None:
            logger.debug('Done with MemDiskQueue %s.', fp)
            break
        yield deserialize_func(data)
