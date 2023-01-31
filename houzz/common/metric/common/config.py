#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################
#  Common constants shared between client and servers.  #
#########################################################

# Parameters for setting up the MemDiskQueue shared by CollectHandler, Aggregator, and
# also Publisher when it tries to persist failing metrics to disk through MemDiskQueue.
AGENT_MEM_DISK_QUEUE_WORKING_DIR = '/home/clipu/c2svc/mem_disk_queues/metric_agent_server/working'
AGENT_MEM_DISK_QUEUE_READY_DIR = '/home/clipu/c2svc/mem_disk_queues/metric_agent_server/ready'
AGENT_MEM_DISK_QUEUE_PROGRAM_NAME = 'handler'
AGENT_MEM_DISK_QUEUE_SLEEP_SECONDS_INIT = 1.0
AGENT_MEM_DISK_QUEUE_SLEEP_SECONDS_MAX = 30.0
AGENT_MEM_QUEUE_SIZE = 10000

# Parameters for setting up the MemDiskQueue shared by MetricClient and MetricPusher
CLIENT_MEM_DISK_QUEUE_WORKING_DIR = '/home/clipu/c2svc/mem_disk_queues/metric_client_python/working'
CLIENT_MEM_DISK_QUEUE_READY_DIR = '/home/clipu/c2svc/mem_disk_queues/metric_client_python/ready'
CLIENT_MEM_DISK_QUEUE_PROGRAM_NAME = 'metric_client_python'
CLIENT_MEM_DISK_QUEUE_SLEEP_SECONDS_INIT = 1.0
CLIENT_MEM_DISK_QUEUE_SLEEP_SECONDS_MAX = 30.0
CLIENT_MEM_QUEUE_SIZE = 2000
