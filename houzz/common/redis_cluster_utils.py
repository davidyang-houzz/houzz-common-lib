#!/usr/bin/env python

''' redis utils.
'''
from __future__ import absolute_import
import logging
import redis
import rediscluster

from rediscluster.exceptions import RedisClusterException

from .config import get_config

Config = get_config()

# separator between parts of a key
KEY_SEP = ':'

logger = logging.getLogger(__name__)


def get_redis_housedata_cluster_connection():
    ''' obtain connection to similarity cluster instance
    '''
    try:
        return rediscluster.StrictRedisCluster(
            startup_nodes=Config.REDIS_HOUSEDATA_CLUSTER_PARAMS,
            max_connections=256,
            max_connections_per_node=True,
            decode_responses=True,
            socket_timeout=60.0,  # read/write timeout
            socket_connect_timeout=10.0,  # connection timeout
            socket_keepalive=True,  # tcp keepalive for idle connections
        )
    except RedisClusterException:
        logger.warn(
            'Failed to create Redis cluster client, fallback...')
        return redis.StrictRedis(**Config.REDIS_HOUSEDATA_CLUSTER_PARAMS)


def get_redis_gold_cluster_connection():
    ''' obtain connection to similarity cluster instance
    '''
    try:
        return rediscluster.StrictRedisCluster(
            startup_nodes=Config.REDIS_GOLD_CLUSTER_PARAMS.get('startup_nodes'),
            max_connections=256,
            max_connections_per_node=True,
            decode_responses=True,
            socket_timeout=60.0,  # read/write timeout
            socket_connect_timeout=10.0,  # connection timeout
            socket_keepalive=True,  # tcp keepalive for idle connections
        )
    except RedisClusterException:
        logger.warn(
            'Failed to create Redis gold cluster client, fallback...')
        return redis.StrictRedis(**Config.REDIS_GOLD_CLUSTER_PARAMS)
