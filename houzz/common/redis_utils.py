#!/usr/bin/env python

''' redis utils.
'''

from __future__ import absolute_import
import redis
import rediscluster

from .config import get_config

from six.moves import zip_longest
from six.moves import range

import logging
import sys

Config = get_config()

# separator between parts of a key
KEY_SEP = ':'

QUERY_BATCH_SIZE = 1000

PREFIX_IMG = 'img'
PERSISTENT_DATA_TTL = 0

logger = logging.getLogger(__name__)
class RedisTTL(object):
    def __init__(self, config):
        self._config = config
        self._default_redis_cluster_ttl = self._config.get('redis_cluster_ttl', PERSISTENT_DATA_TTL)
        self._whitelist = self._config.get('redis_ttl_whitelist', {})

    def get_app_ttl(self, name):
        name_without_slash = name.replace("/", "").lower()
        for key_prefix, ttl in self._whitelist.items():
            if name_without_slash.startswith(key_prefix):
                return ttl
        return self._default_redis_cluster_ttl


class PipelineWrapper(object):
    def __init__(self, pipeline, redis_ttl):
        self._pipeline = pipeline
        self.redis_ttl = redis_ttl

    def __getattr__(self, attr):
        return self._pipeline.__getattribute__(attr)

    def __enter__(self):
        """
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        """
        self._pipeline.reset()

    def __repr__(self):
        """
        """
        return "{0}".format(type(self._pipeline).__name__)

    def __del__(self):
        """
        """
        self._pipeline.reset()

    def __len__(self):
        """
        """
        return len(self._pipeline.command_stack)

    @property
    def __class__(self):
        return self._pipeline.__class__

    def _get_expire_cmd(self, args):
        cmd = args[0]
        result = []
        if cmd in ['SET', 'INCRBY', 'INCRBYFLOAT', 'SADD', 'GETSET', 'ZADD']:
            name = args[1]
            app_ttl = self.redis_ttl.get_app_ttl(name)
            if app_ttl:
                result.append(['EXPIRE', name, app_ttl])
        elif cmd in ['MSET', 'MSETNX']:  # cluster mode dose not support the mset and msetnx
            for i in range(1, len(args), 2):
                app_ttl = self.redis_ttl.get_app_ttl(args[i])
                if app_ttl:
                    result.append(["EXPIRE", args[i], app_ttl])
        return result

    def execute(self, raise_on_error=True):
        new_command_stack = []
        expire_cmd_index = []
        counter = 0
        for command in self._pipeline.command_stack:
            if isinstance(command, rediscluster.pipeline.PipelineCommand):
                command.position = counter
                new_command_stack.append(command)
                counter += 1
                for cmd in self._get_expire_cmd(command.args):
                    new_command_stack.append(rediscluster.pipeline.PipelineCommand(cmd, position=counter))
                    expire_cmd_index.append(counter)
                    counter += 1
            elif isinstance(command, tuple):
                new_command_stack.append(command)
                counter += 1
                for cmd in self._get_expire_cmd(command[0]):
                    new_command_stack.append((cmd, {}))
                    expire_cmd_index.append(counter)
                    counter += 1

        self._pipeline.command_stack = new_command_stack
        results = self._pipeline.execute(raise_on_error=raise_on_error)
        expire_cmd_pointer = 0
        new_results = []
        for index, result_item in enumerate(results):
            if expire_cmd_pointer >= len(expire_cmd_index) or index != expire_cmd_index[expire_cmd_pointer]:
                new_results.append(result_item)
            else:
                expire_cmd_pointer += 1
        return new_results


class RedisWrapper(object):

    def __init__(self, redis_connect):
        self._redis_connect = redis_connect
        try:
            self._config = Config.REDIS_TTL_CONFIG
        except Exception:
            self._config = {}

        self.redis_ttl = RedisTTL(self._config)

    def set_ttl_config(self, config):
        self._config = config
        self.redis_ttl = RedisTTL(self._config)

    def __getattr__(self, attr):
        return self._redis_connect.__getattribute__(attr)

    @property
    def __class__(self):
        return self._redis_connect.__class__

    def _add_expire_command_in_pipe(self, pipe, name):
        app_ttl = self.redis_ttl.get_app_ttl(name)
        if app_ttl:
            pipe.expire(name, app_ttl)

    def set(self, name, value, **kwargs):
        transaction = not isinstance(self._redis_connect, rediscluster.RedisCluster)
        with self._redis_connect.pipeline(transaction=transaction) as pipe:
            pipe.set(name=name, value=value, **kwargs)
            self._add_expire_command_in_pipe(pipe, name)
            results = pipe.execute()
            return results[0]

    def delete(self, name, **kwargs):
        transaction = not isinstance(self._redis_connect, rediscluster.RedisCluster)
        with self._redis_connect.pipeline(transaction=transaction) as pipe:
            pipe.delete(name, **kwargs)
            results = pipe.execute()
            return results[0]

    def mset(self, *args, **kwargs):
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise redis.RedisError('MSET requires **kwargs or a single dict arg')
            kwargs.update(args[0])

        # Cluster mode cannot support the mset and msetnx with pipeline.
        result = self._redis_connect.mset(kwargs)

        with self._redis_connect.pipeline() as pipe:
            for name in kwargs.keys():
                self._add_expire_command_in_pipe(pipe, name)
            pipe.execute()
            return result

    def msetnx(self, *args, **kwargs):
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise redis.RedisError('MSETNX requires **kwargs or a single dict arg')
            kwargs.update(args[0])

        result = self._redis_connect.msetnx(kwargs)
        if result:
            with self._redis_connect.pipeline() as pipe:
                for name in kwargs.keys():
                    self._add_expire_command_in_pipe(pipe, name)
                pipe.execute()
            return result

    def incr(self, name, amount=1):
        transaction = not isinstance(self._redis_connect, rediscluster.RedisCluster)
        with self._redis_connect.pipeline(transaction=transaction) as pipe:
            pipe.incr(name=name, amount=str(amount))
            self._add_expire_command_in_pipe(pipe, name)
            results = pipe.execute()
            return results[0]

    def incrby(self, name, amount=1):
        transaction = not isinstance(self._redis_connect, rediscluster.RedisCluster)
        with self._redis_connect.pipeline(transaction=transaction) as pipe:
            pipe.incrby(name=name, amount=amount)
            self._add_expire_command_in_pipe(pipe, name)
            results = pipe.execute()
            return results[0]

    def incrbyfloat(self, name, amount=1.0):
        transaction = not isinstance(self._redis_connect, rediscluster.RedisCluster)
        with self._redis_connect.pipeline(transaction=transaction) as pipe:
            pipe.incrbyfloat(name=name, amount=amount)
            self._add_expire_command_in_pipe(pipe, name)
            results = pipe.execute()
            return results[0]

    def getset(self, name, value):
        transaction = not isinstance(self._redis_connect, rediscluster.RedisCluster)
        with self._redis_connect.pipeline(transaction=transaction) as pipe:
            pipe.getset(name=name, value=value)
            self._add_expire_command_in_pipe(pipe, name)
            results = pipe.execute()
            return results[0]

    def sadd(self, name, *values):
        transaction = not isinstance(self._redis_connect, rediscluster.RedisCluster)
        with self._redis_connect.pipeline(transaction=transaction) as pipe:
            pipe.sadd(name, *values)
            self._add_expire_command_in_pipe(pipe, name)
            results = pipe.execute()
            return results[0]

    def zadd(self, name, *values):
        transaction = not isinstance(self._redis_connect, rediscluster.RedisCluster)
        with self._redis_connect.pipeline(transaction=transaction) as pipe:
            pipe.zadd(name, *values)
            self._add_expire_command_in_pipe(pipe, name)
            results = pipe.execute()
            return results[0]

    def pipeline(self, transaction=True, shard_hint=None):
        # Backward compatibility for the Python 2 application
        if sys.version_info[0] == 2:
            if isinstance(self._redis_connect, rediscluster.StrictRedisCluster):
                transaction = False
        else:
            if isinstance(self._redis_connect, rediscluster.RedisCluster):
                transaction = False
        pipeline = self._redis_connect.pipeline(transaction=transaction, shard_hint=shard_hint)
        return PipelineWrapper(pipeline, self.redis_ttl)


def get_redis_platinum_cluster_connection():
    ''' obtain connection to the redis platinum cluster
    '''
    return get_redis_connection({'cluster': Config.REDIS_PLATINUM_CLUSTER_PARAMS})


def get_redis_gold_cluster_connection():
    ''' obtain connection to the redis gold cluster
    '''
    return get_redis_connection({'cluster': Config.REDIS_GOLD_CLUSTER_PARAMS})


def get_redis_connection(params, strict=False, **kwargs):
    ''' obtain connection to a redis server or cluster based on the given params
    '''
    cluster_params = params.get('cluster', None)
    if cluster_params:
        params = cluster_params
        conn_class = rediscluster.RedisCluster
    else:
        conn_class = redis.Redis
    params.pop('redis_ttl_white_list', None)
    params.pop('redis_cluster_ttl', None)
    if conn_class == rediscluster.RedisCluster:
        params['skip_full_coverage_check'] = True
        if hasattr(rediscluster, 'VERSION') and rediscluster.VERSION[0] == 2:
            params['health_check_interval'] = 30
    logger.info("Create the Redis connection with parameters {}".format(params))
    return RedisWrapper(conn_class(**_prepare_redis_params(params, kwargs)))


def _prepare_redis_params(params, overrides):
    if overrides:
        result = params.copy()
        result.update(overrides)
    else:
        result = params
    return result


def get_redis_tracking_slave_connection(strict=False, **kwargs):
    ''' obtain connection to tracking slave instance.
    '''
    return get_redis_connection(
        Config.REDIS_TRACKING_SLAVE_PARAMS, strict, **kwargs)


def get_redis_tracking_master_connection(strict=False, **kwargs):
    ''' obtain connection to tracking master instance.
    '''
    return get_redis_connection(
        Config.REDIS_TRACKING_MASTER_PARAMS, strict, **kwargs)


def get_redis_main_slave_connection(strict=False, **kwargs):
    ''' obtain connection to main slave instance.
    '''
    return get_redis_connection(
        Config.REDIS_MAIN_SLAVE_PARAMS, strict, **kwargs)


def get_redis_main_master_connection(strict=False, **kwargs):
    ''' obtain connection to main master instance.
    '''
    return get_redis_connection(
        Config.REDIS_MAIN_MASTER_PARAMS, strict, **kwargs)


def get_redis_newsletter_master_connection(strict=False, **kwargs):
    ''' obtain connection to newsletter instance.
    '''
    return get_redis_connection(
        Config.REDIS_NEWSLETTER_MASTER_PARAMS, strict, **kwargs)


def get_redis_newsletter_slave_connection(strict=False, **kwargs):
    ''' obtain connection to newsletter instance.
    '''
    return get_redis_connection(
        Config.REDIS_NEWSLETTER_SLAVE_PARAMS, strict, **kwargs)


def get_redis_ad_master_connection(strict=False, **kwargs):
    ''' obtain connection to ad master instance.
    '''
    return get_redis_connection(
        Config.REDIS_AD_MASTER_PARAMS, strict, **kwargs)


def get_redis_ad_slave_connection(strict=False, **kwargs):
    ''' obtain connection to ad slave instance.
    '''
    return get_redis_connection(
        Config.REDIS_AD_SLAVE_PARAMS, strict, **kwargs)


def get_redis_hadoop_connection(strict=False, **kwargs):
    ''' obtain connection to hadoop slave instance.
    '''
    return get_redis_connection(
        Config.REDIS_HADOOP_PARAMS, strict, **kwargs)


def get_redis_proplus_slave_connection(strict=False, **kwargs):
    ''' obtain connection to pro plus master instance
    '''
    return get_redis_connection(
        Config.REDIS_PROPLUS_SLAVE_PARAMS, strict, **kwargs)


def get_redis_proplus_master_connection(strict=False, **kwargs):
    ''' obtain connection to pro plus master instance
    '''
    return get_redis_connection(
        Config.REDIS_PROPLUS_MASTER_PARAMS, strict, **kwargs)

def get_redis_cache_service_connection(strict=False, **kwargs):
    ''' obtain connection to cache service instance
    '''
    return get_redis_connection(
        Config.REDIS_CACHE_SERVICE_PARAMS, strict, **kwargs)

def grouper(n, iterable, fillvalue=None):
    ''' Collect data into fixed-length chunks or blocks
        grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    '''
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)
