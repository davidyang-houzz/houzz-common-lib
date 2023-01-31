from __future__ import absolute_import
import uuid
import time
from collections import namedtuple

import redis
import six


Lock = namedtuple("Lock", ("validity", "resource_key", "lock_val"))


class RedisLocker(object):

    _key_prefix = 'r_locker'
    _default_retry_count = 3
    _default_retry_delay = 0.2
    _default_ttl_sec = 60
    _clock_drift_factor = 0.01
    _unlock_script = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end"""
    _relock_script = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("expire",ARGV[2])
    else
        return 0
    end"""

    _global_env_key = None

    @classmethod
    def from_config(cls, config, env_key=None, connection=None):
        if config:
            kwargs = {
                # 'connection': config.get('REDIS_URL', None),
                'env_key': env_key or config.get('ENV_KEY', None)
            }
        else:
            kwargs = {
                'env_key': env_key,
            }
        if connection:
            kwargs['connection'] = connection
        return cls(**kwargs)

    def __init__(
            self, connection=None, retry_count=None,
            retry_delay=None, logger=None, ttl_sec=None,
            resource_key=None, env_key=None):
        self._retry_count = retry_count or self._default_retry_count
        self._retry_delay = retry_delay or self._default_retry_delay
        self._ttl_sec = ttl_sec or self._default_ttl_sec
        self._resource_key = resource_key or None
        self._logger = logger
        if not self._logger:
            import logging
            self._logger = logging.getLogger(__name__)
        self._init_connection(connection)
        if not env_key and self._global_env_key:
            env_key = self._global_env_key
            self._logger.debug(
                'RedisLocker setting env from global_env_key: %s' %
                (env_key, ))
        elif env_key:
            self._logger.debug(
                'RedisLocker has env_key: %s, global: %s' %
                (env_key, self._global_env_key, ))
            if self._global_env_key:
                if self._global_env_key != env_key:
                    self._logger.warn(
                        ('RedisLocker mismatching locks, env_key: %s, ' +
                            'global_env_key : %s') %
                        (env_key, self._global_env_key, ))
            else:
                self.__class__._global_env_key = env_key
                self._logger.debug(
                    'RedisLocker setting global_env_key: %s' %
                    (self._global_env_key, ))
        if env_key:
            self._key_prefix = '%s:%s' % (self._key_prefix, env_key)
        self._logger.debug(
            'RedisLocker initialized, connection: %s' % (self._con))
        self._logger.debug(
            'RedisLocker env_key: %s' % (env_key, ))

    def _init_connection(self, connection):
        if connection is None:
            try:
                from houzz.common import redis_cluster_utils
                self._con = redis_cluster_utils. \
                    get_redis_housedata_cluster_connection()
            except AttributeError:
                self._logger.warn('Cluster connection failed, trying main')
                from houzz.common import redis_utils
                self._con = redis_utils. \
                    get_redis_main_master_connection(strict=True)
        elif isinstance(connection, six.string_types):
            self._con = redis.StrictRedis.from_url(connection)
        elif isinstance(connection, dict):
            self._con = redis.StrictRedis(**connection)
        else:
            self._con = connection

    def _do_set_lock(self, resource_key, lock_val, ttl):
        self._logger.debug(
            'RedisLocker::_do_set_lock rsrc_key: %s, lock_val: %s, ttl: %s' %
            (resource_key, lock_val, ttl, ))
        try:
            result = self._con.set(
                resource_key, lock_val, nx=True, px=int(ttl))
            if not result:
                self._logger.debug(
                    'RedisLocker::_do_set_lock rsrc_key: %s, failed to lock' %
                    (resource_key, ))
            return result
        except Exception:
            self._logger.exception(
                '_do_set_lock failed, rsrc_key: %s, lock_val: %s, ttl: %s' %
                (resource_key, lock_val, ttl, ))
            return False

    def _do_unlock(self, resource_key, lock_val):
        self._logger.debug(
            'RedisLocker::_do_unlock rsrc_key: %s, lock_val: %s' %
            (resource_key, lock_val, ))
        try:
            return self._con.eval(
                self._unlock_script, 1, resource_key, lock_val)
        except Exception:
            self._logger.exception(
                '_do_unlock failed, resource_key: %s, lock_val: %s' %
                (resource_key, lock_val, ))
            return False

    def _do_force_unlock(self, resource_key):
        self._logger.debug(
            'RedisLocker::_do_force_unlock rsrc_key: %s' %
            (resource_key, ))
        try:
            return self._con.delete(resource_key)
        except Exception:
            self._logger.exception(
                '_do_force_unlock failed, resource_key: %s' %
                (resource_key, ))
            return False

    def lock(self, resource_key=None, ttl_sec=None, retry_count=None):
        resource_key = resource_key or self._resource_key
        if not resource_key:
            raise Exception('Missing resource_key')
        resource_key = '%s:%s' % (self._key_prefix, resource_key, )
        lock_val = str(uuid.uuid4())
        return self._do_lock(
            resource_key, lock_val, ttl_sec or self._ttl_sec,
            retry_count or self._retry_count)

    def _do_lock(self, resource_key, lock_val, ttl_sec, retry_count):
        retry = 0
        ttl = ttl_sec * 1000

        # Add 2 milliseconds to the drift to account for Redis expires
        # precision, which is 1 millisecond, plus 1 millisecond min
        # drift for small TTLs.
        drift = int(ttl * self._clock_drift_factor) + 2

        while retry < retry_count:
            locked = True
            start_time = int(time.time() * 1000)
            locked = self._do_set_lock(resource_key, lock_val, ttl)
            elapsed_time = int(time.time() * 1000) - start_time
            validity = int(ttl - elapsed_time - drift) / 1000
            if validity > 0 and locked:
                return Lock(validity, resource_key, lock_val)
            else:
                self._do_unlock(resource_key, lock_val)
                retry += 1
                time.sleep(self._retry_delay)
        return False

    def _do_set_relock(self, resource_key, lock_val, ttl):
        self._logger.debug(
            'RedisLocker::_do_set_relock rsrc_key: %s, lock_val: %s, ttl: %s' %
            (resource_key, lock_val, ttl, ))
        try:
            return self._con.eval(
                self._relock_script, 1, resource_key, [lock_val, ttl, ])
        except Exception:
            self._logger.exception(
                '_do_set_relock failed, resource_key: %s, lock_val: %s' %
                (resource_key, lock_val, ))
            return False

    def _do_relock(self, resource_key, lock_val, ttl_sec, retry_count):
        ttl = ttl_sec * 1000
        drift = int(ttl * self._clock_drift_factor) + 2
        start_time = int(time.time() * 1000)
        relocked = self._do_set_relock(resource_key, lock_val, ttl)
        elapsed_time = int(time.time() * 1000) - start_time
        validity = int(ttl - elapsed_time - drift) / 1000
        if validity > 0 and relocked:
            return Lock(validity, resource_key, lock_val)
        else:
            self._do_unlock(resource_key, lock_val)
            return self._do_lock(resource_key, lock_val, ttl_sec, retry_count)

    def unlock(self, lock):
        return self._do_unlock(lock.resource_key, lock.lock_val)

    def relock(self, lock, ttl_sec=None, retry_count=None):
        return self._do_relock(
            lock.resource_key, lock.lock_val, ttl_sec or self._ttl_sec,
            retry_count or self._retry_count)

    def force_unlock(self, resource_key):
        return self._do_force_unlock(resource_key)
