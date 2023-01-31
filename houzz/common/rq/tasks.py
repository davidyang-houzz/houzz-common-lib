# -*- coding: utf-8 -*-
# @Author: Zviki Cohen
# @Date:   2016-06-29 11:29:47
# @Last Modified by:   Zviki Cohen
# @Last Modified time: 2018-03-28 10:46:17

from __future__ import absolute_import
import logging
import json
import time
from abc import ABCMeta, abstractmethod
import six


class BaseTask(object):
    __meta__ = ABCMeta

    _config_required = True
    _logger = None
    _config = None
    _config_worker = None

    _limit_trials = None
    _limit_trials_silent_failure = False

    _mutex_enable = False
    _mutex_ttl = 60 * 60

    _task_exec_method = '_do_execute'

    _metric_client = None
    _metric_service_name = 'Unknown'
    _metric_client_enable = False

    @classmethod
    def init_post_load(cls, *args, **kwargs):
        cls._logger = logging.getLogger(cls.__name__)
        if cls._config_required:
            cls._config = cls._init_config()
        cls._env_key = kwargs.get('env_key', None)
        cls._config_worker = {}
        daemon_config = None
        if 'daemon_config' in kwargs:
            daemon_config = kwargs['daemon_config']
            if daemon_config and 'config_worker' in daemon_config:
                cls._config_worker = daemon_config.config_worker or {}
            if cls._config_worker and not cls._env_key:
                if 'ENV_KEY' in cls._config_worker:
                    cls._env_key = cls._config_worker.ENV_KEY
                elif 'env_key' in cls._config_worker:
                    cls._env_key = cls._config_worker.env_key
        cls._logger.info(
            'BaseTask: Initialized task class: %s, config: %s' %
            (cls.__name__, cls._config_worker, ))
        cls._logger.info(
            'BaseTask: env_key: %s' % (cls._env_key, ))
        if cls._mutex_enable:
            cls._init_mutex(**kwargs)
        if cls._metric_client_enable and daemon_config:
            cls._init_metric_client(daemon_config)
        cls._do_init_post_load(cls, *args, **kwargs)

    @classmethod
    def _init_config(cls):
        from houzz.common.config import get_config
        return get_config()

    @classmethod
    def _init_mutex(cls, **kwargs):
        from houzz.common.data_access.redis_con import RedisConnection
        from houzz.common.locking.redis_lock import RedisLocker
        cls._locker = RedisLocker.from_config(
            cls._config_worker, env_key=cls._env_key,
            connection=RedisConnection())

    @classmethod
    def _do_init_post_load(cls, *args, **kwargs):
        pass

    _SERVICE_NAME = 'push.native'

    @classmethod
    def _init_metric_client(cls, config_worker):
        try:
            if config_worker.dis_perf:
                cls._logger.debug('BaseTask skipping metric_client')
            else:
                from houzz.common.metric.client import init_metric_client
                cls._metric_client = init_metric_client(
                    collector_uds=config_worker.metric_uds,
                    service_name=cls._metric_service_name)
                cls._logger.debug('BaseTask created metric_client')
        except Exception:
            cls._logger.exception(
                'BaseTask failed to create metric_client')

    def execute(self, *args, **kwargs):
        result = lock = None
        skip = False
        self._jid = kwargs.get('_jid', None)
        if self._limit_trials:
            curr_trial = kwargs.get('_trial', None)
            if curr_trial and curr_trial > self._limit_trials:
                skip = True
                self._logger.info(
                    ('BaseTask: skipping task, max trials exceeded: ' +
                        '%s with args: %s, kwargs: %s') %
                    (self.__class__.__name__, args, kwargs, ))
                if self._limit_trials_silent_failure:
                    pass
                else:
                    raise Exception('max trials exceeded')
        if not skip:
            job_data = kwargs.get('job', None)
            if job_data and isinstance(job_data, six.string_types):
                job_data = json.loads(job_data)
            job_data = job_data or {}
            if self._mutex_enable:
                lock = self._mutex_lock(**job_data)
                if not lock:
                    skip = True
        if not skip:
            try:
                self._logger.info(
                    'BaseTask: Starting task: %s with args: %s, kwargs: %s' %
                    (self.__class__.__name__, args, kwargs, ))
                start_time = time.time()
                result = getattr(self, self._task_exec_method)(**job_data)
                total_time = round(time.time() - start_time, 3)
                self._logger.info(
                    'BaseTask: Finished task: %s, result: %s, total_time: %s' %
                    (self.__class__.__name__, result, total_time, ))
            except Exception:
                self._logger.exception(
                    'BaseTask: Failed task: %s with args: %s, kwargs: %s' %
                    (self.__class__.__name__, args, kwargs, ))
                raise
            finally:
                if self._mutex_enable:
                    self._mutex_unlock(lock)
        return result

    def _mutex_lock(self, **job_data):
        lock = None
        resource_key = self._mutex_resource_key(**job_data)
        self._logger.debug(
            'BaseTask: locking mutex, resource_key: %s' % (resource_key, ))
        try:
            lock = self._locker.lock(resource_key, self._mutex_ttl)
        except Exception:
            self._logger.exception(
                'BaseTask: Failed to lock mutex, resource_key: %s' %
                (resource_key, ))
        if lock:
            self._logger.info(
                'BaseTask: Locked mutex for task, resource_key: %s, lock: %s' %
                (resource_key, lock, ))
        else:
            self._mutex_lock_failed(resource_key, **job_data)
        return lock

    def _mutex_lock_failed(self, resource_key, **job_data):
        self._logger.info(
            'BaseTask: Cannot lock mutex, resource_key: %s' %
            (resource_key, ))

    def _mutex_unlock(self, lock):
        if lock:
            self._logger.info(
                'BaseTask: Unlocking mutex, lock: %s' % (lock, ))
            try:
                unlocked = self._locker.unlock(lock)
                if not unlocked:
                    self._logger.warning(
                        'BaseTask: Cannot unlock mutex, lock: %s' % (lock, ))
            except Exception:
                self._logger.exception(
                    'BaseTask: Cannot unlock mutex, lock: %s' % (lock, ))
        else:
            self._logger.error('Unlock called without key')

    def _mutex_resource_key(self, **job_data):
        extra = self._mutex_resource_key_extra(**job_data)
        return ('%s:%s' % (self.__class__.__name__, extra, )) \
            if extra else self.__class__.__name__

    def _mutex_resource_key_extra(self, **job_data):
        return None

    @abstractmethod
    def _do_execute(self, *args, **kwargs):
        pass
