# -*- coding: utf-8 -*-
# @Author: Zviki Cohen
# @Date:   2018-03-22 14:38:45
# @Last Modified by:   Zviki Cohen
# @Last Modified time: 2018-03-28 12:28:49
from __future__ import absolute_import
import logging
import functools
import time
import threading

import redis


class RedisConnection(object):

    _conn_by_thread = {
        True: {

        },
        False: {

        }
    }

    _CONN_KWARGS = {
        'max_connections': 256,
        'socket_timeout': 4.0,  # read/write timeout
        'socket_connect_timeout': 2.0,  # connection timeout
        'socket_keepalive': True,  # tcp keepalive for idle connections
        'retry_on_timeout': True,
    }

    def __init__(self, master=True, logger=None):
        self._logger = logger or logging.getLogger(__name__)
        self._thread_id = curr_thread = threading.current_thread().ident
        connections = self._conn_by_thread[master]
        if curr_thread in connections:
            self._conn = connections[curr_thread]
            self._logger.info(
                'RedisConnection, (tid: %s) reused: %s' %
                (self._thread_id, self._conn, ))
        else:
            # late import to avoid early intialization of configs
            from houzz.common import redis_utils
            if master:
                self._conn = redis_utils.get_redis_main_master_connection(
                    strict=True, **self._CONN_KWARGS)
            else:
                self._conn = redis_utils.get_redis_main_slave_connection(
                    strict=True, **self._CONN_KWARGS)
            connections[curr_thread] = self._conn
            self._logger.info(
                'RedisConnection, (tid: %s) opened: %s' %
                (self._thread_id, self._conn, ))
        self._proxies = {}

    _MAX_ATTEMPTS = 2

    def _run_conn_method(self, method_name, *args, **kwargs):
        attempt = 0
        method = getattr(self._conn, method_name, None)
        # self._logger.debug('Redis running: %s' % (method_name, ))
        while True:
            start = time.time()
            try:
                result = method(*args, **kwargs)
                elapsed = int(round((time.time() - start) * 1000))
                self._logger.debug(
                    'Redis: exec method: %s, elapsed: %sms' %
                    (method_name, elapsed, ))
                return result
            except redis.TimeoutError:
                elapsed = int(round((time.time() - start) * 1000))
                if attempt >= self._MAX_ATTEMPTS:
                    self._logger.warn(
                        ('Redis: timeout, giving up, method: %s, '
                            'elapsed: %sms, attempt: %s') %
                        (method_name, elapsed, attempt, ))
                    raise
                else:
                    self._logger.warn(
                        ('Redis: timeout, will retry, method: %s, '
                            'elapsed: %sms, attempt: %s') %
                        (method_name, elapsed, attempt, ))
                    attempt += 1
                    time.sleep(0.5 * attempt)

    def _create_proxy(self, method_name):
        method = getattr(self._conn, method_name, None)
        if not method:
            raise AttributeError('method not found: %s' % (method_name, ))
        return functools.partial(self._run_conn_method, method_name)

    def __getattr__(self, name):
        if name not in self._proxies:
            self._proxies[name] = self._create_proxy(name)
        return self._proxies[name]

    def __repr__(self):
        return 'RedisConnection (tid: %s, connection: %s)' %\
            (self._thread_id, self._conn, )

    @property
    def connection(self):
        return self._conn
