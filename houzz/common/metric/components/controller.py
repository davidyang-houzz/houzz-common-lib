#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import copy
import logging
import threading

from houzz.common.metric.common.utils import log_func_call
import six

logger = logging.getLogger('metric.controller')


class Controller(object):

    """Controller coordinates all the components. Singleton and thread-safe."""

    # whitelist components
    COLLECTOR = 'prometheus_collector'

    SUPPORTED_COMPONENTS = set([COLLECTOR])

    # need RLock because some locked calls will trigger increment_stat
    # which will also try to lock on the same lock.
    __lock = threading.RLock()
    __instance = None

    def __new__(cls, *args, **kwargs):
        """Override __new__ to enforce singleton."""
        with cls.__lock:
            if not cls.__instance:
                logger.info('create new Controller instance')
                cls.__instance = super(Controller, cls).__new__(cls)

                cls.__instance.alive = True
                cls.__instance.__components = {}
                cls.__instance.__stats = {}
            else:
                logger.info('Controller instance already exists, reuse')
        logger.info('controller instance address: %s', cls.__instance)
        return cls.__instance

    def __init__(self, *_args, **_kwargs):
        # NOTE: This will be called everytime you construct a new Controller,
        # even it is the same instance. Don't do any initialization here.
        # Do it in __new__.
        pass

    def reset(self):
        """Used for unittest.

        py.test runs all tests in the same process, therefore use the same
        Controller instance. Use reset to cleanup.
        """
        with self.__lock:
            logger.info('reset Controller instance')
            self.__instance.alive = True
            self.__instance.__components = {}
            self.__instance.__stats = {}

    def register(self, name, obj):
        assert name in self.SUPPORTED_COMPONENTS, 'unknown component %s' % name
        with self.__lock:
            assert name not in self.__components, '%s already exists' % name
            self.__components[name] = obj
        # also register the controller with the component, so it can
        # store states through controller and shared among components.
        obj.controller = self

    def unregister(self, name):
        with self.__lock:
            assert name in self.__components, '%s does not exist' % name
            # also unregister the controller with the component
            self.__components[name].controller = None
            del self.__components[name]

    def check_alive(self):
        if not self.alive:
            logger.info('controller is already down. Ignore')
            return False
        return True

    @log_func_call
    def get_stat(self, key):
        return self.__stats.get(key, None)

    @log_func_call
    def get_all_stats(self):
        return copy.copy(self.__stats)

    def increment_stat(self, key, val, with_lock=True):
        """Increment if key exists, otherwise set the stat to value."""
        if not self.check_alive():
            return False

        def increment():
            if key in self.__stats:
                self.__stats[key] += val
            else:
                self.__stats[key] = val

        if with_lock:
            with self.__lock:
                increment()
        else:
            increment()
        return True

    def set_stat(self, key, val):
        if not self.check_alive():
            return False

        with self.__lock:
            self.__stats[key] = val
        return True

    @log_func_call
    def reset_stats(self):
        if not self.check_alive():
            return False

        with self.__lock:
            self.__stats = {}
        return True

    @log_func_call
    def snapshot(self):
            return False

    @log_func_call
    def shutdown(self, join_timeout=None):
        # TODO(zheng): consider all components' join_timeout, the join_timeout is total limit.
        if not self.check_alive():
            return False

        self.alive = False

        for name, obj in six.iteritems(self.__components):
            if hasattr(obj, 'shutdown'):
                logger.info('initiating shutdown on %s', name)
                obj.shutdown(join_timeout)
            if isinstance(obj, threading.Thread):
                logger.info('joining on %s', name)
                obj.join(0.1)
                if obj.is_alive():
                    logger.warning('%s is still alive. Ignore', name)
                else:
                    logger.info('%s is shutdown successfully', name)

        return True

    @log_func_call
    def get_dryrun_out(self):
        return pr.dryrun_out.getvalue()

    @log_func_call
    def reset_dryrun_out(self):
        pr = self.__components.get(self.PUBLISHER, None)
        if not (pr and pr.dryrun and isinstance(pr.dryrun_out, StringIO)):
            # only applicable in dryrun mode and using StringIO to hold the printouts
            return False
        pr.dryrun_out.truncate(0)
        return True
