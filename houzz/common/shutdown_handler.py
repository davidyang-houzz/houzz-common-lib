#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import atexit
import logging
import signal
import threading
import traceback
from collections import defaultdict

shutdown_handlers = defaultdict(list)
initialized = False
lock = threading.RLock()

logger = logging.getLogger('shutdown_handler')

# Map of signal number to names.
SIGNAL_NAMES = {
    getattr(signal, n): n
    for n in dir(signal) if n.startswith('SIG') and '_' not in n
}

def get_func_name_safe(func):
    return getattr(func, '__name__', 'unknown')


def get_func_module_safe(func):
    return getattr(func, '__module__', 'unknown')


def _execute_shutdown_handlers():
    # Make a copy of the handlers, so we can ignore handlers added after shutdown starts.
    # Also reverse the order, because most likely objects created later depends on objects
    # created earlier, so we should cleanup them first.

    handlers = shutdown_handlers.copy()
    for scope in handlers:
        for func in handlers[scope]:
            logger.info('calling shutdown_handler: scope:%s, %s::%s',
                        scope,
                        get_func_module_safe(func),
                        get_func_name_safe(func))
            func()
    logger.info('done executing shutdown handlers')


def _signal_shutdown(sig_num, stack):
    logger.info('handle shutdown due to signal %s.',
                SIGNAL_NAMES.get(sig_num, sig_num))
    _execute_shutdown_handlers()
    logger.info(''.join(traceback.format_stack(stack)))


def _normal_exit():
    logger.info('handle normal interpreter exit.')
    _execute_shutdown_handlers()


def remove_shutdown_handlers(scope=None):
    scope = scope or 'root'
    with lock:
        if not initialized:
            logger.info('Shutdown handler was not initialized')
            return
        logger.info('remove shutdown_handler: scope:%s',
                    scope)
        shutdown_handlers[scope] = []


def add_shutdown_handler(func, scope=None):
    scope = scope or 'root'
    with lock:
        if not initialized:
            logger.info('Shutdown handler was not initialized')
            init_shutdown_handler()

        if func in shutdown_handlers[scope]:
            logger.info('skip adding already added shutdown_handler: scope:%s, %s::%s',
                        scope,
                        get_func_module_safe(func),
                        get_func_name_safe(func))
        else:
            shutdown_handlers[scope].append(func)
            logger.info('added shutdown_handler: scope:%s, %s::%s',
                        scope,
                        get_func_module_safe(func),
                        get_func_name_safe(func))


def init_shutdown_handler(sig_nums=None):
    """Initialize shutdown handler for the given list of signals.

    NOTE: 1) This has to be called from the main thread.
          2) The signal is not caught in py.test. You have to manually cleanup
             in unittests.

    Args:
        sig_nums (list(signal.SIG*)): signals to trigger the shutdown handler
    """
    global initialized

    if sig_nums:
        assert isinstance(sig_nums, list), 'sig_nums need to be a list of signal.SIG*'
    else:
        sig_nums = [signal.SIGTERM, signal.SIGINT, signal.SIGQUIT]

    with lock:
        for sig_num in set(sig_nums):
            logger.info('registering shutdown handler for signal %s',
                        SIGNAL_NAMES[sig_num])
            signal.signal(sig_num, _signal_shutdown)

        # also register for normal interpreter exit
        if not initialized:
            atexit.register(_normal_exit)
        initialized = True
