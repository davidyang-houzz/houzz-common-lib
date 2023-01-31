#!/usr/bin/env python
# this helper class is for handle the sigint sigterm signal
from __future__ import absolute_import
import signal
import uuid

from . import shutdown_handler
import logging

logger = logging.getLogger(__name__)

class InterruptHandler(object):
    def __init__(self, sig=None):
        logger.debug('__init__')
        self.sig = sig if sig else [signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT] 
        self.interrupted = False
        self.released = False
        self.original_handlers = dict()
        self.scope = str(uuid.uuid4())

    def __enter__(self):
        logger.debug('__enter__')
        def handler():
            logger.debug(': handler : ')
            # self.release()
            self.interrupted = True
            logger.debug('self.interrupted :  {}'.format(self.interrupted))

        shutdown_handler.init_shutdown_handler(self.sig)
        shutdown_handler.add_shutdown_handler(handler, self.scope)

        return self

    def __exit__(self, ty, value, tb):
        logger.debug('__exit__')
        self.release()

    def release(self):
        shutdown_handler.remove_shutdown_handlers(self.scope)
        self.released = True
