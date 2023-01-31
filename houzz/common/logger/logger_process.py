#!/usr/bin/env python
from __future__ import absolute_import, print_function

import functools
import logging
import logging.handlers
import multiprocessing
import multiprocessing.pool

from houzz.common.logger.queue_handler import QueueHandler

class LoggerProcess(multiprocessing.Process):
    _global_process_logger = None

    def __init__(self):
        super(LoggerProcess, self).__init__()
        self.queue = multiprocessing.Queue(-1)

    @classmethod
    def get_global_logger(cls):
        if cls._global_process_logger is not None:
            return cls._global_process_logger
        return None

    @classmethod
    def create_global_logger(cls):
        if cls._global_process_logger:
            raise Exception("The global LoggerProcess should be a singleton.")
        cls._global_process_logger = LoggerProcess()
        return cls._global_process_logger

    def stop(self):
        self.queue.put_nowait(None)

    def run(self):
        while True:
            try:
                record = self.queue.get()
                if record is None:
                    break
                logger = logging.getLogger(record.name)
                logger.handle(record)
            except Exception:
                import sys, traceback
                print('Whoops! Problem:', file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

    def new_process(self, target, args=[], kwargs={}):
        return ProcessWithLogging(self, target, args, kwargs)

# TODO: use standard library after migated to python 3.
class ContextDecorator(object):
    def __call__(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwds):
            with self:
                return f(*args, **kwds)
        return decorated


def stopLoggerProcess():
    logger_process = LoggerProcess.get_global_logger()
    if logger_process:
        logger_process.stop()
        logger_process.join()


class loggerProcessRunning(ContextDecorator):
    def __enter__(self):
        logger_process = LoggerProcess.create_global_logger()
        logger_process.start()
        
    def __exit__(self, *exc):
        stopLoggerProcess()


def configure_new_process(log_process_queue):
    root = logging.getLogger() # configure root logger should have no harm here.
    root.addHandler(QueueHandler(log_process_queue))


class ProcessWithLogging(multiprocessing.Process):
    def __init__(self, target, args=[], kwargs={}, log_process=None):
        super(ProcessWithLogging, self).__init__()
        self.target = target
        self.args = args
        self.kwargs = kwargs
        if log_process is None:
            log_process = LoggerProcess.get_global_logger()
        if log_process is None:
            raise Exception("Logger Process should start before run any ProcessWithLogging instance!")
        self.log_process_queue = log_process.queue

    def run(self):
        configure_new_process(self.log_process_queue)
        self.target(*self.args, **self.kwargs)
