# Thread pool wrapper for boto3.
#
# Apparently, boto3 is NOT thread-safe.  Also, we frequently need to run several
# AWS requests in parallel.  This is a helper class that creates a thread pool,
# where each thread gets its own boto3 session and client.  You can simply
# "call" the thread pool in the same way you'd use boto3, which will enqueue an
# asynchronous operation: later, you can call result() to wait for completion
# and/or retrieve the result.
#
# When using this module, we recommend using a global variable to keep the same
# thread pool during program execution: you probably don't want to keep creating
# and destroying thread pools.  (It's OK to have multiple instances of this
# class, if you need to.)
#
# For boto3's thread-(un)safety, see:
# cf. https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-multiprocessing
#
# ========== Example code ==========
#
#   # When the program starts:
#   s3_pool = boto3_thread_pool.Boto3ThreadPool('s3',
#       aws_access_key_id=...,
#       aws_secret_access_key=...)
#
#   # When you have S3 operations: these requests will run in parallel.
#   # You can call any function that's supported by boto3.client('s3').
#   request1 = s3_pool.upload_file(file1, bucket, key1)
#   request2 = s3_pool.upload_file(file2, bucket, key2)
#   request3 = s3_pool.upload_file(file3, bucket, key3)
#
#   # You MUST call result(), even when you don't need the result: otherwise
#   # there's no guarantee that the operation will finish!
#   # Any exception that occurs during the operation will be propagated to
#   # the caller of result().
#   result1 = request1.result()
#   result2 = request2.result()
#   result3 = request3.result()
#
# ==================================

from __future__ import absolute_import
import copy
import logging
import threading

import concurrent.futures
import boto3

logger = logging.getLogger()

THREAD_POOL_SIZE = 10
THREAD_NAME_PREFIX = 'Boto3Pool'

class Boto3ThreadPool(object):
    # All arguments are passed directly to boto3.client(), except for
    # 'max_workers' and 'thread_name_prefix', which can be used to instantiate
    # ThreadPoolExecutor.
    def __init__(self, client_type, *args, **kwargs):
        # The first argument to boto3.client() should be the type of the client,
        # e.g., 's3' or 'mediaconvert'.
        self.client_type = client_type

        # See the comments inside __getattr__() below.
        args = copy.deepcopy(args)
        kwargs = copy.deepcopy(kwargs)

        max_workers = kwargs.pop('max_workers', THREAD_POOL_SIZE)
        thread_name_prefix = kwargs.pop(
            'thread_name_prefix', THREAD_NAME_PREFIX)

        logger.info(
            'Creating boto3 thread pool (%s, max_workers=%d, thread_name_prefix=%s) ...',
            self.client_type, max_workers, thread_name_prefix)

        self.thr_local = threading.local()
        self.client_args = args
        self.client_kwargs = kwargs

        # Apparently older version of concurrent.futures (3.0.5) doesn't
        # understand 'thread_name_prefix'.  Although we require 3.2.0 in BUILD
        # file, because we need 'inherit_path' for some PEX binaries, apparently
        # these binaries still manage to find an older system library and die
        # here.
        #
        # Let's guard against it until we can get rid of PEX and/or Python 2.7.
        try:
            self.pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix=thread_name_prefix)
        except:
            self.pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers)

    # A python magic to dynamically generate class attributes.
    #
    # E.g., if the user calls pool.upload_file(...), then Python calls
    # pool.__getattr__('upload_file'), and we basically generate the
    # upload_file() function on the fly: when called, the function will
    # "enqueue" the job to the thread pool and return a Future object.
    def __getattr__(self, name):
        def _fn(*args, **kwargs):
            # Because boto3 methods are executed inside a thread pool, if the
            # input contains lists or dicts, the value may change before the
            # function is actually executed.  E.g.,
            #
            # extra_args = {'foo': 'bar', ...}
            # thread_pool.upload_file(..., ExtraArgs=extra_args)
            # extra_args['foo'] = 'new value'  # Boom!
            #
            # Using deepcopy() is rather ugly, but it should catch most common
            # cases.
            args = copy.deepcopy(args)
            kwargs = copy.deepcopy(kwargs)
            return self.pool.submit(
                lambda: self._run_in_thread(name, args, kwargs))
        return _fn

    # In case you want to run something more complicated than a single boto3
    # call (e.g., catching exceptions).  'func' should be a function that takes
    # a single argument, which is the client.
    #
    # Example usage:
    #       bucket, key = ..., ...
    #       def _fn(s3_client):
    #           try:
    #               return s3_client.head_object(Bucket=bucket, Key=key)
    #           except s3_client.exceptions.ClientError:
    #               return None
    #       future = s3_pool.submit_job(_fn)
    def submit_job(self, func):
        def _call_func_in_thread():
            client = self._get_or_create_client()
            return func(client)
        return self.pool.submit(_call_func_in_thread)

    # Internal function to run a given boto3 function inside the thread pool.
    def _run_in_thread(self, name, args, kwargs):
        client = self._get_or_create_client()
        fn = getattr(client, name)
        return fn(*args, **kwargs)

    # Internal function to create a boto3 client for the current thread, if
    # necessary.  Do not call directly!
    def _get_or_create_client(self):
        try:
            return self.thr_local.client
        except AttributeError:
            logger.info('Creating boto3 (%s) client for thread %s ...',
                        self.client_type, threading.current_thread().name)

            # We can't just call boto3.client(), because it reuses the default
            # session: so all threads will end up sharing the same default
            # session, a situation we are trying to avoid in the first place.
            self.thr_local.session = boto3.session.Session()
            self.thr_local.client = self.thr_local.session.client(
                self.client_type, *self.client_args, **self.client_kwargs)

            return self.thr_local.client
