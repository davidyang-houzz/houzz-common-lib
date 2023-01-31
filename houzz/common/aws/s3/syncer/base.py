#!/usr/bin/env python

"""Base classes to support syncing from local disk to S3."""

from __future__ import absolute_import
import threading
import time
import os
from six.moves.queue import (
    Queue,
    Empty,
)

from boto3 import Session
from boto3.s3.transfer import TransferConfig
from twitter.common import log

from houzz.common.aws.s3.syncer.exceptions import S3SyncException
from houzz.common.aws.s3.syncer.job import Job
from houzz.common.aws.s3.syncer.utils import (
    s3_bucket_exists,
    s3_file_exists,
)
from six.moves import range

DEFAULT_NUM_WORKER_THREADS = 3
MAX_NUMBER_OF_TRIES_PER_JOB = 3
PROGRESS_REPORT_CHUNK_SIZE = 1000
QUEUE_GET_BLOCK_SECONDS = 0.2

# Default AWS CLI S3 Config (http://docs.aws.amazon.com/cli/latest/topic/s3-config.html)
S3_MAX_CONCURRENCY = 10
S3_MAX_IO_QUEUE = 1000
S3_MULTIPART_THRESHOLD = 8 * 1024 * 1024
S3_MULTIPART_CHUNKSIZE = 8 * 1024 * 1024


class S3SyncerBase(object):
    """Base class for S3 sync master and worker classes."""

    def __init__(
            self,
            access_key_id='',
            access_key='',
            region='',
            per_report_size=PROGRESS_REPORT_CHUNK_SIZE,
            ignore_missing_local_file=True,
            ignore_missing_s3_file=False,
            delete_local_file_after_upload=False,
            dry_run=False,
            multipart_threshold=S3_MULTIPART_THRESHOLD,
            max_concurrency=S3_MAX_CONCURRENCY,
            multipart_chunksize=S3_MULTIPART_CHUNKSIZE,
            max_io_queue=S3_MAX_IO_QUEUE,
            metric_client=None,
            **kwargs):
        assert access_key_id and access_key and region, 'invalid s3 credentials or region'

        super(S3SyncerBase, self).__init__()

        # defaults for the session, can be overwritten when generate client from session
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.region = region
        self.per_report_size = per_report_size
        self.ignore_missing_local_file = ignore_missing_local_file
        self.ignore_missing_s3_file = ignore_missing_s3_file
        self.delete_local_file_after_upload = delete_local_file_after_upload
        self.dry_run = dry_run

        # metric related
        self.metric_client = metric_client
        self.service_name = 'syncer'

        # customize S3Transfer config to speed up
        self.transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold,
            max_concurrency=max_concurrency,
            multipart_chunksize=multipart_chunksize,
            max_io_queue=max_io_queue,
        )

        # NOTE: session and resource should *NOT* be shared across thread or process
        self._session = None
        self.resources = {}  # region => s3 resource
        self.buckets = {}    # name => bucket object

    def get_metric_tag_from_job(self, job, fields=None):
        """Create additional metric tags from the job."""
        if not fields:
            fields = [
                'operation',
                'platform',
                'region',
                'success',
            ]
        return dict((str(k), str(getattr(job, k))) for k in fields)

    def log_counter(self, key, val, tags=None):
        if self.metric_client:
            self.metric_client.log_counter(
                key, val, service_name=self.service_name, tags=tags
            )

    def log_gauge(self, key, val, tags=None):
        if self.metric_client:
            self.metric_client.log_gauge(
                key, val, service_name=self.service_name, tags=tags
            )

    def log_histogram(self, key, val, tags=None):
        if self.metric_client:
            self.metric_client.log_histogram(
                key, val, service_name=self.service_name, tags=tags
            )

    @property
    def session(self):
        if not self._session:
            self._session = Session(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.access_key,
                region_name=self.region
            )
        return self._session

    def get_resource(self, region=None, access_key_id=None, access_key=None):
        assert self.session, 'No session has been created before'
        region = region if region else self.region
        if region not in self.resources:
            self.resources[region] = self.session.resource(
                's3',
                region_name=region,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=access_key
            )
        return self.resources[region]

    def get_bucket(self, bucket_name, region, create_if_missing=True):
        """Get Bucket object.

        Create a new bucket for non-existing bucket if create_if_missing is True.
        """
        if bucket_name not in self.buckets:
            s3 = self.get_resource(region=region)
            bucket = s3.Bucket(bucket_name)
            if not s3_bucket_exists(bucket):
                if create_if_missing:
                    bucket.create()
                    # the wait may raise WaiterError after 100 seconds
                    bucket.wait_until_exists()
                    self.log_counter('create_bucket', 1)
                    log.info('created bucket:%s', bucket_name)
                else:
                    raise S3SyncException('bucket %s does not exist' % bucket_name)
            self.buckets[bucket_name] = bucket
        return self.buckets[bucket_name]

    def list_bucket(self, bucket, callback, output, prefix=None, delimiter=None):
        """List file objects under the bucket, and feed to the callback.

        TODO(zheng): verify the behavior of broken conn during iteration.
        """
        if isinstance(bucket, str):
            bucket = self.get_bucket(bucket, self.session.region_name)
        paginator = bucket.meta.client.get_paginator('list_objects')
        filter_params = {'Bucket': bucket.name}
        if prefix:
            filter_params['Prefix'] = prefix
        if delimiter:
            filter_params['Delimiter'] = delimiter
        # TODO(zheng): support query with JMESPath

        page_iterator = paginator.paginate(**filter_params)

        for page_result in page_iterator:
            log.debug('getting meta:%r', page_result)
            callback(bucket, page_result, output)
        self.log_counter('list_bucket', 1)


class S3SyncMasterBase(S3SyncerBase):
    """Master figures out what files to sync and enqueue them as jobs."""

    STATUS_FILE_TMPL_DONE = '%s.done'
    STATUS_FILE_TMPL_UPLOADING = '%s.uploading'
    STATUS_FILE_TMPL_COMPRESSED = '%s.compressed'

    def __init__(self, **kwargs):
        super(S3SyncMasterBase, self).__init__(**kwargs)
        self.queue = Queue()
        self.children = []
        self.num_worker_threads = kwargs.get('num_worker_threads',
                                             DEFAULT_NUM_WORKER_THREADS)
        self.worker_cls = kwargs.get('worker_cls', S3SyncWorkerBase)
        self.update_existing_file = kwargs.get('update_existing_file', False)
        self.kwargs = kwargs
        # `enqueue_done` event is set when all jobs have been enqueued, so worker
        # threads can quit when they see empty queue.
        self.enqueue_done = threading.Event()
        self.num_leftover_jobs = 0

    @classmethod
    def get_status_done_file_path(cls, abs_path):
        """Get the absolute path for the status done file. Not Implemented."""
        raise NotImplementedError

    @classmethod
    def get_status_uploading_file_path(cls, abs_path):
        """Get the absolute path for the status uploading file. Not Implemented."""
        raise NotImplementedError

    @classmethod
    def get_status_compressed_file_path(cls, abs_path):
        """Get the absolute path for the status compressed file. Not Implemented."""
        raise NotImplementedError

    def start_workers(self):
        """Spawn worker threads to handle jobs."""
        for _ in range(self.num_worker_threads):
            t = self.worker_cls(
                self.queue,
                self.enqueue_done,
                **self.kwargs
            )
            t.daemon = True
            t.start()
            self.children.append(t)

    def join_workers(self):
        """Wait till all jobs are handled."""
        for t in self.children:
            t.join()

    def cleanup(self):
        """Clean up any leftover jobs in the queue after all workers quit."""
        while True:
            try:
                job = self.queue.get_nowait()
                self.num_leftover_jobs += 1
                self.log_counter(
                    'leftover_job', 1, tags=self.get_metric_tag_from_job(job)
                )
                self.cleanup_job(job)
            except Empty:
                break

    def finalize(self):
        pass

    def cleanup_job(self, job):
        """Clean up on individual leftover job."""
        log.warn('Drop leftover job: %r', job)

    def run(self):
        """Start worker threads and create jobs for them."""
        log.info('start master')
        start_ts = time.time()
        self.start_workers()

        # implemented by concrete subclasses
        self.enqueue_jobs()

        # tell workers to exit after they see empty queue
        self.enqueue_done.set()

        self.join_workers()

        self.cleanup()
        if self.num_leftover_jobs:
            raise S3SyncException('All workers are dead, but queue has %d jobs left.' %
                                  self.num_leftover_jobs)
        self.finalize()
        self.log_gauge('master_total_time', int(time.time() - start_ts))
        log.info('done master')

    def enqueue_jobs(self):
        """Enqueue jobs for workers. Not implemented."""
        raise NotImplementedError


class S3SyncWorkerBase(S3SyncerBase, threading.Thread):
    """Worker dequeue jobs and do the S3 operation."""

    def __init__(self, queue, can_quit, **kwargs):
        super(S3SyncWorkerBase, self).__init__(**kwargs)
        self.queue = queue
        # master will use `can_quit` event to signal worker free to quit
        self.can_quit = can_quit
        self.processed_count = 0
        self.requeued_count = 0
        self.dropped_cout = 0
        self.max_tries_per_job = kwargs.get('max_tries_per_job',
                                            MAX_NUMBER_OF_TRIES_PER_JOB)
        self.block_seconds = kwargs.get('block_seconds',
                                        QUEUE_GET_BLOCK_SECONDS)
        self.verbose = kwargs.get('verbose', False)

    def pre_run(self, job):
        """Hook to execute before run. Default to noop."""
        pass

    def post_run(self, job):
        """Hook to execute after run."""
        if (job.operation == Job.OPERATION_UPLOAD and
                job.delete_after_upload and
                job.success):
            log.info('Remove local file %s for the successful upload job', job.abs_path)
            os.unlink(job.abs_path)

    def process_job(self, job):
        """Process the job.

        NOTE: Any unsuccessful processing should raise to signal the failure.
        """
        s3_obj = self.get_resource(region=job.region).Object(
            job.bucket_name,
            job.key_name
        )
        if job.operation == Job.OPERATION_UPLOAD:
            if os.path.exists(job.abs_path):
                if not job.dry_run:
                    s3_obj.upload_file(
                        job.abs_path,
                        Config=self.transfer_config,
                        ExtraArgs={'ACL': 'bucket-owner-full-control'}
                    )
                    # the wait may raise WaiterError after 100 seconds
                    s3_obj.wait_until_exists()
            elif self.ignore_missing_local_file:
                log.warn('local file has been removed. skip: %r', job)
            else:
                raise S3SyncException('local file does not exist: %r' % job)
        elif job.operation == Job.OPERATION_DOWNLOAD:
            if s3_file_exists(s3_obj):
                if not job.dry_run:
                    s3_obj.download_file(
                        job.abs_path,
                        Config=self.transfer_config
                    )
            elif self.ignore_missing_s3_file:
                log.warn('s3 file has been removed. skip: %r', job)
            else:
                raise S3SyncException('s3 file does not exist: %r' % job)
        else:
            raise S3SyncException('unknown operation type: %s' % job)

    def finalize_job(self, job):
        """Finalize the job regardless success or not."""
        # rename the companion status file name to allow others to work on it if necessary
        if os.path.exists(job.status_uploading_path):
            os.rename(job.status_uploading_path, job.status_done_path)

        # drop or requeue failed job
        if not job.success:
            if job.num_trial >= self.max_tries_per_job:
                log.error('Job failed too many times. Drop %r', job)
                self.dropped_cout += 1
                self.log_counter(
                    'dropped_job', 1, tags=self.get_metric_tag_from_job(job)
                )
            else:
                log.warn('Requeue failed job %r', job)
                self.queue.put(job)
                self.requeued_count += 1
                self.log_counter(
                    'requeued_job', 1, tags=self.get_metric_tag_from_job(job)
                )

    def run(self):
        """Dequeue and do the operation:(up|down)load."""
        worker_start_ts = time.time()
        while True:
            try:
                job = self.queue.get(True, self.block_seconds)
            except Empty:
                if self.can_quit.is_set():
                    break
                else:
                    continue

            if self.processed_count % self.per_report_size == 0:
                log.info('No.%s job:%r', self.processed_count + 1, job)
            elif self.verbose:
                log.debug('No.%s job:%r', self.processed_count + 1, job)

            job_start_ts = time.time()
            job.num_trial += 1
            try:
                self.pre_run(job)
                self.process_job(job)
                job.success = True  # post_run needs to know if the job succeeded
                self.post_run(job)
                self.log_gauge(
                    'num_try_to_success',
                    job.num_trial,
                    tags=self.get_metric_tag_from_job(job)
                )
                log.info('Successfully finished job %r', job)
            except:
                log.error('Failed job %r. Finished job count:%s', job, self.processed_count,
                          exc_info=1)
            finally:
                self.finalize_job(job)
                self.log_gauge(
                    'job_process_time',
                    int(time.time() - job_start_ts),
                    tags=self.get_metric_tag_from_job(job)
                )
                self.log_gauge(
                    'job_file_size',
                    job.file_size,
                    tags=self.get_metric_tag_from_job(job)
                )

            self.processed_count += 1
            self.log_counter(
                'processed_job',
                1,
                tags=self.get_metric_tag_from_job(job)
            )

        self.log_gauge('worker_total_time', int(time.time() - worker_start_ts))
        log.info('Worker: done all jobs. processed:%s, requeued:%s, dropped:%s',
                 self.processed_count, self.requeued_count, self.dropped_cout)
