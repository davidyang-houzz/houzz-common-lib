#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import copy
import logging

from twitter.common import (
    app,
    log,
    options,
)

from houzz.common import (
    process_utils,
    shutdown_handler,
)
from houzz.common.module.config_module import ConfigModule
from houzz.common.module.metric_module import MetricModule


class S3SyncerModule(app.Module):

    OPTIONS = {
        'max_allowed_seconds': options.Option(
            '-t', '--max_allowed_seconds',
            dest='max_allowed_seconds',
            type='int',
            default=86400,
            help='Number of seconds the process is allowed to run. Default to 1-day'
        ),
        'num_worker_threads': options.Option(
            '-n', '--num_worker_threads',
            dest='num_worker_threads',
            type='int',
            default=8,
            help='Number of concurrent processes to upload. Default to 8'
        ),
        'root_dir': options.Option(
            '-d', '--root_dir',
            dest='root_dir',
            default='/home/clipu/c2/backup_log',
            help='root dir path to sync to s3'
        ),
        'dry_run': options.Option(
            '--dry_run',
            dest='dry_run',
            action='store_true',
            help='dry run without do real upload'
        ),
        's3_max_concurrency': options.Option(
            '--s3_max_concurrency',
            dest='s3_max_concurrency',
            type='int',
            default=10,
            help='The maximum number of threads that will be '
                 'making requests to perform a transfer'
        ),
        's3_max_io_queue': options.Option(
            '--s3_max_io_queue',
            dest='s3_max_io_queue',
            type='int',
            default=1000,
            help='The maximum amount of parts that can be queued in memory'
        ),
        's3_multipart_threshold': options.Option(
            '--s3_multipart_threshold',
            dest='s3_multipart_threshold',
            type='int',
            default=8 * 1024 * 1024,
            help='The transfer size threshold for which multipart will be triggered'
        ),
        's3_multipart_chunksize': options.Option(
            '--s3_multipart_chunksize',
            dest='s3_multipart_chunksize',
            type='int',
            default=8 * 1024 * 1024,
            help='The partition size of each part for a multipart transfer.'
        ),
        'ignore_missing_local_file': options.Option(
            '--ignore_missing_local_file',
            dest='ignore_missing_local_file',
            action='store_false',
            help='Assume the job is successful when the local file is missing.'
        ),
        'ignore_missing_s3_file': options.Option(
            '--ignore_missing_s3_file',
            dest='ignore_missing_s3_file',
            action='store_true',
            help='Assume the job is successful when the file is missing on S3.'
        ),
        'delete_local_file_after_upload': options.Option(
            '--delete_local_file_after_upload',
            dest='delete_local_file_after_upload',
            action='store_true',
            help='Delete local file after it is successfully uploaded.'
        ),
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(ConfigModule())
        app.register_module(MetricModule())

        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[
                ConfigModule.full_class_path(),
                MetricModule.full_class_path(),
            ],
            description="S3 Sync Module")

    def setup_function(self):
        if not shutdown_handler.initialized:
            shutdown_handler.init_shutdown_handler()

    def sync(self, syncer_class):
        options = app.get_options()

        # make sure only one instance is running.
        process_utils.run_only_one_instance(
            app.name(),
            options.max_allowed_seconds
        )

        # Boto lib is noisy, log more serious message only
        log.logger('botocore').setLevel(logging.WARN)
        log.logger('boto3').setLevel(logging.WARN)

        log.info('START rsync root: %s', options.root_dir)

        config = ConfigModule().config
        # always have a class tag in metric to show type of jobs
        metric_client = MetricModule().metric
        new_tags = copy.copy(metric_client.tags)
        new_tags.update({'class': syncer_class.__name__})
        metric_client.set_tags(new_tags)

        syncer = syncer_class(
            num_worker_threads=options.num_worker_threads,
            root_dir=options.root_dir,
            per_report_size=1,
            ignore_missing_local_file=options.ignore_missing_local_file,
            ignore_missing_s3_file=options.ignore_missing_s3_file,
            delete_local_file_after_upload=options.delete_local_file_after_upload,
            update_existing_file=True,
            dry_run=options.dry_run,
            access_key_id=config.AWS_PARAMS.AWS_ACCESS_KEY_ID,
            access_key=config.AWS_PARAMS.AWS_SECRET_ACCESS_KEY,
            region=config.AWS_PARAMS.S3_DEFAULT_LOCATION,
            multipart_threshold=options.s3_multipart_threshold,
            max_concurrency=options.s3_max_concurrency,
            multipart_chunksize=options.s3_multipart_chunksize,
            max_io_queue=options.s3_max_io_queue,
            metric_client=metric_client
        )

        shutdown_handler.add_shutdown_handler(syncer.cleanup)
        syncer.run()

        log.info('DONE rsync on root: %s', options.root_dir)
