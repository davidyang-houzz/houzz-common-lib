#!/usr/bin/env python

from __future__ import absolute_import
import os
import re

from houzz.common.aws.s3.syncer.base import S3SyncWorkerBase
from houzz.common.aws.s3.syncer.uploader import S3UploadMaster


class S3DumpUploadWorker(S3SyncWorkerBase):
    """Worker dequeue jobs and do the S3 operation on dump files."""

    def post_run(self, job):
        """Overload default no-op to log metric by job's key_prefix."""
        super(S3DumpUploadWorker, self).post_run(job)

        # We take off 'operation' tag to be within the max_num_tags limit.
        tags = self.get_metric_tag_from_job(
            job,
            fields=['platform', 'region', 'success', 'key_prefix']
        )
        self.log_counter('processed_dump_job', 1, tags=tags)


class S3DumpUploadMaster(S3UploadMaster):
    """Master figures out what dump files to upload and enqueue them as jobs."""

    MYSQL_DUMP_FILE_PATH_PTTN = re.compile(r'.*/innobackup.*.tar.gz.\d{14}$')
    REDIS_DUMP_FILE_PATH_PTTN = re.compile(r'.*/.*dump.rdb.(\d{14})$')
    BUCKET_NAME = 'dumps.houzz.com'

    def __init__(self, **kwargs):
        # use customized worker
        kwargs['worker_cls'] = S3DumpUploadWorker
        super(S3DumpUploadMaster, self).__init__(**kwargs)

    @classmethod
    def generate_bucket_name(cls, _file_path):
        """Get the s3 bucket the file should go to."""
        return cls.BUCKET_NAME

    @classmethod
    def generate_key_name(cls, file_path):
        """Get the s3 key for the file."""
        bn = os.path.basename(file_path)
        return os.path.join(bn.split('.')[0], bn)

    @classmethod
    def generate_bucket_key_prefix(cls, file_path):
        """Get the s3 bucket key prefix for the file."""
        return os.path.basename(file_path).split('.')[0]

    @classmethod
    def need_sync(cls, file_path):
        """Return True if the file should be uploaded."""
        if os.path.exists(cls.get_status_done_file_path(file_path)):
            # The corresponding done file has to exist.
            if cls.MYSQL_DUMP_FILE_PATH_PTTN.match(file_path):
                # for MySQL dump, we just upload since its daily dump.
                return True
            redis_mg = cls.REDIS_DUMP_FILE_PATH_PTTN.match(file_path)
            if redis_mg:
                # for Redis dump, take one every 3 hours.
                return int(redis_mg.group(1)[8:10]) % 3 == 0
        return False

    @classmethod
    def get_status_done_file_path(cls, abs_path):
        return cls.STATUS_FILE_TMPL_DONE % abs_path

    @classmethod
    def get_status_uploading_file_path(cls, abs_path):
        return cls.STATUS_FILE_TMPL_UPLOADING % abs_path

    @classmethod
    def get_status_compressed_file_path(cls, abs_path):
        # no compressed status file for dumps
        return ''
