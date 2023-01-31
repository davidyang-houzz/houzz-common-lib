#!/usr/bin/env python

from __future__ import absolute_import
import os
import re

from houzz.common.aws.s3.syncer.uploader import S3UploadMaster


class S3LogUploadMaster(S3UploadMaster):
    """Master figures out what log files to upload and enqueue them as jobs."""

    ACCESS_LOG_FILE_PATH_PTTN = re.compile(r'.*/access.log.\d{4}-\d{2}-\d{2}-\d{2}_\d{2}_\d{2}.gz$')
    HOUZZ_LOG_FILE_PATH_PTTN = re.compile(r'.*/[a-z_]+_\d{2}_\d{2}_\d{2}__\d{2}.gz$')
    BUCKET_NAME = 'logs.houzz.com'

    def __init__(self, **kwargs):
        super(S3LogUploadMaster, self).__init__(**kwargs)

    @classmethod
    def generate_bucket_name(cls, _file_path):
        """Get the s3 bucket the file should go to."""
        return cls.BUCKET_NAME

    @classmethod
    def generate_key_name(cls, file_path):
        """Get the s3 key for the file."""

        def get_date_hour(fn):
            """return (yr, mth, day, hour)."""
            if fn.startswith('access'):
                return fn[11:15], fn[16:18], fn[19:21], fn[22:24]
            else:
                return '20' + fn[-15:-13], fn[-12:-10], fn[-9:-7], fn[-5:-3]

        assert file_path.endswith('.gz'), 'only *.gz files'
        parts = file_path.split('/')
        yr, mth, day, hr = get_date_hour(parts[-1])
        return '/'.join((parts[-2], hr, day, mth, yr, parts[-1]))

    @classmethod
    def generate_bucket_key_prefix(cls, file_path):
        """Get the s3 bucket key prefix for the file."""
        return file_path.split('/')[-2]

    @classmethod
    def need_sync(cls, file_path):
        """Return True if the file should be uploaded."""
        return (
            # It is gzipped data file
            cls.ACCESS_LOG_FILE_PATH_PTTN.match(file_path) or
            cls.HOUZZ_LOG_FILE_PATH_PTTN.match(file_path)
        ) and (
            # The corresponding .done and .compressed files exist
            os.path.exists(cls.get_status_done_file_path(file_path)) and
            os.path.exists(cls.get_status_compressed_file_path(file_path))
        )

    @classmethod
    def get_status_done_file_path(cls, abs_path):
        # `.done` is used to replace the `.gz` extension
        return cls.STATUS_FILE_TMPL_DONE % os.path.splitext(abs_path)[0]

    @classmethod
    def get_status_uploading_file_path(cls, abs_path):
        # `.uploading` is used to replace the `.gz` extension
        return cls.STATUS_FILE_TMPL_UPLOADING % os.path.splitext(abs_path)[0]

    @classmethod
    def get_status_compressed_file_path(cls, abs_path):
        # `.compressed` is used to replace the `.gz` extension
        return cls.STATUS_FILE_TMPL_COMPRESSED % os.path.splitext(abs_path)[0]
