#!/usr/bin/env python

class Job(object):

    OPERATION_UPLOAD = 'U'
    OPERATION_DOWNLOAD = 'D'
    OPERATION_CUSTOM = 'C'
    SUPPORTED_OPERATIONS = [OPERATION_UPLOAD, OPERATION_DOWNLOAD, OPERATION_CUSTOM]

    PLATFORM_AMAZON = 'A'
    PLATFORM_GOOGLE = 'G'
    SUPPORTED_PLATFORMS = [PLATFORM_AMAZON, PLATFORM_GOOGLE]

    __slots__ = [
        'operation',
        'platform',
        'region',
        'bucket_name',
        'key_name',
        'key_prefix',
        'abs_path',
        'status_done_path',
        'status_compressed_path',
        'status_uploading_path',
        'file_size',
        'dry_run',
        'success',
        'num_trial',
        'delete_after_upload',
        'kwargs',
    ]

    def __init__(self,
                 operation,
                 platform,
                 region,
                 bucket_name,
                 key_name,
                 key_prefix,
                 abs_path,
                 status_done_path,
                 status_compressed_path,
                 status_uploading_path,
                 file_size,
                 dry_run,
                 success,
                 num_trial,
                 delete_after_upload,
                 kwargs):
        assert operation in self.SUPPORTED_OPERATIONS, 'unknown operation: %s' % operation
        assert platform in self.SUPPORTED_PLATFORMS, 'unknown platform: %s' % platform
        super(Job, self).__init__()
        self.operation = operation
        self.platform = platform
        self.region = region
        self.bucket_name = bucket_name
        self.key_name = key_name
        self.key_prefix = key_prefix
        self.abs_path = abs_path
        self.status_done_path = status_done_path
        self.status_compressed_path = status_compressed_path
        self.status_uploading_path = status_uploading_path
        self.file_size = file_size
        self.dry_run = dry_run
        self.success = success
        self.num_trial = num_trial
        self.delete_after_upload = delete_after_upload
        self.kwargs = kwargs

    def __repr__(self):
        return (
            "Job("
            "operation=%s, "
            "platform=%s, "
            "region=%s, "
            "bucket_name=%r, "
            "key_name=%r, "
            "key_prefix=%r, "
            "abs_path=%s, "
            "status_done_path=%s, "
            "status_compressed_path=%s, "
            "status_uploading_path=%s, "
            "file_size=%s, "
            "dry_run=%s, "
            "success=%s, "
            "num_trial=%s, "
            "delete_after_upload=%s, "
            "kwargs=%s)" % (
                self.operation,
                self.platform,
                self.region,
                self.bucket_name,
                self.key_name,
                self.key_prefix,
                self.abs_path,
                self.status_done_path,
                self.status_compressed_path,
                self.status_uploading_path,
                self.file_size,
                self.dry_run,
                self.success,
                self.num_trial,
                self.delete_after_upload,
                self.kwargs
            )
        )

    def __str__(self):
        return str(dict(self))
