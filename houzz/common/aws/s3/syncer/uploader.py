#!/usr/bin/env python

"""Uploader classes to support uploading from local disk to S3."""

from __future__ import absolute_import
import os

from twitter.common import log

from houzz.common.aws.s3.syncer.base import S3SyncMasterBase
from houzz.common.aws.s3.syncer.job import Job
import six


class S3UploadMaster(S3SyncMasterBase):
    """Master figures out what files to upload and enqueue them as jobs."""

    def __init__(self, **kwargs):
        super(S3UploadMaster, self).__init__(**kwargs)
        assert 'root_dir' in kwargs, 'root_dir is not specified'
        self.root_dir = os.path.normpath(kwargs['root_dir'])  # remove potential trailing '/'
        assert os.path.exists(self.root_dir), 'No such root_dir locally: %s' % self.root_dir
        self.num_files_to_upload = 0

    @classmethod
    def generate_bucket_name(cls, file_path):
        """Get the s3 bucket the file should go to. Not Implemented."""
        raise NotImplementedError

    @classmethod
    def generate_key_name(cls, file_path):
        """Get the s3 key for the file. Not Implemented."""
        raise NotImplementedError

    @classmethod
    def generate_bucket_key_prefix(cls, file_path):
        """Get the s3 bucket key prefix for the file. Not Implemented."""
        raise NotImplementedError

    @classmethod
    def need_sync(cls, file_path):
        """Return True if the file should be uploaded. Not Implemented."""
        raise NotImplementedError

    def create_jobs(self, abs_path):
        """Create upload job for the given file."""
        if os.path.exists(self.get_status_done_file_path(abs_path)):
            # if there is a .done file, change that to .uploading, so other
            # scripts won't try to update/move/delete it while we are in-fly.
            os.rename(self.get_status_done_file_path(abs_path),
                      self.get_status_uploading_file_path(abs_path))
        job = Job(
            operation=Job.OPERATION_UPLOAD,
            platform=Job.PLATFORM_AMAZON,
            region=self.session.region_name,
            bucket_name=self.generate_bucket_name(abs_path),
            key_name=self.generate_key_name(abs_path),
            key_prefix=self.generate_bucket_key_prefix(abs_path),
            abs_path=abs_path,
            status_done_path=self.get_status_done_file_path(abs_path),
            status_compressed_path=self.get_status_compressed_file_path(abs_path),
            status_uploading_path=self.get_status_uploading_file_path(abs_path),
            file_size=os.path.getsize(abs_path),
            dry_run=self.dry_run,
            success=False,
            num_trial=0,
            delete_after_upload=self.delete_local_file_after_upload,
            kwargs=None
        )
        self.num_files_to_upload += 1
        log.debug('new upload job: %r', job)
        self.queue.put(job)
        return 1

    def enqueue_jobs(self):
        """Find all files to be uploaded, then create corresponding jobs."""
        # loop through top level sub-dir under root, and process one sub-dir
        # at a time. This is to save the memory usage during the run.
        # store [(subdir, recurse)] for each subdir.
        subdirs = [(os.path.join(self.root_dir, i), True)
                   for i in os.walk(self.root_dir).next()[1]]
        # also need to work on files under self.root_dir
        subdirs.append((self.root_dir, False))
        self.log_counter('num_sub_dirs', len(subdirs))
        log.info('Need to work on (top_dir, recurse):%s', subdirs)

        for root_dir, recurse in subdirs:
            # get local files
            local_file_info = {}  # s3_key => (size, abs_path)
            prefixes = set()  # set((bucket_name, prefix))
            log.info('start on root dir:%s, recurse:%s', root_dir, recurse)
            for root, dirs, files in os.walk(root_dir):
                log.debug('working on dir:%s', root)
                if not recurse:
                    # We do NOT want to get into any subdirs.
                    log.info('IGNORE any subdir under:%s', root)
                    dirs[:] = []
                for file_path in files:
                    abs_path = os.path.join(root, file_path)
                    if not self.need_sync(abs_path):
                        continue
                    b = self.generate_bucket_name(abs_path)
                    k = self.generate_key_name(abs_path)
                    p = self.generate_bucket_key_prefix(abs_path)
                    local_file_info[k] = (os.path.getsize(abs_path), abs_path)
                    prefixes.add((b, p))
            log.info('total local files:%s, total s3 prefix:%s',
                     len(local_file_info), len(prefixes))
            self.log_counter('num_local_files', len(local_file_info))
            self.log_counter('num_s3_prefix', len(prefixes))

            # get s3 files
            s3_file_info = {}  # s3_key => (size, bucket_name)

            def callback(bucket, page_result, output):
                if 'Contents' not in page_result:
                    return
                for r in page_result['Contents']:
                    output[r['Key']] = (r['Size'], bucket.name)

            for bucket_name, prefix in prefixes:
                log.info('listing s3 keys for prefix:%s', prefix)
                self.list_bucket(bucket_name, callback, s3_file_info, prefix=prefix)
            log.info('total s3 files under the prefixes:%s', len(s3_file_info))
            self.log_counter('num_files_under_s3_prefix', len(s3_file_info))

            local_files = set(six.iterkeys(local_file_info))
            s3_files = set(six.iterkeys(s3_file_info))

            # upload new local files
            num_new_files = 0
            for k in local_files - s3_files:
                _file_size, abs_path = local_file_info[k]
                num_new_files += self.create_jobs(abs_path)
            log.info('total new s3 file to create:%s', num_new_files)
            self.log_counter('num_new_files', num_new_files)

            # update shared files if size changes
            num_update_files = 0
            if self.update_existing_file:
                for k in local_files & s3_files:
                    remote_size, bucket_name = s3_file_info[k]
                    local_size, abs_path = local_file_info[k]
                    log.debug('key:%s, path:%s, bucket:%s, local_size:%s, remote_size:%s',
                              k, abs_path, bucket_name, local_size, remote_size)
                    if local_size != remote_size:
                        num_update_files += self.create_jobs(abs_path)
                        log.debug('existing file to update:%s', abs_path)
                log.info('total existing files to update:%s', num_update_files)
                self.log_counter('num_update_files', num_update_files)

            log.info('under root_dir:%s, total files to upload:%s',
                     root_dir, num_new_files + num_update_files)

            # release memory
            del local_files, s3_files, local_file_info, s3_file_info
        log.info('Overall number of files to upload:%s', self.num_files_to_upload)
        self.log_counter('total_num_files_to_upload', self.num_files_to_upload)
