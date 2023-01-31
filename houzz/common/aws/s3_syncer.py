#!/usr/bin/env python

# Sync images and files to Amazon EBS storage and create snapshots

from __future__ import absolute_import
import grp
import logging
import os
import pwd
import shutil
import tempfile
import time

from botocore.exceptions import ClientError

import houzz.common.aws.ec2_utils
import houzz.common.config
import houzz.common.operation_utils
from houzz.common.aws import s3_image_util
from houzz.common.aws.s3.syncer.base import (S3SyncMasterBase, S3SyncWorkerBase)
from houzz.common.aws.s3.syncer.job import Job
import six

logger = logging.getLogger(__name__)
Config = houzz.common.config.get_config()


PROGRESS_REPORT_CHUNK_SIZE = 1000
S3_ERROR_CODE_NO_KEY = 'NoSuchKey'
S3_ERROR_CODE_ALREADY_EXISTS = 'BucketAlreadyOwnedByYou'


# ... image files ...
def image_file_bucket_name_gen():
    """Get the s3 bucket the file should go to.
    file_path is no longer needed for generating bucket_name. But we just keep for compatibility purpose.
    """
    bucket_name = Config.S3_SYNCER_PARAMS.BUCKET_NAME
    if bucket_name in Config.S3_BUCKETS_TO_REGION_MAP:
        return bucket_name
    return None


def image_file_additional_bucket_gen():
    return Config.S3_SYNCER_PARAMS.ADDITIONAL_BUCKETS


# wasabi cloud storage: use the bucket name defined in config
def wasabi_image_file_bucket_name_gen():
    '''
    Must prepend 'wasabi:' to indicate it's a wasabi bucket or it won't be recognized.
    Since default is 's3'.
    '''
    return 'wasabi:' + Config.WASABI_PARAMS.WASABI_BUCKET_NAME

class S3SyncException(Exception):
    pass


def s3_exception_callback(ex, *pargs, **_kwargs):
    '''Refresh the connection to S3 in case of failure.'''
    logger.error('refresh s3 connection after catching exception: %s', ex)
    assert pargs
    assert isinstance(pargs[0], S3SyncerBase)
    syncer = pargs[0]
    syncer.flush_connections()


class S3SyncerBase(object):
    def __init__(
            self,
            access_key_id=Config.AWS_PARAMS.AWS_ACCESS_KEY_ID,
            access_key=Config.AWS_PARAMS.AWS_SECRET_ACCESS_KEY,
            s3_host=Config.AWS_PARAMS.S3_DEFAULT_HOST,
            location=Config.AWS_PARAMS.S3_DEFAULT_LOCATION,
            per_report_size=PROGRESS_REPORT_CHUNK_SIZE,
            **_kwargs):
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.s3_host = s3_host
        self.location = location
        self.connections = {}
        self.buckets = {}  # bucket_name => bucket, cache locally to speed up
        self.per_report_size = per_report_size
        logger.info('S3SyncerBase (id %d) initialized.', id(self))

    def get_connection(self, platform_bucket_name):
        platform_bucket_name = s3_image_util.canonicalize_platform_bucket_name(platform_bucket_name)
        if platform_bucket_name not in self.connections:
            logger.info('S3SyncerBase: creating new connection for [%s]' % platform_bucket_name)

            bucket_name = s3_image_util.extract_bucket_name(platform_bucket_name)
            # If mapping exists in config.yaml use that first, otherwise fall back to the defaults
            s3_host = self.s3_host
            access_key_id = self.access_key_id
            secret_access_key = self.access_key
            logger.info('S3SyncerBase:current connection: [%s]', str(self.connections))

            if s3_image_util.get_platform_from_bucket_name(platform_bucket_name) == 'wasabi':
                s3_host = Config.WASABI_PARAMS.WASABI_DEFAULT_HOST
                access_key_id = Config.WASABI_PARAMS.WASABI_ACCESS_KEY
                secret_access_key = Config.WASABI_PARAMS.WASABI_SECRET_ACCESS_KEY
            else:
                if bucket_name in Config.S3_BUCKETS_TO_REGION_MAP:
                    if 'HOST' in Config.S3_BUCKETS_TO_REGION_MAP[bucket_name]:
                        s3_host = f"https://{Config.S3_BUCKETS_TO_REGION_MAP[bucket_name].HOST}"
                    if 'AWS_ACCESS_KEY_ID' in Config.S3_BUCKETS_TO_REGION_MAP[bucket_name]:
                        access_key_id = Config.S3_BUCKETS_TO_REGION_MAP[bucket_name].AWS_ACCESS_KEY_ID
                    if 'AWS_SECRET_ACCESS_KEY' in Config.S3_BUCKETS_TO_REGION_MAP[bucket_name]:
                        secret_access_key = Config.S3_BUCKETS_TO_REGION_MAP[bucket_name].AWS_SECRET_ACCESS_KEY

            self.connections[platform_bucket_name] = houzz.common.aws.ec2_utils.get_boto3_s3_resource(
                access_key_id, secret_access_key, s3_host)
        logger.info('S3SyncerBase (%d) has %d connections.', id(self), len(self.connections))

        return self.connections[platform_bucket_name]

    def flush_connections(self):
        self.buckets = {}
        self.connections = {}

    def get_bucket(self, platform_bucket_name):
        '''
        :param platform_bucket_name: should be in the format of [s3|wasabi]:bucket_name
        '''
        platform_bucket_name = s3_image_util.canonicalize_platform_bucket_name(platform_bucket_name)
        '''Get bucket for the file_path, or create a new one for it.'''
        if platform_bucket_name not in self.buckets:
            logger.info('S3SyncerBase: creating new bucket [%s]' % platform_bucket_name)

            bucket_name = s3_image_util.extract_bucket_name(platform_bucket_name)
            bucket = self.get_connection(platform_bucket_name).Bucket(bucket_name)
            self.buckets[platform_bucket_name] = bucket

        logger.info('S3SyncerBase (%d) has %d buckets.', id(self), len(self.buckets))

        return self.buckets[platform_bucket_name]

    def upload_dummy_file_to_buckets(self, tmp_dir, dummy_file_name, s3_dir, bucket_names):
        dummy_file_path = os.path.join(tmp_dir, dummy_file_name)
        # Create a local non-empty tmp file. (Not empty so that we can check the upload size.)
        try:
            fp = open(dummy_file_path, 'w')
            fp.write('1')
            fp.close()
        except:
            logger.exception("Invalid tmp dir for generating tmp local dummy file: %s", dummy_file_path)
            return False

        ret = self.upload_file_to_buckets(tmp_dir, dummy_file_name, s3_dir, bucket_names)
        try:
            os.unlink(dummy_file_path)
        except OSError:
            logger.exception('ignore exception when removing local temp file')
        return ret

    def upload_file_to_buckets(self, local_dir, local_file_name, s3_dir, bucket_names):
        '''
        Upload single local file to the same dir in one or more bucket.
        If any single bucket upload file, it'll rollback all the upload.
        '''
        local_file_path = os.path.join(local_dir, local_file_name)
        key_name = os.path.join(s3_dir, local_file_name)
        max_retries = 3
        for bucket_name in bucket_names:
            bucket = self.get_bucket(bucket_name)
            retries = 0
            retry_timeout = 1
            success = False
            while not success and retries <= max_retries:
                try:
                    with open(local_file_path, 'rb') as data:
                        bucket.upload_fileobj(data, key_name)
                    success = True
                except:
                    logger.exception('cannot upload %s : %s (%s) retry: %s.',
                                     bucket_name, key_name, local_file_path, retries)
                    success = False

                if not success:
                    time.sleep(retry_timeout)
                    retry_timeout *= 2
                    retries += 1
            # Don't allow partial success. Must succeed in all buckets or none.
            if not success:
                break

        if not success:
            # rollback any already uploaded ones.
            self.delete_file_from_buckets(key_name, bucket_names)
            return False

        return True

    def delete_file_from_buckets(self, key_name, bucket_names):
        max_retries = 3
        for bucket_name in bucket_names:
            bucket = self.get_bucket(bucket_name)
            retries = 0
            retry_timeout = 1
            success = False
            while not success and retries <= max_retries:
                try:
                    bucket.delete_objects(Delete={"Objects": [{"Key": key_name}]})
                    success = True
                except ClientError as e1:
                    if e1.response['Error']['Code'] == S3_ERROR_CODE_NO_KEY:
                        # File already not exists, looks good.
                        success = True
                    else:
                        logger.exception('cannot delete %s : %s retry: %s.',
                                         bucket_name, key_name, retries)
                        success = False

                if not success:
                    time.sleep(retry_timeout)
                    retry_timeout *= 2
                    retries += 1

            if not success:
                return False

        return True


class S3ImageJob(Job):
    def __init__(self, kwargs):
        super(S3ImageJob, self).__init__(
            operation=Job.OPERATION_CUSTOM,
            platform=Job.PLATFORM_AMAZON,
            region='dummy',
            bucket_name=kwargs.get('bucket_name', ''),
            key_name=kwargs.get('key_name', ''),
            key_prefix=kwargs.get('key_prefix', ''),
            abs_path=kwargs.get('abs_path', ''),
            status_done_path=kwargs.get('status_done_path', ''),
            status_compressed_path=kwargs.get('status_compressed_path', ''),
            status_uploading_path=kwargs.get('status_uploading_path', ''),
            file_size=kwargs.get('file_size', 0),
            dry_run=kwargs.get('dry_run', False),
            success=kwargs.get('success', False),
            num_trial=kwargs.get('num_trial', 0),
            delete_after_upload=kwargs.get('delete_after_upload', False),
            kwargs=kwargs)


class S3ImageSyncMasterBase(S3SyncMasterBase):
    def __init__(self, **kwargs):
        super(S3ImageSyncMasterBase, self).__init__(region='dummy', **kwargs)
        self.boto3_syncer = S3SyncerBase()


class S3ImagesHourlySyncMaster(S3ImageSyncMasterBase):
    '''
    Master figures out the diff cross different buckets for hourly image sync dir
    to copy and enqueue them as jobs.
    '''
    IN_SYNC_RATIO_THRESH = 0.3
    IN_SYNC_COUNT_THRESH = 100000

    def __init__(self, **kwargs):
        kwargs['worker_cls'] = S3CopyWorker
        super(S3ImagesHourlySyncMaster, self).__init__(**kwargs)
        # parameters specific to this class
        # These buckets should be the bucket where the hourly_sync dir is located.
        self.master_bucket_names = list(set(kwargs['master_bucket_names']))
        self.slave_bucket_names = list(set(kwargs.get('slave_bucket_names', [])))
        self.hourly_sync_dir = kwargs['hourly_sync_dir']
        self.working_dir = kwargs['working_dir']
        self.tmp_dir = tempfile.mkdtemp(dir=self.working_dir)

    def finalize(self):
        shutil.rmtree(self.tmp_dir)

    def create_jobs(self, key_name, abs_path, src_bucket_name, dest_bucket_names):
        kwargs = {
            'src_bucket_name': src_bucket_name,
            'dest_bucket_names': dest_bucket_names,
            'key_name': key_name,
            'abs_path': abs_path,
            'dry_run': self.dry_run
        }
        job = S3ImageJob(kwargs)
        self.queue.put(job)
        logger.debug('Sync file %s from %s to %s', abs_path, src_bucket_name, str(dest_bucket_names))

    def load_images_from_bucket(self, bucket_name):
        local_dir_path = os.path.join(self.tmp_dir, bucket_name)
        os.mkdir(local_dir_path)
        try:
            dir_download_master = S3DirDownloadMaster(
                num_worker_threads=self.num_worker_threads,
                access_key_id=self.access_key_id,
                access_key=self.access_key,
                bucket_name=bucket_name,
                dir_path=self.hourly_sync_dir,
                local_path=local_dir_path,
                black_list=[])
            dir_download_master.run()
            logger.debug("finished download sync file for bucket %s" % bucket_name)
        except:
            shutil.rmtree(local_dir_path)
            logger.exception("Download hourly sync info job crashed %s." % self.hourly_sync_dir)
            self.finalize()
            raise

        images_dict = {}
        for sync_file_name in os.listdir(local_dir_path):
            with open(os.path.join(local_dir_path, sync_file_name), 'r') as fp:
                for line in fp.readlines():
                    line = line.rstrip()
                    info_list = line.split("\t")
                    if len(info_list) != 3:
                        logger.error("Invalid sync file found for hourly image sync: %s", sync_file_name)
                        continue
                    images_dict[info_list[0]] = (info_list[1], int(info_list[2]))
        shutil.rmtree(local_dir_path)
        logger.debug("completed loading sync info for bucket %s" % bucket_name)

        return images_dict

    def enqueue_jobs(self):
        '''
        For given 'hourly_sync_dir', do the following things:
        1) For all master buckets, calculate the diff based on md5 and image ts
           (if md5 are different, newer image is the source of truth).
        2) For all slave buckets, calculate diff between master's source of truth.
        3) Create worker jobs to do the sync for the diffs from master source of truth
           to other master buckets and slaves.
        '''
        # A dict of file_name -> {list of bucket_names; md5; ts}. The list of bucket_names
        # have the file exist with same md5. So these buckets are already in sync.
        src_of_truth = {}
        for master_bucket_name in self.master_bucket_names:
            bucket_files = self.load_images_from_bucket(master_bucket_name)
            for file_name, md5_ts in six.iteritems(bucket_files):
                md5, ts = md5_ts
                file_info = src_of_truth.get(file_name, None)
                if file_info is None:
                    # first appearance of this image. directly add to source of truth.
                    src_of_truth[file_name] = {'buckets': [master_bucket_name], 'md5': md5, 'ts': ts}
                elif file_info.get('md5', '') == md5:
                    # same image, append to the src_of_truth's bucket list.
                    file_info['buckets'].append(master_bucket_name)
                    if file_info.get('ts', 0) < ts:
                        file_info['ts'] = ts
                elif file_info.get('ts', 0) < ts:
                    # different image with newer update ts, make it the source of truth instead.
                    file_info['buckets'] = [master_bucket_name]
                    file_info['md5'] = md5
                    file_info['ts'] = ts
                # skipped case here is: image is different and update time is older.

        # A dict of bucket name -> file sync info dict.
        slave_bucket_files_dict = {}
        for slave_bucket_name in self.slave_bucket_names:
            slave_bucket_files_dict[slave_bucket_name] = self.load_images_from_bucket(slave_bucket_name)

        # Go over each file in src_of_truth to calculate needed sync operations.
        master_buckets_set = set(self.master_bucket_names)
        num_images_to_sync = 0
        for file_name, truth_info in six.iteritems(src_of_truth):
            src_md5 = truth_info.get('md5', '')
            src_bucket_name = truth_info['buckets'][0]
            dest_bucket_names = list(master_buckets_set - set(truth_info['buckets']))

            # add slave buckets to dest_bucket_names in case there is any diff.
            for slave_bucket_name, slave_files_dict in six.iteritems(slave_bucket_files_dict):
                slave_md5_ts = slave_files_dict.get(file_name, None)
                if slave_md5_ts is None:
                    dest_bucket_names.append(slave_bucket_name)
                else:
                    slave_md5, slave_ts = slave_md5_ts
                    if slave_md5 != src_md5:
                        dest_bucket_names.append(slave_bucket_name)

            # create worker job for current image if there is any diff needs to sync.
            if len(dest_bucket_names) > 0:
                abs_path = os.path.join(self.tmp_dir, file_name)
                s3_key_name = s3_image_util.generate_s3_key_name_from_image_name(file_name)
                dest_bucket_names = []
                for dest_bucket_name in dest_bucket_names:
                    dest_bucket_names.append(dest_bucket_name)
                self.create_jobs(s3_key_name, abs_path, src_bucket_name, dest_bucket_names)
                num_images_to_sync += 1
        logger.info('total files to sync: %s', num_images_to_sync)
        total_count = len(list(src_of_truth.keys()))
        if float(num_images_to_sync) / total_count >= self.IN_SYNC_RATIO_THRESH \
                and total_count >= self.IN_SYNC_COUNT_THRESH:
            houzz.common.aws.ec2_utils.send_alert_email(
                's3 image hourly sync out of sync ratio too high',
                'total images for hourly sync %s: %s, num files to sync: %s'
                % (self.hourly_sync_dir, total_count, num_images_to_sync))
            raise Exception('out of sync ratio too high. '
                            'total images for hourly sync %s: %s, num files to sync: %s'
                            % (self.hourly_sync_dir, total_count, num_images_to_sync))


class S3DirDeleteMaster(S3ImageSyncMasterBase):
    '''
    Master to delete all files in given dir.
    '''
    def __init__(self, **kwargs):
        kwargs['worker_cls'] = S3DeleteWorker
        super(S3DirDeleteMaster, self).__init__(**kwargs)
        # parameters specific to this class
        self.bucket_names = kwargs['bucket_names']
        self.dir_path = kwargs['dir_path']
        self.black_list = kwargs['black_list']

    def create_jobs(self, key_name, bucket_name):
        kwargs = {
            'bucket_names': [bucket_name],
            'key_name': key_name,
            'dry_run': self.dry_run
        }
        job = S3ImageJob(kwargs)
        self.queue.put(job)

    def enqueue_jobs(self):
        '''
        For given 'file_dir', delete all files in all given buckets.
        For now, only allow delete from hourly_sync dir. You can add more in the white list but need to
        be extremely careful.
        '''
        if len(self.dir_path) == 0 or self.dir_path.find('hourly_sync') < 0:
            logger.error('Invalid dir %s for s3 delete', self.dir_path)
            raise ValueError('Invalid dir %s for s3 delete' % self.dir_path)
        num_files_deleted = 0
        for bucket_name in self.bucket_names:
            bucket = self.boto3_syncer.get_bucket(bucket_name)
            for key in bucket.list(prefix=self.dir_path):
                skip = False
                key_name = key.name
                for no_delete_file in self.black_list:
                    if key_name.find(no_delete_file) >= 0:
                        skip = True
                        break
                if not skip:
                    self.create_jobs(key_name, bucket_name)
                    num_files_deleted += 1
        logger.info('total files to delete: %s', num_files_deleted)


class S3DirDownloadMaster(S3ImageSyncMasterBase):
    '''
    Master to download all files in given dir.
    '''
    def __init__(self, **kwargs):
        kwargs['worker_cls'] = S3DownloadWorker
        super(S3DirDownloadMaster, self).__init__(**kwargs)
        # parameters specific to this class
        self.bucket_name = kwargs['bucket_name']
        self.dir_path = kwargs['dir_path']
        self.local_path = kwargs['local_path']
        self.black_list = kwargs['black_list']

    def create_jobs(self, key_name, bucket_name):
        local_file_path = os.path.join(self.local_path, os.path.basename(key_name))
        kwargs = {
            'bucket_name': bucket_name,
            'key_name': key_name,
            'abs_path': local_file_path,
            'dry_run': self.dry_run
        }
        job = S3ImageJob(kwargs)
        self.queue.put(job)
        #logger.debug('Download %s from %s to %s', key_name, bucket_name, local_file_path)

    def enqueue_jobs(self):
        '''
        For given 'file_dir', download all files in given bucket.
        '''
        num_files_downloaded = 0
        bucket = self.boto3_syncer.get_bucket(self.bucket_name)
        for key in bucket.list(prefix=self.dir_path):
            skip = False
            key_name = key.name
            for no_download_file in self.black_list:
                if key_name.find(no_download_file) >= 0:
                    skip = True
                    break
            if not skip:
                self.create_jobs(key_name, self.bucket_name)
                num_files_downloaded += 1
        logger.info('total files to download: %s', num_files_downloaded)


class S3ImageSyncWorkerBase(S3SyncWorkerBase):
    def __init__(self, queue, can_quit, **kwargs):
        super(S3ImageSyncWorkerBase, self).__init__(queue, can_quit, **kwargs)
        self.boto3_syncer = S3SyncerBase()


class S3DownloadWorker(S3ImageSyncWorkerBase):
    '''Worker dequeue jobs and download them to S3.'''
    def __init__(self, queue, can_quit, **kwargs):
        super(S3DownloadWorker, self).__init__(queue, can_quit, **kwargs)
        # will be used to set the ownership of the downloaded files
        self.owner_uid = pwd.getpwnam(Config.DAEMON_PARAMS.FILE_OWN_USER).pw_uid
        self.owner_gid = grp.getgrnam(Config.DAEMON_PARAMS.FILE_OWN_GROUP).gr_gid

    def pre_run(self, job):
        '''Hook to execute before run.'''
        # Create intermediary local dir if missing
        dir_path = os.path.dirname(job.abs_path)
        houzz.common.operation_utils.create_dir_if_missing(
                dir_path,
                user=Config.DAEMON_PARAMS.FILE_OWN_USER,
                group=Config.DAEMON_PARAMS.FILE_OWN_GROUP)

    def process_job(self, job):
        key_name = job.key_name
        bucket_name = job.bucket_name
        s3_bucket = self.boto3_syncer.get_bucket(bucket_name)
        with open(job.abs_path, "wb") as data:
            s3_bucket.download_fileobj(key_name, data)

    def post_run(self, job):
        '''Hook to execute after run.'''
        # Set the ownership for the downloaded file
        os.chown(job.abs_path, self.owner_uid, self.owner_gid)


class S3CopyWorker(S3ImageSyncWorkerBase):
    '''
    Worker dequeue jobs and do the S3 copy operation.
    Input job must have 'abs_path' as tmp file path.
    Assumptions:
    1) The key_name for source and destinations are the same,
    which means copying to the exactly same dir with same file name.
    2) Files to copy will not have the same name.
    Or same name files will not share the same 'tmp_dir'.
    '''

    def __init__(self, queue, can_quit, **kwargs):
        super(S3CopyWorker, self).__init__(queue, can_quit, **kwargs)
        # will be used to set the ownership of the downloaded files
        self.owner_uid = pwd.getpwnam(Config.DAEMON_PARAMS.FILE_OWN_USER).pw_uid
        self.owner_gid = grp.getgrnam(Config.DAEMON_PARAMS.FILE_OWN_GROUP).gr_gid

    def pre_run(self, job):
        '''Hook to execute before run.'''
        # Create intermediary local dir if missing
        dir_path = os.path.dirname(job.abs_path)
        houzz.common.operation_utils.create_dir_if_missing(
            dir_path,
            user=Config.DAEMON_PARAMS.FILE_OWN_USER,
            group=Config.DAEMON_PARAMS.FILE_OWN_GROUP)

    def process_job(self, job):
        key_name = job.key_name
        src_bucket_name = job.kwargs['src_bucket_name']
        src_bucket = self.boto3_syncer.get_bucket(src_bucket_name)
        abs_path = job.abs_path

        # Download from src bucket to tmp_dir
        with open(abs_path, "wb") as data:
            src_bucket.download_fileobj(key_name, data)

        # Upload to dest buckets
        for dest_bucket_name in job.kwargs['dest_bucket_names']:
            dest_bucket = self.boto3_syncer.get_bucket(dest_bucket_name)
            logger.info('s3_syncer: sync %s to %s', key_name, dest_bucket_name)
            if self.dry_run:
                logger.info("DRY RUN: upload file %s to bucket %s", key_name, dest_bucket_name)
            else:
                try:
                    with open(abs_path, "rb") as data:
                        dest_bucket.upload_fileobj(data, key_name)
                except IOError:
                    if os.path.exists(abs_path):
                        raise
                    logger.error('file %s has been removed. skip on it.', abs_path)
                except ClientError:
                    logger.error('Upload file failed with key_name %s.', abs_path)
        os.unlink(abs_path)


class S3DeleteWorker(S3ImageSyncWorkerBase):
    '''Worker dequeue jobs and delete files from S3.'''
    def __init__(self, queue, can_quit, **kwargs):
        super(S3DeleteWorker, self).__init__(queue, can_quit, **kwargs)

    def process_job(self, job):
        key_name = job.key_name
        bucket_names = job.kwargs['bucket_names']
        self.boto3_syncer.delete_file_from_buckets(key_name, bucket_names)
