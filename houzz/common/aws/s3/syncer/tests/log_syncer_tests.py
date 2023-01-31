#!/usr/bin/env python
from __future__ import absolute_import
import boto3
import os
import shutil
import tempfile
import unittest

from moto import mock_s3

from houzz.common.aws.s3.syncer.log_syncer import S3LogUploadMaster

AWS_ACCESS_KEY_ID = 'dummy_aws_key'
AWS_SECRET_ACCESS_KEY = 'dummy_aws_secret'
S3_DEFAULT_LOCATION = 'us-west-2'

class S3LogSyncMasterTest(unittest.TestCase):

    # (file path, content)
    LOCAL_FILES = [
        ('hweb9/access.log.2013-02-06-14_00_00.gz', b'111'),  # new file
        ('hweb11/ad_log_12_11_08__00.gz', b'222'),            # existing no-change file
        ('hweb32/js_error_13_02_06__13.gz', b'4444'),         # existing changed file
    ]

    # (key, content)
    EXISTING_S3_FILES = [
        ('hweb11/00/08/11/2012/ad_log_12_11_08__00.gz', b'222'),
        ('hweb32/13/06/02/2013/js_error_13_02_06__13.gz', b'333'),
    ]

    # (key, content)
    EXPECTED_S3_FILES = [
        ('hweb9/14/06/02/2013/access.log.2013-02-06-14_00_00.gz', b'111'),
        ('hweb11/00/08/11/2012/ad_log_12_11_08__00.gz', b'222'),
        ('hweb32/13/06/02/2013/js_error_13_02_06__13.gz', b'4444'),
    ]

    def setUp(self):
        self.root_dir = tempfile.mkdtemp(prefix='log_syncer_')
        for p, data in self.LOCAL_FILES:
            fp = os.path.join(self.root_dir, p)
            os.mkdir(os.path.dirname(fp))
            # create data file
            with open(fp, 'wb') as out:
                out.write(data)
            # create status files
            with open(S3LogUploadMaster.get_status_done_file_path(fp), 'wb') as out:
                pass
            with open(S3LogUploadMaster.get_status_compressed_file_path(fp), 'wb') as out:
                pass

    def tearDown(self):
        shutil.rmtree(self.root_dir, ignore_errors=True)

    def test_generate_bucket_name(self):
        self.assertEqual(
            S3LogUploadMaster.generate_bucket_name(
                '/home/alon/backup_log/hweb9/access.log.2013-02-06-14_00_00.gz'),
            'logs.houzz.com'
        )
        self.assertEqual(
            S3LogUploadMaster.generate_bucket_name(
                '/home/alon/backup_log/hweb32/ad_log_12_11_08__00.gz'),
            'logs.houzz.com'
        )

    def test_generate_key_name(self):
        self.assertEqual(
            S3LogUploadMaster.generate_key_name(
                '/home/alon/backup_log/hweb9/access.log.2013-02-06-14_00_00.gz'),
            'hweb9/14/06/02/2013/access.log.2013-02-06-14_00_00.gz'
        )
        self.assertEqual(
            S3LogUploadMaster.generate_key_name(
                '/home/alon/backup_log/hweb11/ad_log_12_11_08__00.gz'),
            'hweb11/00/08/11/2012/ad_log_12_11_08__00.gz'
        )
        self.assertEqual(
            S3LogUploadMaster.generate_key_name(
                '/home/alon/backup_log/hweb32/js_error_13_02_06__13.gz'),
            'hweb32/13/06/02/2013/js_error_13_02_06__13.gz'
        )

    def test_generate_bucket_key_prefix(self):
        self.assertEqual(
            S3LogUploadMaster.generate_bucket_key_prefix(
                '/home/alon/backup_log/hweb9/access.log.2013-02-06-14_00_00.gz'),
            'hweb9'
        )
        self.assertEqual(
            S3LogUploadMaster.generate_bucket_key_prefix(
                '/home/alon/backup_log/hweb32/js_error_13_02_06__13.gz'),
            'hweb32'
        )

    def test_need_sync(self):
        self.assertTrue(
            S3LogUploadMaster.need_sync(
                os.path.join(
                    self.root_dir,
                    'hweb9/access.log.2013-02-06-14_00_00.gz'
                )
            )
        )
        self.assertFalse(
            S3LogUploadMaster.need_sync(
                os.path.join(
                    self.root_dir,
                    'hweb9/access.log.2013-02-06-14_00_00'
                )
            )
        )
        self.assertTrue(
            S3LogUploadMaster.need_sync(
                os.path.join(
                    self.root_dir,
                    'hweb32/js_error_13_02_06__13.gz'
                )
            )
        )
        self.assertFalse(
            S3LogUploadMaster.need_sync(
                os.path.join(
                    self.root_dir,
                    'hweb32/js_error_13_02_06__13.gz.done'
                )
            )
        )
        # missing done file
        os.remove(
            S3LogUploadMaster.get_status_done_file_path(
                os.path.join(
                    self.root_dir,
                    'hweb9/access.log.2013-02-06-14_00_00.gz'
                )
            )
        )
        self.assertFalse(
            S3LogUploadMaster.need_sync(
                os.path.join(
                    self.root_dir,
                    'hweb9/access.log.2013-02-06-14_00_00.gz'
                )
            )
        )
        # missing compressed file
        os.remove(
            S3LogUploadMaster.get_status_compressed_file_path(
                os.path.join(
                    self.root_dir,
                    'hweb32/js_error_13_02_06__13.gz'
                )
            )
        )
        self.assertFalse(
            S3LogUploadMaster.need_sync(
                os.path.join(
                    self.root_dir,
                    'hweb32/js_error_13_02_06__13.gz'
                )
            )
        )

    def prepare_s3(self):
        self.bucket = boto3.resource(
            's3',
            region_name=S3_DEFAULT_LOCATION
        ).create_bucket(
            Bucket=S3LogUploadMaster.BUCKET_NAME
        )
        for key, data in self.EXISTING_S3_FILES:
            self.bucket.put_object(
                Key=key,
                Body=data
            )

    def verify_s3(self):
        self.bucket = boto3.resource(
            's3',
            region_name=S3_DEFAULT_LOCATION
        ).Bucket(S3LogUploadMaster.BUCKET_NAME)
        # file content is correct
        for key, data in self.EXPECTED_S3_FILES:
            self.assertEqual(
                self.bucket.Object(key).get()['Body'].read(),
                data
            )
        # no extra files on s3
        self.assertEqual(
            len(self.EXPECTED_S3_FILES),
            len(list(self.bucket.objects.all()))
        )

    @mock_s3
    def test_log_sync(self):
        self.prepare_s3()

        # do the sync work
        log_syncer = S3LogUploadMaster(
            num_worker_threads=1,  # NOTE: moto lib is not thread-safe
            root_dir=self.root_dir,
            per_report_size=1,
            update_existing_file=True,
            access_key_id=AWS_ACCESS_KEY_ID,
            access_key=AWS_SECRET_ACCESS_KEY,
            region=S3_DEFAULT_LOCATION
        )
        log_syncer.run()

        # remote files are good
        self.verify_s3()

        # status files are good
        for p, _data in self.LOCAL_FILES:
            fp = os.path.join(self.root_dir, p)
            self.assertTrue(
                os.path.exists(S3LogUploadMaster.get_status_compressed_file_path(fp))
            )
            self.assertTrue(
                os.path.exists(S3LogUploadMaster.get_status_done_file_path(fp))
            )
