#!/usr/bin/env python
from __future__ import absolute_import
import boto3
import os
import shutil
import tempfile
import unittest

from moto import mock_s3

from houzz.common.aws.s3.syncer.dump_syncer import S3DumpUploadMaster

AWS_ACCESS_KEY_ID = 'dummy_aws_key'
AWS_SECRET_ACCESS_KEY = 'dummy_aws_secret'
S3_DEFAULT_LOCATION = 'us-west-2'

class S3DumpSyncMasterTest(unittest.TestCase):

    # (file path, content)
    LOCAL_FILES = [
        ('innobackup.tar.gz.20130827133356', b'111'),              # new file
        ('dump.rdb.20130827123356', b'222'),                       # existing no-change file
        ('innobackup_misc_shard.tar.gz.20130827133356', b'4444'),  # existing changed file
    ]

    # (key, content)
    EXISTING_S3_FILES = [
        ('dump/dump.rdb.20130827123356', b'222'),
        ('innobackup_misc_shard/innobackup_misc_shard.tar.gz.20130827133356', b'333'),
    ]

    # (key, content)
    EXPECTED_S3_FILES = [
        ('innobackup/innobackup.tar.gz.20130827133356', b'111'),
        ('dump/dump.rdb.20130827123356', b'222'),
        ('innobackup_misc_shard/innobackup_misc_shard.tar.gz.20130827133356', b'4444'),
    ]

    def setUp(self):
        self.root_dir = tempfile.mkdtemp(prefix='dump_syncer_')
        for p, data in self.LOCAL_FILES:
            fp = os.path.join(self.root_dir, p)
            # create data file
            with open(fp, 'wb') as out:
                out.write(data)
            # create status files
            with open(S3DumpUploadMaster.get_status_done_file_path(fp), 'wb') as out:
                pass

    def tearDown(self):
        shutil.rmtree(self.root_dir, ignore_errors=True)

    def test_generate_bucket_name(self):
        self.assertEqual(
            S3DumpUploadMaster.generate_bucket_name(
                '/home/alon/data_dump/dump.rdb.20130827133356'),
            'dumps.houzz.com'
        )
        self.assertEqual(
            S3DumpUploadMaster.generate_bucket_name(
                '/home/alon/data_dump/innobackup.tar.gz.20130827133356'),
            'dumps.houzz.com'
        )

    def test_generate_key_name(self):
        self.assertEqual(
            S3DumpUploadMaster.generate_key_name(
                '/home/alon/data_dump/dump.rdb.20130827133356'),
            'dump/dump.rdb.20130827133356'
        )
        self.assertEqual(
            S3DumpUploadMaster.generate_key_name(
                '/home/alon/data_dump/innobackup.tar.gz.20130827133356'),
            'innobackup/innobackup.tar.gz.20130827133356'
        )

    def test_generate_bucket_key_prefix(self):
        self.assertEqual(
            S3DumpUploadMaster.generate_bucket_key_prefix(
                '/home/alon/data_dump/dump.rdb.20130827133356'),
            'dump'
        )
        self.assertEqual(
            S3DumpUploadMaster.generate_bucket_key_prefix(
                '/home/alon/data_dump/innobackup.tar.gz.20130827133356'),
            'innobackup'
        )
        self.assertEqual(
            S3DumpUploadMaster.generate_bucket_key_prefix(
                '/home/alon/data_dump/innobackup_misc_shard.tar.gz.20160926013704'),
            'innobackup_misc_shard'
        )

    def test_need_sync(self):
        for p, _ in self.LOCAL_FILES:
            self.assertTrue(
                S3DumpUploadMaster.need_sync(
                    os.path.join(self.root_dir, p)
                )
            )
        # missing done file
        for p, _ in self.LOCAL_FILES:
            os.remove(
                S3DumpUploadMaster.get_status_done_file_path(
                    os.path.join(self.root_dir, p)
                )
            )
            self.assertFalse(
                S3DumpUploadMaster.need_sync(
                    os.path.join(self.root_dir, p)
                )
            )

    def prepare_s3(self):
        self.bucket = boto3.resource(
            's3',
            region_name=S3_DEFAULT_LOCATION
        ).create_bucket(
            Bucket=S3DumpUploadMaster.BUCKET_NAME
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
        ).Bucket(S3DumpUploadMaster.BUCKET_NAME)
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
    def test_dump_sync(self):
        self.prepare_s3()

        # do the sync work
        dump_syncer = S3DumpUploadMaster(
            num_worker_threads=1,  # NOTE: moto lib is not thread-safe
            root_dir=self.root_dir,
            per_report_size=1,
            update_existing_file=True,
            access_key_id=AWS_ACCESS_KEY_ID,
            access_key=AWS_SECRET_ACCESS_KEY,
            region=S3_DEFAULT_LOCATION
        )
        dump_syncer.run()

        # remote files are good
        self.verify_s3()

        # status files are good
        for p, _data in self.LOCAL_FILES:
            fp = os.path.join(self.root_dir, p)
            self.assertTrue(
                os.path.exists(S3DumpUploadMaster.get_status_done_file_path(fp))
            )
