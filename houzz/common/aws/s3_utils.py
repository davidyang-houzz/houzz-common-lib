#!/usr/bin/env python
from __future__ import absolute_import
import logging
import boto3
from botocore.exceptions import ClientError
import os

logger = logging.getLogger(__name__)

def get_local_aws_cred():
    cred_file = os.path.join(os.path.expanduser("~"), '.aws/credentials')
    with open(cred_file, 'r') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if 'staging' in line:
                key_id = lines[i+1].rstrip().split("=")[1]
                access_key= lines[i+2].rstrip().split("=")[1]
    return key_id, access_key

class S3Util:

    def __init__(self, **kwargs):
        self.bucket_name = kwargs.get('bucket_name')
        get_cred = kwargs.get('get_cred')
        if get_cred:
            access_key_id, secret_access_key = get_cred()
            self.client = boto3.client(
                "s3",
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key)
            logger.info("Connected to S3 with credentials")
        else:
            self.client = boto3.client("s3")
            logger.info("Connected to S3 using EC2 role access")
        logger.info("Got bucket {0}".format(self.bucket_name))

    def upload_file(self, **kwargs):
        src_path = kwargs.get('src_path', '')
        dst_dir = kwargs.get('dst_dir', '')
        logger.info("Going to upload file {0} to bucket {1}/{2}".format(src_path, self.bucket_name, dst_dir))
        if not os.path.exists(src_path):
            logger.warning("Not upload because the file {0} does not exist".format(src_path))
            return
        source_size = os.stat(src_path).st_size
        #Uploading empty file will result in error as follows:
        #<Code>MalformedXML</Code>
        #<Message>The XML you provided was not well-formed or did not validate against our published schema</Message>
        if source_size == 0:
            logger.warning("Not upload because the file {0} is of size 0".format(src_path))
            return

        dst_path = os.path.join(dst_dir, os.path.basename(src_path))
        try:
            self.client.upload_file(src_path, self.bucket_name, dst_path)
        except ClientError as e:
            logger.error(e)
        logger.info("Uploaded file {0} to s3".format(src_path))

    def download_file(self, **kwargs):
        dst_path = kwargs.get('dst_path', '')
        src_path = kwargs.get('src_path', '')
        logger.info("Going to download file {0} from bucket {1}/{2}".format(dst_path, self.bucket_name, src_path))
        if os.path.exists(dst_path):
            logger.warning("file {0} exist. Going to override it.".format(dst_path))
        self.client.download_file(self.bucket_name, dst_path, src_path)
        logger.info("Downloaded file {0} from s3".format(dst_path))

    def get_file_names_with_filter(self, name_filter,
                    prefix='', delimiter='', marker='',
                    encoding_type=None):
        logger.debug("Going to get file names with name_filter={0}, prefix={1}, delimiter={2}, marker={3}"
                          .format(name_filter, prefix, delimiter, marker))
        file_names = []
        for k in self.client.list_objects(
            Bucket=self.bucket_name,
            Delimiter=delimiter,
            EncodingType=encoding_type,
            Marker=marker,
            Prefix=prefix
        ):
            logger.debug('getting meta for:%s', k["Name"])
            if k["Name"].find(name_filter) >= 0:
                file_names.append(os.path.basename(k.name))
        logger.debug("After filtered by {0}, got file names: {1}".format(name_filter, file_names))
        return file_names

    def list(self, prefix='', delimiter='', marker=''):
        logger.debug("Going to get list with prefix={}, delimiter={}, marker={}".format(prefix, delimiter, marker))
        object_names = []
        for k in self.client.list_objects(
            Bucket=self.bucket_name,
            Delimiter=delimiter,
            Marker=marker,
            Prefix=prefix
        ):
            logger.debug('getting meta for:%s', k["Name"])
            object_names.append(k["Name"])
        logger.debug("Got file names: {}".format(object_names))
        return object_names
