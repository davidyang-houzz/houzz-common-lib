#!/usr/bin/env python

from __future__ import absolute_import
from botocore.exceptions import ClientError

def s3_resource_exists(s3_resource):
    try:
        s3_resource.load()
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise
    else:
        return True

def s3_bucket_exists(s3_bucket):
    # This uses list_buckets() to load the bucket. It is ok if we don't have too many buckets.
    # Change to slightly cheaper Client.head_bucket if otherwise.
    return s3_resource_exists(s3_bucket)

def s3_file_exists(s3_object):
    return s3_resource_exists(s3_object)
