#!/usr/bin/env python
#
# Run: ./boto3_thread_pool_test.py (aws key id) (secret access key)

from __future__ import absolute_import
from __future__ import print_function
import logging
import sys
import time

from . import boto3_thread_pool
from six.moves import range

root = logging.getLogger()
root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
root.addHandler(handler)

TEST_BUCKET = 'houzz-test'

s3_pool = boto3_thread_pool.Boto3ThreadPool('s3',
    aws_access_key_id=sys.argv[1],
    aws_secret_access_key=sys.argv[2],
    region_name='us-west-2',
    max_workers=4)

requests = []
for idx in range(20):
    filepath = 'yongjik-test/boto3-test/test%03d.txt' % idx
    print('Creating file %s/%s ...' % (TEST_BUCKET, filepath))
    req = s3_pool.put_object(
        Bucket=TEST_BUCKET, Key=filepath,
        Body='Test file content %d\n' % idx)
    requests.append(req)

for idx, req in enumerate(requests):
    T0 = time.time()
    result = req.result()
    elapsed_ms = (time.time() - T0) * 1000.0
    print('(Elapsed %.3f ms) Etag for file #%d = %s' % \
          (elapsed_ms, idx, result['ETag']))
