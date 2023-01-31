from __future__ import absolute_import
from __future__ import print_function
import rq
import json
from redis.client import StrictRedis
from rq.job import Job
from houzz.common.rq.async_client import RQAsyncClient

rq.job.__dict__['dumps'] = json.dumps
rq.job.__dict__['loads'] = json.loads

def test_cancel_job():
    rq_client = RQAsyncClient(host='127.0.0.1', port='8191', connect_timeout=30)
    result = rq_client.cancel_job(id='001a5802-3534-4155-83b1-6fa71641165e')
    print('result is : {}'.format(result))
def main():
    test_cancel_job()

if __name__ == "__main__":
    main()
