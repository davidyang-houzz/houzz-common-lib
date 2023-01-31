from __future__ import absolute_import
from __future__ import print_function
from .client import RQClient


class RQClientTester(object):

    def start(self):
        client = RQClient(use_func='push_queue_clients', use_tube="default")
        result = client.put({'data': 'test', })
        print(result)

if __name__ == "__main__":
    RQClientTester().start()
