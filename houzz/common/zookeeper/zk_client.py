from __future__ import absolute_import
from kazoo.client import KazooClient
import logging


logging.basicConfig()

def connect():
    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()
