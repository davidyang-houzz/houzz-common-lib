# Utility functions to support the functionality of properties() method in zc.zk
# library: we have a basic support for reading and updating json values.
#
# We only support the basic functionality.  In particular, we don't support
# watchers (properties with watch=True).
#
# How to use:
#
#   if use_legacy_code:
#       # Create a property object using zc.zk
#       zk = zc.zk.ZooKeeper(zk_endpoint)
#       prop = zk.properties(zk_path, watch=False)
#   else:
#       # Create a property object using this library. 
#       prop = ZkProperty(zk_endpoint, zk_path)
#      
#   print('Value at key = ', prop[key])
#   print('Value at key = ', prop.get(key, 'None'))
#   print('Updating value')
#   prop[key] = newvalue  # The value is updated inside Zookeeper.
#
#   # Read-only copy
#   for key, value in prop.data.items():
#       print(key, ':', value)

from __future__ import absolute_import
import json

import kazoo.client

import houzz.common.py3_util

class ZkProperty(object):
    def __init__(self, zk_endpoint, zk_path):
        self.client = kazoo.client.KazooClient(zk_endpoint)
        self.client.start()
        self.zk_path = zk_path

        data, _ = self.client.get(zk_path)
        self.data = json.loads(data)

    def __repr__(self):
        return 'ZkProperty(path="%s", %s)' % (self.zk_path, self.data)

    def __len__(self):
        return len(self.data)

    def __contains__(self, key):
        return key in self.data

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, key):
        return self.data[key]

    def get(self, key, default=None):
        return self.data.get(key, default)

    def __setitem__(self, key, value):
        self.data[key] = value
        self._sync()

    def __delitem__(self, key):
        del self.data[key]
        self._sync()

    def update(self, data=None, **vals):
        if data:
            self.data.update(data)
        self.data.update(vals)
        self._sync()

    # Update the property value in the server.
    def _sync(self):
        serialized = json.dumps(self.data, separators=(',', ':'))

        # In Python 3, Kazoo requires data in bytes type.
        serialized = houzz.common.py3_util.str2bytes(serialized)

        self.client.set(self.zk_path, serialized)
