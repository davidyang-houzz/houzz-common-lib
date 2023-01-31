from __future__ import absolute_import
import json
import logging
import os
import uuid
import threading

import kazoo.client
import kazoo.exceptions
import kazoo.protocol.states
from kazoo.security import READ_ACL_UNSAFE

import houzz.common.py3_util

logger = logging.getLogger(__name__)

# A new version that does not depend on zc.zk.
#
# (Should be compatible with the old class - old class is gone now.)
# We only support register() and unregister() because they are the two methods
# that are ever called.
class ServiceRegister(object):
    def __init__(self, zk_endpoint, zk_path, addr, svc_path='/houzz/providers'):
        self.state = None
        self.ephemeral = {}
        self.zk_path = zk_path
        self.addr = addr
        self.svc_path = svc_path

        self.client = kazoo.client.KazooClient(zk_endpoint)
        self.client.add_listener(self._watch_session)
        self.client.start()

    def register(self, prop=None):
        prop = prop or {}
        self.client.ensure_path(self.zk_path)
        if self.svc_path:
            self.client.ensure_path(self.svc_path)

        prefix = uuid.uuid4().hex
        addr = self.addr
        if not isinstance(addr, str):
            addr = '%s:%s' % tuple(addr)
        addr = '-'.join([prefix, addr])

        self._register_path(self.zk_path, addr, prop)
        if self.svc_path:
            prop['type'] = self.zk_path
            self._register_path(self.svc_path, addr, prop)

    def unregister(self):
        self.client.stop()
        self.client.close()

    # TODO: This is weird semantics, because this actually counts the number of
    # childrens under 'zk_path' which is shared by other instances of the same
    # service.  E.g., if zk_path is '/houzz/foo/providers/prod', then this
    # returns the number of all services currently registered under that path.
    #
    # In particular, since __len__ is also used for boolean conversion, if we do
    # this:
    #
    #   register = NewServiceRegister(...)
    #   ......
    #   if register:
    #       (do something)
    #
    # Then the if statement does *not* check if we registered a service
    # ourselves; it checks if there is *any service* that's currently registered
    # (including our own instance).
    def __len__(self):
        children = self.client.retry(self.client.get_children, self.zk_path)
        return len(children)

    # Stolen from zc.zk.Zookeeper.register().
    #
    # I didn't include the "resolve" part of zc.zk that makes zookeeper path
    # work like an actual filesystem path with symlinks: I don't think we're
    # using it.
    def _register_path(self, path, addr, prop):
        if not path.endswith('/'): path = path + '/'

        prop = prop.copy()
        prop['pid'] = os.getpid()
        data = json.dumps(prop, separators=(',', ':'))

        # In Python 3, Kazoo requires data in bytes type.
        data = houzz.common.py3_util.str2bytes(data)

        self.client.create(path + addr, data, READ_ACL_UNSAFE, ephemeral=True)
        self.ephemeral[path + addr] = data

    # Stolen from zc.zk.Zookeeper.__init__().
    def _watch_session(self, state):
        logger.info('NewServiceRegister state changed from %s to %s',
                    self.state, state)
        old_state = self.state
        self.state = state
        do_restore = (old_state == kazoo.protocol.states.KazooState.LOST) and \
                     (state == kazoo.protocol.states.KazooState.CONNECTED)

        if not do_restore: return
        logger.info('Restoring ephemeral states')

        ephemeral = self.ephemeral.copy()
        def _do_restore():
            for path, data in ephemeral.items():
                logger.info('Restoring ephemeral %s -> %s', path, data)
                try:
                    self.client.create(
                        path, data, READ_ACL_UNSAFE, ephemeral=True)
                except kazoo.exceptions.NodeExistsError:
                    pass # threads? <shrug>

        thread = threading.Thread(
            target=_do_restore, name='ZooKeeper restore thread')
        thread.daemon = True
        thread.start()

if __name__ == '__main__':
    s = ServiceRegister(None, '/a/b/c', ('1.1.1.1', '222'))
    s.register()
