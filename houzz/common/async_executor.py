from __future__ import absolute_import
__author__ = 'jasonl'

from gevent import spawn
#monkey.patch_all()
from gevent.pool import Pool


class AsyncExecutor(object):

    def __init__(self, pool_size):
        self._pool = Pool(pool_size)

    @staticmethod
    def exec_task(func, **kwargs):
        future = spawn(func, **kwargs)
        return future

    def exec_batch(self, func, iterable):
        for it in iterable:
            self._pool.spawn(func, it)
        res = self._pool.join()
        return res

