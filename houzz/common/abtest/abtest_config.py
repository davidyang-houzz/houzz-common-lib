from __future__ import absolute_import
__author__ = 'menglei'

import datetime


DEFAULT_TTL = 1000*60*3

class ABTestConfig:
    """

    """
    def __init__(self, env, abtest_service, cache_ttl=None):
        self._env = env
        self._abtest_service = abtest_service
        self._cache = dict()
        self._refresh_time = None
        if cache_ttl:
            self._cache_ttl = cache_ttl
        else:
            self._cache_ttl = DEFAULT_TTL
        self.refresh_config()

    def refresh_config(self):
        self._cache = dict()
        # always get prod
        abtests = self._abtest_service.get_all_abtests_by_state(start_ts=0, end_ts=-1, test_state=2)
        for abtest in abtests:
            self._cache[abtest.name] = abtest
        self._refresh_time = datetime.datetime.now()

    def get_abtest_config(self, key):
        if not key:
            return None
        if datetime.datetime.now() - self._refresh_time > self._cache_ttl:
            self.refresh_config()
        if key in self._cache:
            return self._cache[key]
        else:
            return None
