from __future__ import absolute_import
__author__ = 'jasonl'

from twitter.common import app, options, log
from houzz.common.module.config_module import ConfigModule
from houzz.common import redis_utils

class RedisModule(app.Module):

    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'redis_url': options.Option(
            '--redis_url',
            dest='redis_url',
            help="the redis server url",
            action='append',
            default=None
        ),
        'redis_cluster': options.Option(
            '--redis_cluster',
            dest='redis_cluster',
            help="the redis server config that can be a cluster",
            action='append',
            default=None
        )
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(ConfigModule())
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[ConfigModule.full_class_path()],
            description="Redis Module")
        self._instance = None

    def setup_function(self):
        app_options = app.get_options()
        app_config = ConfigModule().app_config
        redis_cluster = app_options.redis_cluster or app_config.get("REDIS_CLUSTER", None)
        if redis_cluster:
            self._instance = redis_utils.get_redis_connection(redis_cluster)
        else:
            redis_url = app_options.redis_url or app_config.get("REDIS_URL")
            log.info("redis_url: %s", redis_url)
            if not redis_url:
                raise Exception("REDIS_URL is not defined in the config or app options")
            from redis import utils
            self._instance = utils.from_url(url=redis_url)

    @property
    def connection(self):
        return self._instance
