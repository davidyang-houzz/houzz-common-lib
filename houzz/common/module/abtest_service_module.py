from __future__ import absolute_import
__author__ = 'menglei'

import logging

from twitter.common import app, options
from houzz.common.module.config_module import ConfigModule
from houzz.common.service_proxy import ServiceProxy
from houzz.common.thrift_gen.discovery_service import ttypes

from houzz.common.module.env_module import EnvModule
from houzz.common.module.discovery_service_module import DiscoveryServiceModule
from houzz.common.thrift_gen.tm_service import TMService


class ABTestServiceModule(app.Module):

    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'abtest_identity': options.Option(
            '--abtest_identity',
            dest='abtest_identity',
            help="abtest identity"
        )
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(ConfigModule())
        app.register_module(EnvModule())
        app.register_module(DiscoveryServiceModule())
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[ConfigModule.full_class_path(), DiscoveryServiceModule.full_class_path()],
            description="ABTest Service Module")
        self._log = logging.getLogger(self.__class__.__name__)
        self._instance = None

    def setup_function(self):
        app_options = app.get_options()
        app_config = ConfigModule().app_config
        if EnvModule().is_dev():
            endpoint = ttypes.Endpoint('127.0.0.1', 6002)
            abtest_identity = None
        else:
            endpoint = None
            abtest_identity = app_options.abtest_identity or app_config.get('ABTEST_IDENTITY', None)
        self._log.info("ABTEST identity read: [%s]", abtest_identity)

        self._instance = ServiceProxy(TMService.Client, abtest_identity, endpoints=[endpoint])

    @property
    def connection(self):
        return self._instance
