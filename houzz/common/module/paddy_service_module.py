from __future__ import absolute_import
__author__ = 'xiaofeng'

import logging

from twitter.common import app, options
from houzz.common.module.config_module import ConfigModule
from houzz.common.service_proxy import ServiceProxy

from houzz.common.module.discovery_service_module import DiscoveryServiceModule
from houzz.common.module.service_module import ServiceModule

class PaddyServiceModule(app.Module):
    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'svc_type': options.Option(
            '--svc_type',
            dest='svc_type',
            help="paddy server service type",
            default=ServiceModule.TORNADO_SERVER,
        )
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(ConfigModule())
        app.register_module(DiscoveryServiceModule())
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[ConfigModule.full_class_path(), DiscoveryServiceModule.full_class_path()],
            description="Paddy Service Module")
        self._log = logging.getLogger(self.__class__.__name__)
        self._instance = None

    def setup_function(self):
        app_config = ConfigModule().app_config
        resolver = DiscoveryServiceModule().connection
        paddy_identity = app_config.get('IDENTITY', None)
        self._log.info("paddy identity read: [%s]", paddy_identity)
        end_points = resolver.resolve(paddy_identity)
        self._log.info("paddy points resolved: [%s]", end_points)

        if app.get_options().svc_type == ServiceModule.TORNADO_SERVER:
            from houzz.paddy.thrift_gen_tornado.paddy import Paddy
            self._instance = ServiceProxy(Paddy.Client, endpoints=end_points, identity=None)
        else:
            from houzz.common.thrift_gen.paddy import Paddy
            self._instance = ServiceProxy(Paddy.Client, endpoints=end_points, identity=None)

    @property
    def connection(self):
        return self._instance
