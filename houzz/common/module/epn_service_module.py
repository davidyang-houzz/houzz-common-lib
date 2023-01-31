from __future__ import absolute_import
__author__ = 'xiaofeng'

import logging

from twitter.common import app, options
from houzz.common.module.config_module import ConfigModule
from houzz.common.service_proxy import ServiceProxy

from houzz.common.module.discovery_service_module import DiscoveryServiceModule

class EPNServiceModule(app.Module):

    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'epn_service_url': options.Option(
            '--epn_service_url',
            dest='epn_service_url',
            help="the EPN service server url",
            default=None
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
            description="EPN Service Module")
        self._log = logging.getLogger(self.__class__.__name__)
        self._instance = None

    def setup_function(self):
        # app_options = app.get_options()
        app_config = ConfigModule().app_config
        resolver = DiscoveryServiceModule().connection
        epn_identity = app_config.get('IDENTITY', None)
        self._log.info("epn identity read: [%s]", epn_identity)
        end_points = resolver.resolve(epn_identity)
        self._log.info("end points resolved: [%s]", end_points)

        from houzz.common.thrift_gen.epn_service import EPNService
        self._instance = ServiceProxy(EPNService.Client, identity=None, endpoints=end_points)

    @property
    def connection(self):
        return self._instance
