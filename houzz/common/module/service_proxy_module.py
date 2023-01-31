from __future__ import absolute_import
__author__ = 'menglei'

from twitter.common import app, options

from houzz.common.module.discovery_service_module import DiscoveryServiceModule
from houzz.common.service_proxy import ServiceProxy


class ServiceProxyModule(app.Module):

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(DiscoveryServiceModule())
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[DiscoveryServiceModule.full_class_path()],
            description="Service Proxy Module")

    @staticmethod
    def get_client(cls, identity):
        return ServiceProxy(cls, identity)
