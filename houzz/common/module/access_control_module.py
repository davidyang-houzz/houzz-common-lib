from __future__ import absolute_import
__author__ = 'menglei'

import logging

from twitter.common import app
from houzz.common.module.config_module import ConfigModule
from houzz.common.module.discovery_service_module import DiscoveryServiceModule
from houzz.common.service_proxy import ServiceProxy
from houzz.common.thrift_gen.discovery_service import ttypes
from houzz.common.thrift_gen.access_control import AccessControlService


class AccessControlModule(app.Module):

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
            description="ACL Service Module")
        self._log = logging.getLogger(self.__class__.__name__)
        self._instance = None

    def setup_function(self):
        app_config = ConfigModule().app_config
        acl_host =  app_config.get('ACL_ENDPOINT', None)
        acl_port = app_config.get('ACL_PORT', None)
        if not acl_host or not acl_port:
            self._log.error("ACL_HOST or ACL_PORT is not defined in the your app config")
            raise Exception("ACL_HOST or ACL_PORT is not defined in the your app config")
        endpoint = ttypes.Endpoint(acl_host, acl_port)
        self._instance = ServiceProxy(AccessControlService.Client, None, [endpoint])

    @property
    def connection(self):
        return self._instance
