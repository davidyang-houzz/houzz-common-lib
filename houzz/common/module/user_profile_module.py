from __future__ import absolute_import
__author__ = 'jack'

import logging

from twitter.common import app, options
from houzz.common.module.config_module import ConfigModule
from houzz.common.service_proxy import ServiceProxy
from houzz.common.thrift_gen.discovery_service import ttypes

from houzz.common.module.env_module import EnvModule
from houzz.common.module.discovery_service_module import DiscoveryServiceModule
from houzz.common.thrift_gen.user_profile_service import UserProfileService


class UserProfileServiceModule(app.Module):

    """
    Binds this application with The Configuration module
    """

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
            description="User Profile Service Module")
        self._log = logging.getLogger(self.__class__.__name__)
        self._instance = None

    def setup_function(self):
        app_options = app.get_options()
        app_config = ConfigModule().app_config
        if EnvModule().is_dev():
            endpoint = ttypes.Endpoint('127.0.0.1', 17010)
            user_profile_service_identity = None
        else:
            endpoint = None
            user_profile_service_identity = app_config.get('USER_PROFILE_IDENTITY', None)
        self._log.info("UserProfileService identity read: [%s]", user_profile_service_identity)

        self._instance = ServiceProxy(UserProfileService.Client, user_profile_service_identity, endpoints=[endpoint])

    @property
    def connection(self):
        return self._instance
