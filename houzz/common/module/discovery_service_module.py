from __future__ import absolute_import
__author__ = 'xiaofeng'

from twitter.common import app, options, log
from houzz.common.module.config_module import ConfigModule

from thrift.transport import THttpClient
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from houzz.common.module.env_module import EnvModule

from houzz.common.thrift_gen.discovery_service import DiscoveryService
from houzz.common.thrift_gen.discovery_service.ttypes import *


class DiscoveryServiceModule(app.Module):

    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'discover_service_socket': options.Option(
            '--discover_service_socket',
            dest='discover_service_socket',
            help="discovery service socket file path",
            default=None
        )
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[EnvModule().full_class_path(),ConfigModule.full_class_path()],
            description="Discover Service Module")
        self._instance = None
        self._transport = None

    def setup_function(self):
        """

        :return:
        """
        self.init_discovery_client_instance()

    def init_discovery_client_instance(self):
        """

        :return:
        """
        try:
            if not EnvModule().is_dev():
                app_options = app.get_options()
                app_config = ConfigModule().app_config
                sock = app_options.discover_service_socket or app_config.get("DISCOVERY_SERVICE_SOCKET")
                socket = TSocket.TSocket(unix_socket=sock)
                self._transport = TTransport.TFramedTransport(socket)
                protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
                self._instance = DiscoveryService.Client(protocol)
                self._transport.open()
        except TException as e:
            log.error('Discovery Service Client Error: failed to get discovery service. ' + e.message)
            raise e

    def refresh(self):
        """

        :return:
        """
        self._transport.close()
        self.init_discovery_client_instance()

    @property
    def connection(self):
        return self._instance
