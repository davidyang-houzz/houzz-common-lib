from __future__ import absolute_import
__author__ = 'jonathan'

from houzz.common.service_proxy import ServiceProxy
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from houzz.common.module.env_module import EnvModule
from houzz.common.thrift_gen.discovery_service import DiscoveryService


DISCOVERY_SOCKET = '/var/lib/haproxy/sl.sock'

"""
Service client that allows you to specify fallback endpoints instead of using the discovery service to call thrift
locally.

Allows for a more explicit declaration of fallback endpoints for local than the regular service_proxy does.
"""


class ServiceProxyDiscoveryless(ServiceProxy):

    def __init__(self, cls, fallback_endpoints, env, identity=''):

        # Environment (stg, env, prod)
        self._env = env

        # fallback endpoints
        self._fallback_endpoints = fallback_endpoints

        ServiceProxy.__init__(self, cls, identity)

    """
    Override
    """
    def _init_discovery_agent(self):

        if self._env != EnvModule.ENV_DEV:
            discovery_socket = TSocket.TSocket(unix_socket=DISCOVERY_SOCKET)
            self._discovery_transport = TTransport.TFramedTransport(discovery_socket)
            discovery_protocol = TBinaryProtocol.TBinaryProtocol(self._discovery_transport)
            self._discovery_client = DiscoveryService.Client(discovery_protocol)

    """
    Override
    """
    def _init_service_end_points(self, identity):
        if self._env != EnvModule.ENV_DEV:
            self._discovery_transport.open()
            self._endpoints = self._discovery_client.resolve(identity)
            self._discovery_transport.close()
        else:
            self._endpoints = self._fallback_endpoints
