from __future__ import absolute_import
import types
from types import FunctionType
from random import shuffle

from twitter.common import log
from houzz.common.service_client_wrapper import ServiceClientWrapper
from retry import retry
from houzz.common.module.discovery_service_module import DiscoveryServiceModule
from thrift.Thrift import TException


class ServiceProxy():
    DEFAULT_TIMEOUT = 10
    DEFALT_RETRIES = 3

    def __init__(self, cls, identity, endpoints=None):
        """

        :param cls: thrift service client
        :param identity: service identity registered in discovery service
        """
        self._cls = cls
        self._identity = identity
        self._discovery_agent = None
        self._endpoints = endpoints
        self._instances = []
        self._init_discovery_agent()
        self._init_service_end_points(identity)
        self._init_service()
        self._bind_methods()

    def _init_service(self, timeout=DEFAULT_TIMEOUT):
        """

        :param end_points:
        :param timeout:
        :return:
        """
        self._instances = []
        if self._endpoints is not None:
            for end_point in self._endpoints:
                self._instances.append(ServiceClientWrapper(self._cls, end_point.host, end_point.port, timeout=timeout))

    def _init_service_end_points(self, identity):
        """
        init service end points
        :param identity: service identity registered in discovery service
        :return: a list of end points
        """
        try:
            if identity is not None:
                self._endpoints = self._discovery_agent.resolve(identity)
        except TException as e:
            # keep the current endpoints if agent cannot resolve the identity
            log.error("Service Proxy Error: resolve error" + e.__class__.__name__ + " " + e.message)

    def _init_discovery_agent(self):
        """
        init discovery agent
        :param refresh: refresh discovery connection
        :return:
        """
        try:
            # no need to refresh module if it is first initialization
            if self._discovery_agent is not None:
                DiscoveryServiceModule().refresh()
            self._discovery_agent = DiscoveryServiceModule().connection
        except TException as e:
            log.error("Service Proxy Error: failed to get discovery service: " + e.__class__.__name__ + " " + e.message)

    def _bind_methods(self):
        # magic method, bind thrift client method to the service proxy instance
        method_names = [method_name for method_name, method_type in self._cls.__dict__.items() if type(method_type) == FunctionType]
        for name in method_names:
            self.__dict__[name] = types.MethodType(self._create_proxy_method(name), self)

    def _close_instances(self):
        for instance in self._instances:
            if instance.is_open():
                instance.close()

    def refresh_service(self):
        """
        refresh connections of service end points
        :return:
        """
        # close current transport connection first
        # when thrift service is down, the is_open will still return True.
        # If thrift service is back, need to reopen connection, so close it first
        self._close_instances()
        self._init_service_end_points(self._identity)
        self._init_service(ServiceProxy.DEFAULT_TIMEOUT)

    def refresh_discovery_agent(self):
        """
        refresh discovery agent
        :return:
        """
        self._init_discovery_agent()

    @retry(exceptions=TException, tries=DEFALT_RETRIES, logger=log)
    def call_proxy_method(self, name, *args, **kwargs):
        """
        magix method to call thrift client method
        :param name: method name
        :param args:
        :param kwargs:
        :return:
        """
        local_client_wrapper_list = list(self._instances)
        shuffle(local_client_wrapper_list)
        for client_wrapper in local_client_wrapper_list:
            try:
                # lazy initialization for opening connection
                if not client_wrapper.is_open():
                    client_wrapper.open()
                client = client_wrapper.get_service_client()
                call_method = getattr(client, name)
                return call_method(*args, **kwargs)
            except Exception as e:
                log.error('Service Proxy Error: Exception when calling method: ' + name + ', Exception msg: ' + str(e) + ', Type:' +
                          e.__class__.__name__)
                log.info("try other endpoints")

        log.info("Cannot find available endpoints, refreshing....")
        try:
            self.refresh_discovery_agent()
        except Exception as e:
            # catch all eceptions to make sure doing retry if any exceptions raised during refreshing
            log.error("Service Proxy Error: got exception when refreshing discovery service: " + e.message)

        self.refresh_service()
        raise TException(message="All endpoints failed:" + str(self._endpoints))

    def _create_proxy_method(self, name):
        """
        bind method with service proxy object
        :param name: method name
        :return:
        """
        return lambda self, *args, **kwargs: self.call_proxy_method(name, *args, **kwargs)
