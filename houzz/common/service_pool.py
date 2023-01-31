from __future__ import absolute_import
from random import randint
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol


class ServicePool:
    def __init__(self, cls, end_points):
        self.cls = cls
        self._init_service(end_points)

    def _init_service(self, end_points, timeout=10):
        """

        :param end_points:
        :param timeout:
        :return:
        """
        self._instances = []
        self._transports = []

        for end_point in end_points:
            host = end_point.host
            port = end_point.port
            socket = TSocket.TSocket(host, port)
            socket.setTimeout(timeout)
            transport = TTransport.TFramedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            self._instances.append(self.cls(protocol))
            self._transports.append(transport)
            transport.open()

    def refresh(self, end_points):
        """

        :param end_points:
        :return:
        """
        for transport in self._transports:
            transport.close()

        self._init_service(end_points)

    def get_service(self):
        """

        :return:
        """
        idx = randint(0, len(self._instances) - 1)
        return self._instances[idx]

