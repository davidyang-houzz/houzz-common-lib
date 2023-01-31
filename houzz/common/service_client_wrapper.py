from __future__ import absolute_import
__author__='menglei'

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

class ServiceClientWrapper:
    def __init__(self, cls, host, port, timeout=10):
        self._cls = cls
        self._host = host
        self._port = port
        self._socket = TSocket.TSocket(host, port)
        self._transport = TTransport.TFramedTransport(self._socket)
        self._protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
        self._instance = self._cls(self._protocol)
        # lazy initialization, will not open the connection in the first place

    def is_open(self):
        return self._transport.isOpen()

    def open(self):
        return self._transport.open()

    def close(self):
        return self._transport.close()

    def get_service_client(self):
        return self._instance
