#!/usr/bin/env python
from __future__ import print_function
from __future__ import absolute_import
import logging
import socket
import time
import pickle
import struct

from houzz.common.thrift_gen.log_record import LogCollector
from houzz.common.thrift_gen.log_record.ttypes import LogRecord
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol


class HZSocketHandler(logging.Handler):
    """
    A handler class which writes logging records, in pickle format, to
    a streaming socket. The socket is kept open across logging calls.
    If the peer resets it, an attempt is made to reconnect on the next call.
    The pickle which is sent is that of the LogRecord's attribute dictionary
    (__dict__), so that the receiver does not need to have the logging module
    installed in order to process the logging event.

    To unpickle the record at the receiving end into a LogRecord, use the
    makeLogRecord function.
    """

    def __init__(self, host, port, log_name=None):
        """
        Initializes the handler with a specific host address and port.

        When the attribute *closeOnError* is set to True - if a socket error
        occurs, the socket is silently closed and then reopened on the next
        logging call.
        """
        logging.Handler.__init__(self)
        self.host = host
        self.port = port
        self.log_name = log_name
        if port is None:
            self.address = host
        else:
            self.address = (host, port)
        self.sock = None
        self.closeOnError = False
        self.retryTime = None
        #
        # Exponential backoff parameters.
        #
        self.retryStart = 1.0
        self.retryMax = 30.0
        self.retryFactor = 2.0

    def makeSocket(self, timeout=1):
        """
        A factory method which allows subclasses to define the precise
        type of socket they want.
        """
        if self.port is not None:
            result = socket.create_connection(self.address, timeout=timeout)
        else:
            result = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            result.settimeout(timeout)
            try:
                result.connect(self.address)
            except Exception:
                result.close()  # Issue 19182
                raise
        return result

    def createSocket(self):
        """
        Try to create a socket, using an exponential backoff with
        a max retry time. Thanks to Robert Olson for the original patch
        (SF #815911) which has been slightly refactored.
        """
        now = time.time()
        # Either retryTime is None, in which case this
        # is the first time back after a disconnect, or
        # we've waited long enough.
        if self.retryTime is None:
            attempt = True
        else:
            attempt = (now >= self.retryTime)
        if attempt:
            try:
                self.sock = self.makeSocket()
                self.retryTime = None  # next time, no delay before trying
            except Exception as err:
                if not hasattr(err, 'errno') or err.errno != 2:
                    raise
                # Creation failed, so set the retry time and return.
                if self.retryTime is None:
                    self.retryPeriod = self.retryStart
                else:
                    self.retryPeriod *= self.retryFactor
                    if self.retryPeriod > self.retryMax:
                        self.retryPeriod = self.retryMax
                self.retryTime = now + self.retryPeriod

    def send(self, s):
        """
        Send a pickled string to the socket.

        This function allows for partial sends which can happen when the
        network is busy.
        """
        if self.sock is None:
            self.createSocket()
        # self.sock can be None either because we haven't reached the retry
        # time yet, or because we have reached the retry time and retried,
        # but are still unable to connect.
        if self.sock:
            try:
                self.sock.sendall(s)
            except OSError: # pragma: no cover
                self.sock.close()
                self.sock = None  # so we can call createSocket next time
            except Exception as err:
                print(err)
                if not hasattr(err, 'errno') or err.errno != 32:
                    raise
                self.sock.close()
                self.sock = None  # so we can call createSocket next time
                print(err)

    def makePickle(self, record):
        """
        Pickles the record in binary format with a length prefix, and
        returns it ready for transmission across the socket.
        """
        ei = record.exc_info
        if ei:
            # just to get traceback text into record.exc_text ...
            dummy = self.format(record)
        # See issue #14436: If msg or args are objects, they may not be
        # available on the receiving end. So we convert the msg % args
        # to a string, save it as msg and zap the args.
        d = dict(record.__dict__)
        d['msg'] = record.getMessage()
        d['args'] = None
        d['exc_info'] = None
        d['log_name'] = self.log_name
        s = pickle.dumps(d, 1)
        slen = struct.pack(">L", len(s))
        return slen + s

    def handleError(self, record):
        """
        Handle an error during logging.

        An error has occurred during logging. Most likely cause -
        connection lost. Close the socket so that we can retry on the
        next event.
        """
        if self.closeOnError and self.sock:
            self.sock.close()
            self.sock = None        #try to reconnect next time
        else:
            logging.Handler.handleError(self, record)

    def emit(self, record):
        """
        Emit a record.

        Pickles the record and writes it to the socket in binary format.
        If there is an error with the socket, silently drop the packet.
        If there was a problem with the socket, re-establishes the
        socket.
        """
        try:
            s = self.makePickle(record)
            self.send(s)
        except Exception as err:
            print (err)
            self.handleError(record)

    def close(self):
        """
        Closes the socket.
        """
        self.acquire()
        try:
            sock = self.sock
            if sock:
                self.sock = None
                sock.close()
            logging.Handler.close(self)
        finally:
            self.release()


class HZThriftHandler(logging.Handler):
    def __init__(self, host, port, log_name=None):
        """
        Initializes the handler with a specific host address and port.

        When the attribute *closeOnError* is set to True - if a socket error
        occurs, the socket is silently closed and then reopened on the next
        logging call.
        """
        logging.Handler.__init__(self)
        self.host = host
        self.port = port
        self.log_name = log_name
        self.client = None
        self.closeOnError = False
        self.retryTime = None
        self.transport = None
        #
        # Exponential backoff parameters.
        #
        self.retryStart = 1.0
        self.retryMax = 30.0
        self.retryFactor = 2.0
        self.retryPeriod = 0

    def send(self, s):
        """
        Send a pickled string to the socket.

        This function allows for partial sends which can happen when the
        network is busy.
        """
        if self.client is None:
            self.create_client()
        # self.sock can be None either because we haven't reached the retry
        # time yet, or because we have reached the retry time and retried,
        # but are still unable to connect.
        if self.client:
            try:
                self.client.log(s)
            except (OSError, TTransport.TTransportException):
                self.client = None  # so we can call createSocket next time
                self.transport.close()
            except Exception as err:
                print(err)
                if not hasattr(err, 'errno') or (err.errno != 32 and err.errno != 57 and err.errno != 107):
                    raise
                self.client = None  # so we can call createSocket next time
                self.transport.close()

    def create_client(self):
        """
        Try to create a socket, using an exponential backoff with
        a max retry time. Thanks to Robert Olson for the original patch
        (SF #815911) which has been slightly refactored.
        """
        now = time.time()
        # Either retryTime is None, in which case this
        # is the first time back after a disconnect, or
        # we've waited long enough.
        if self.retryTime is None:
            attempt = True
        else:
            attempt = (now >= self.retryTime)
        if attempt:
            try:
                socket = TSocket.TSocket(self.host, self.port) if self.port else TSocket.TSocket(unix_socket=self.host)
                self.transport = TTransport.TFramedTransport(socket)
                protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
                self.client = LogCollector.Client(protocol)
                self.transport.open()
                self.retryTime = None  # next time, no delay before trying
            except TTransport.TTransportException as err:
                # Creation failed, so set the retry time and return.
                if self.retryTime is None:
                    self.retryPeriod = self.retryStart
                else:
                    self.retryPeriod *= self.retryFactor
                    if self.retryPeriod > self.retryMax:
                        self.retryPeriod = self.retryMax
                self.retryTime = now + self.retryPeriod

    def emit(self, record):
        """
        Emit a record.

        Pickles the record and writes it to the socket in binary format.
        If there is an error with the socket, silently drop the packet.
        If there was a problem with the socket, re-establishes the
        socket.
        """
        try:
            fmt = logging.Formatter()
            if record.exc_info:
                if not record.exc_text:
                    record.exc_text = fmt.formatException(record.exc_info)

            rec = LogRecord(
                msg=record.getMessage(),
                log_name=self.log_name,
                name=record.name,
                level=record.levelno,
                pathname=record.pathname,
                lineno=record.lineno,
                func=record.funcName,
                thread=record.thread,
                threadname=record.threadName,
                processname=record.processName,
                process=record.process,
                exc_text=record.exc_text
            )
            self.send(rec)
        except Exception as err:
            self.handleError(record)
