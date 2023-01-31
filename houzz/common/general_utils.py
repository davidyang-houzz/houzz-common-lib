from __future__ import absolute_import
from threading import Thread, Event
import socket


def call_repeatedly(interval, fn):
    stopped = Event()

    def loop():
        while not stopped.wait(interval):
            # the first call is in `interval` secs
            fn()
    func_thread = Thread(target=loop)
    func_thread.setDaemon(True)
    func_thread.start()
    return stopped.set


def get_ip():
    # finds external-facing interface without sending any packets (Linux)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('1.0.0.1', 0))
        ip = s.getsockname()[0]
        return ip
    except socket.error:
        pass

    # Generic method, sometimes gives useless results
    try:
        dumb_ip = socket.gethostbyaddr(socket.gethostname())[2][0]
        if not dumb_ip.startswith('127.') and ':' not in dumb_ip:
            return dumb_ip
    except (socket.gaierror, socket.herror):
        dumb_ip = '127.0.0.1'

    # works elsewhere, but actually sends a packet
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('1.0.0.1', 1))
        ip = s.getsockname()[0]
        return ip
    except socket.error:
        pass

    return dumb_ip
