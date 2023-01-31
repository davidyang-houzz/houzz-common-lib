# Finds internal IP of a box.
# Refactored out of operation_utils.py & salt_utils.py on 2019-03-06.

from __future__ import absolute_import
from __future__ import print_function
import socket

def get_internal_ip():
    ip = ''
    try:
        ip = socket.gethostbyname(socket.gethostname())
    except:
        return ip
    return ip
