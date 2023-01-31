from __future__ import absolute_import
__author__ = 'menglei'

import socket, struct

VISITOR_ID_CHAR_LIMIT = 5
ABTEST_SEED_COOKIE_NAME = "abts"
ABTEST_NO_IP_HASH = "NO_IP"


class ABTestSeed:
    """
    This class encapsulate the User-related seeds that helps place users into ABTests buckets
    In the future, session info might be used and stored in the seed
    """
    @staticmethod
    def get_user_id_hash(user_id):
        if not isinstance(user_id, str):
            return int(user_id)
        else:
            return user_id

    @staticmethod
    def get_ip_hash(ip):
        if isinstance(ip, str):
            return ABTestSeed.ip2long(ip)
        else:
            raise Exception("not a valid ip")

    @staticmethod
    def get_visitor_id_hash(visitor_id):
        if isinstance(visitor_id, str):
            return int(visitor_id[0:VISITOR_ID_CHAR_LIMIT], 16)
        else:
            raise Exception("not a valid visitor id")

    @staticmethod
    def get_idfa_hash(idfa):
        if isinstance(idfa, str):
            return int(idfa[0:VISITOR_ID_CHAR_LIMIT], 16)
        else:
            raise Exception("not a valid idfa")

    @staticmethod
    def ip2long(ip):
        """
        Convert an IP string to long
        """
        packedIP = socket.inet_aton(ip)
        return struct.unpack("!L", packedIP)[0]

