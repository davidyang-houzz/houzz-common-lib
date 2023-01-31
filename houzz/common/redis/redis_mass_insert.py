from __future__ import absolute_import
from inspect import ismethod
import itertools
import subprocess
import contextlib
import tempfile
import collections

from houzz.common.redis_utils import get_redis_connection

REDIS_TOTAL_SLOTS = 16384


@contextlib.contextmanager
def dict_closing(d):
    try:
        yield d
    finally:
        for val in d.values():
            val.close()

class TemoraryWrapper(object):
    def __init__(self):
        self._temp_file = tempfile.TemporaryFile(mode='w')

    def __getattr__(self, attr):
        return self._temp_file.__getattribute__(attr)

    @property
    def __class__(self):
        return self._temp_file.__class__

class RedisMassInsert(object):
    def __init__(self, conf, cluster=True):
        self._conf = conf

        self._slots_info_cache = []
        self._cluster = cluster

        # The file dict for each master instance of the cluster
        self._file_dict = collections.defaultdict(TemoraryWrapper)

        self.conn = get_redis_connection(self._conf)
        if self._cluster:
            self._init_slots_info_cache()
        else:
            self._slots_info_cache = (self._conf['host'], str(self._conf['port']))

    def _init_slots_info_cache(self):
        """ Init the slots info cache list """

        nodes = self.conn.cluster_nodes()
        self._slots_info_cache = [None] * REDIS_TOTAL_SLOTS
        for node in nodes:
            if 'master' in node.get('flags', []):
                for slot in node.get('slots', []):
                    self._slots_info_cache[slot] = (node['host'], node['port'])

    def get_key_slot(self, key):
        """
        Get the host/port information of slot based on the key.
        We will try to calculate keyslot locally instead of calling 
        command "CLUSTER KEYSLOT" if available, which is more 
        efficient because of no RTT and Req/Resp parsing.
        """

        if self._cluster:
            if (
                self.conn.connection_pool
                and self.conn.connection_pool.nodes
                and ismethod(self.conn.connection_pool.nodes.keyslot)
            ):
                slot = self.conn.connection_pool.nodes.keyslot(key)
            else:
                slot = self.conn.cluster_keyslot(key)

            return self.get_slot_info(slot)
        return self._slots_info_cache

    def get_slot_info(self, slot):
        """ Get the host/port information of slot. """

        return self._slots_info_cache[slot]

    def write_temp_file(self, slot_info, string):
        self._file_dict[slot_info].write(string)

    def flush_temp_file(self):
        """ prepare files for reading """

        for redis_file in self._file_dict.values():
            redis_file.flush()  # ensure everything is written
            redis_file.seek(0)  # make everything readable

    def run_mass_insert(self):
        """ Read all the mass insertion file and execute all cmds into the target instances. """

        self.flush_temp_file()
        with dict_closing(self._file_dict):
            for (host, port), command_file in self._file_dict.items():
                self.run_pipe(command_file, host, port)
        self._file_dict = collections.defaultdict(TemoraryWrapper)

    @staticmethod
    def run_pipe(input_file, host, port):
        subprocess.check_call(['redis-cli', '-h', host, '-p', str(port), '--pipe'], stdin=input_file)

    @staticmethod
    def redis_command(*args):
        r""" Generate the mass insertion string from the command args.

        >>> redis_command("SET", "key", "value")
        "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        >>> redis_command("SADD", "key1", "value1", "value2")
        "*4\r\n$4\r\nSADD\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
        >>> redis_command("MULTI")
        '*1\r\n$5\r\nMULTI\r\n'
        """

        # Using concatenation instead of string template and join is 2x faster
        # when input args is a short array
        cmd = '*' + str(len(args)) + '\r\n'
        for arg in map(str, args):
            cmd += '$' + str(len(arg.encode("utf8"))) + '\r\n' + arg + '\r\n'
        return cmd

    def add_single_cmd(self, slot, *arg_list):
        cmd = self.redis_command(*arg_list)
        self.write_temp_file(slot, cmd)
        return cmd

    def redis_set(self, key, val):
        return self.add_single_cmd(self.get_key_slot(key), 'SET', key, val)

    def set_ex(self, key, val, expire_seconds):
        return self.add_single_cmd(self.get_key_slot(key), 'SET', key, val, 'EX', expire_seconds)

    def hmset(self, key, val_dict):
        val_pairs = itertools.chain.from_iterable(list(val_dict.items()))
        return self.add_single_cmd(self.get_key_slot(key), 'HMSET', key, *val_pairs)

    def array_hmset(self, key, vals):
        return self.add_single_cmd(self.get_key_slot(key), 'HMSET', key, *vals)

    def array_hmset_expire(self, key, vals, expire_seconds):
        cmd = ''.join((
            self.redis_command('HMSET', key, *vals),
            self.redis_command('EXPIRE', key, expire_seconds),
        ))
        self.write_temp_file(self.get_key_slot(key), cmd)
        return cmd

    def array_hmreplace(self, key, vals):
        cmd = ''.join((
            self.redis_command('MULTI'),
            self.redis_command('DEL', key),
            self.redis_command('HMSET', key, *vals),
            self.redis_command('EXEC'),
        ))
        self.write_temp_file(self.get_key_slot(key), cmd)
        return cmd

    def array_hmreplace_expire(self, key, vals, expire_seconds):
        cmd = ''.join((
            self.redis_command('MULTI'),
            self.redis_command('DEL', key),
            self.redis_command('HMSET', key, *vals),
            self.redis_command('EXPIRE', key, expire_seconds),
            self.redis_command('EXEC'),
        ))
        self.write_temp_file(self.get_key_slot(key), cmd)
        return cmd

    def sreplace_expire(self, key, values, expire_seconds):
        cmd = ''.join((
            self.redis_command('MULTI'),
            self.redis_command('DEL', key),
            self.redis_command('SADD', key, *values),
            self.redis_command('EXPIRE', key, expire_seconds),
            self.redis_command('EXEC'),
        ))
        self.write_temp_file(self.get_key_slot(key), cmd)
        return cmd

    def sreplace(self, key, values):
        cmd = ''.join((
            self.redis_command('MULTI'),
            self.redis_command('DEL', key),
            self.redis_command('SADD', key, *values),
            self.redis_command('EXEC'),
        ))
        self.write_temp_file(self.get_key_slot(key), cmd)
        return cmd

    def sadd(self, key, values):
        return self.add_single_cmd(self.get_key_slot(key), 'SADD', key, *values)

    def zadd(self, key, score, values):
        score_values = itertools.chain.from_iterable((score, value) for value in values)
        return self.add_single_cmd(self.get_key_slot(key), 'ZADD', key, *score_values)

    def delete(self, key):
        return self.add_single_cmd(self.get_key_slot(key), 'DEL', key)

    def expire(self, key, td):
        return self.add_single_cmd(self.get_key_slot(key), 'EXPIRE', key, int(td.total_seconds()))
