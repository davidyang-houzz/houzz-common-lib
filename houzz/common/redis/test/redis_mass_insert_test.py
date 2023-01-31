#!/usr/bin/env python

from __future__ import absolute_import
from datetime import timedelta
import unittest
import time

from houzz.common.redis.redis_mass_insert import RedisMassInsert


class RedisMassInsertTests(unittest.TestCase):
    def setUp(self):
        self.redis_mass_insert = RedisMassInsert({'cluster': {'host': '192.168.10.134', 'port': 7000}}, cluster=True) # Test by STG
        self.redis_mass_insert_single = RedisMassInsert({'host': 'localhost', 'port': 6379}, cluster=False)

    def test_slot_info_init(self):
        self.assertEqual(len(self.redis_mass_insert._slots_info_cache), 16384)
        self.assertTrue(all(item for item in self.redis_mass_insert._slots_info_cache))

    def test_key_slot(self):
        # Test slot
        slot = self.redis_mass_insert.conn.cluster_keyslot('a')
        self.redis_mass_insert._slots_info_cache = [None] * 16384
        self.redis_mass_insert._slots_info_cache[slot] = ('127.0.0.1', 7000)
        self.assertEqual(self.redis_mass_insert.get_key_slot('a'), ('127.0.0.1', 7000))

        # Test cache item
        self.redis_mass_insert._slots_info_cache = [('127.0.0.1', 7000)]
        self.assertEqual(self.redis_mass_insert.get_slot_info(0), ('127.0.0.1', 7000))

    def test_run_pipe(self):
        clients = [self.redis_mass_insert]
        for client in clients:
            client.redis_set('key', 'value1')
            client.run_mass_insert()
            self.assertEqual(client.conn.get('key'), b'value1')

            client.set_ex('key', 'value2', 1)
            client.run_mass_insert()
            self.assertEqual(client.conn.get('key'), b'value2')
            time.sleep(1)
            self.assertEqual(client.conn.get('key'), None)

            client.hmset('key3', {"field2": "Hello2"})
            client.run_mass_insert()
            self.assertEqual(client.conn.hget('key3', 'field2'), b'Hello2')

            client.array_hmset('key2', ["field2", "Hello2"])
            client.run_mass_insert()
            self.assertEqual(client.conn.hget('key2', 'field2'), b'Hello2')

            client.array_hmset_expire('key2', ["field2", "Hello2"], 1)
            client.run_mass_insert()
            self.assertEqual(client.conn.hget('key2', 'field2'), b'Hello2')
            time.sleep(1)
            self.assertEqual(client.conn.hget('key2', 'field2'), None)

            client.conn.set('key', 'value')
            client.array_hmreplace('key', ["value1", "value2"])
            client.run_mass_insert()
            self.assertEqual(client.conn.hget('key', 'value1'), b'value2')

            client.conn.set('key', 'value')
            client.array_hmreplace_expire('key', ["value1", "value2"], 1)
            client.run_mass_insert()
            self.assertEqual(client.conn.hget('key', 'value1'), b'value2')
            time.sleep(1)
            self.assertEqual(client.conn.hget('key', 'value1'), None)

            client.conn.set('key', 'value')
            client.sreplace('key', ["value1", "value2"])
            client.run_mass_insert()
            self.assertEqual(client.conn.smembers('key'), {b"value1", b"value2"})

            client.conn.set('key', 'value')
            client.sreplace_expire('key', ["value1", "value2"], 1)
            client.run_mass_insert()
            self.assertEqual(client.conn.smembers('key'), {b"value1", b"value2"})
            time.sleep(1)
            self.assertEqual(client.conn.smembers('key'), set([]))

            client.sadd('key', ["value1", "value2"])
            client.run_mass_insert()
            self.assertEqual(client.conn.smembers('key'), {b"value1", b"value2"})

            client.delete('key')
            client.run_mass_insert()
            self.assertEqual(client.conn.smembers('key'), set([]))

            client.zadd('key', 2, ["value1", "value2"])
            client.run_mass_insert()
            self.assertEqual(client.conn.zrange('key', 0, -1), [b"value1", b"value2"])

            client.expire('key', timedelta(seconds=1))
            client.run_mass_insert()
            time.sleep(1)
            self.assertEqual(client.conn.smembers('key'), set([]))

    def test_command(self):
        self.assertEqual("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
                         self.redis_mass_insert.redis_set('key', 'value'))
        self.assertEqual("*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$2\r\n10\r\n",
                         self.redis_mass_insert.set_ex('key', 'value', 10))
        self.assertEqual("*4\r\n$5\r\nHMSET\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$5\r\nHello\r\n",
                         self.redis_mass_insert.hmset('key', {"field1": "Hello"}))
        self.assertEqual("*4\r\n$5\r\nHMSET\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n",
                         self.redis_mass_insert.array_hmset('key', ["value1", "value2"]))
        self.assertEqual("*4\r\n$5\r\nHMSET\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
                         "*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n",
                         self.redis_mass_insert.array_hmset_expire('key', ["value1", "value2"], 10))
        self.assertEqual("*1\r\n$5\r\nMULTI\r\n"
                         "*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n"
                         "*4\r\n$5\r\nHMSET\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
                         "*1\r\n$4\r\nEXEC\r\n",
                         self.redis_mass_insert.array_hmreplace('key', ["value1", "value2"]))
        self.assertEqual("*1\r\n$5\r\nMULTI\r\n"
                         "*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n"
                         "*4\r\n$5\r\nHMSET\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
                         "*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n"
                         "*1\r\n$4\r\nEXEC\r\n",
                         self.redis_mass_insert.array_hmreplace_expire('key', ["value1", "value2"], 10))
        self.assertEqual("*1\r\n$5\r\nMULTI\r\n"
                         "*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n"
                         "*4\r\n$4\r\nSADD\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
                         "*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n"
                         "*1\r\n$4\r\nEXEC\r\n",
                         self.redis_mass_insert.sreplace_expire('key', ["value1", "value2"], 10))
        self.assertEqual("*1\r\n$5\r\nMULTI\r\n"
                         "*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n"
                         "*4\r\n$4\r\nSADD\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
                         "*1\r\n$4\r\nEXEC\r\n",
                         self.redis_mass_insert.sreplace('key', ["value1", "value2"]))
        self.assertEqual("*4\r\n$4\r\nSADD\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n",
                         self.redis_mass_insert.sadd('key', ["value1", "value2"]))
        self.assertEqual("*6\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n2\r\n$6\r\nvalue1\r\n$1\r\n2\r\n$6\r\nvalue2\r\n",
                         self.redis_mass_insert.zadd('key', 2, ["value1", "value2"]))
        self.assertEqual("*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n",
                         self.redis_mass_insert.delete('key'))
        self.assertEqual("*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n",
                         self.redis_mass_insert.expire('key', timedelta(seconds=10)))

if __name__ == '__main__':
    unittest.main()