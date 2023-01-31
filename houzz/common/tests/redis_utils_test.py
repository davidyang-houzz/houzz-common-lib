#!/usr/bin/env python

""" Test the Python redis utils """

from __future__ import absolute_import
import redis
import rediscluster
import unittest
import time
from houzz.common.redis_utils import get_redis_connection
from houzz.common.config import get_config

Config = get_config()
TTL_CONFIG = {
    "redis_cluster_ttl": 172800,
    "redis_ttl_whitelist": {
        "no_ttl": 0,
        "customized_ttl": 100
    }
}


class RedisUtilsTests(unittest.TestCase):
    def setUp(self):
        self.cluster_redis = get_redis_connection({'cluster': Config.DEV_REDIS_PLATINUM_CLUSTER_PARAMS}, strict=False)
        self.cluster_redis.set_ttl_config(TTL_CONFIG)

        self.single_redis = get_redis_connection({'host': 'localhost', 'port': 6379}, strict=False)
        self.single_redis.set_ttl_config(TTL_CONFIG)

        self.connections = [self.cluster_redis, self.single_redis]

    def test_basic(self):
        # Test the class of connections are redis.StrictRedis
        for connection in self.connections:
            self.assertTrue(isinstance(connection, redis.StrictRedis))

        # Test the class of cluster_redis are rediscluster.StrictRedisCluster
        self.assertTrue(isinstance(self.cluster_redis, rediscluster.StrictRedisCluster))

    def test_ttl(self):
        # Test the default TTL
        for connection in self.connections:
            self.assertTrue(connection.set("test:foo", "bar1"))
            self.assertEqual(connection.get("test:foo"), "bar1")
            self.assertIsNotNone(connection.ttl("test:foo"))
            self.addCleanup(connection.delete, "test:foo")

            # Test the key in the whitelist without TTL
            self.assertTrue(connection.set("no_ttl:foo", "bar1"))
            if isinstance(connection, rediscluster.StrictRedisCluster):
                self.assertEqual(-1, connection.ttl("no_ttl:foo"))
            else:
                self.assertIsNone(connection.ttl("no_ttl:foo"))
            self.addCleanup(connection.delete, "no_ttl:foo")

            # Test the key in the whitelist with customized TTL
            self.assertTrue(connection.set("customized_ttl:foo", "bar1"))
            self.assertGreaterEqual(100, connection.ttl("customized_ttl:foo"))

            # Test getset
            time.sleep(2)
            self.assertEqual("bar1", connection.getset("customized_ttl:foo", "bar2"))
            self.assertLess(98, connection.ttl("customized_ttl:foo"))
            self.addCleanup(connection.delete, "customized_ttl:foo")

    def test_mset(self):
        for connection in self.connections:
            self.assertTrue(connection.mset({"customized_ttl:mfoo": "bar", "no_ttl:mfoo": "bar"}, mfoo2="bar"))
            self.assertGreaterEqual(100, connection.ttl("customized_ttl:mfoo"))
            if isinstance(connection, rediscluster.StrictRedisCluster):
                self.assertEqual(-1, connection.ttl("no_ttl:mfoo"))
            else:
                self.assertIsNone(connection.ttl("no_ttl:mfoo"))
            self.assertIsNotNone(connection.ttl("mfoo2"))
            self.addCleanup(connection.delete, "customized_ttl:mfoo", "no_ttl:mfoo", "mfoo2")

    def test_msetnx(self):
        for connection in self.connections:
            # Test msetnx failure
            connection.set("customized_ttl:mnxfoo", "bar")
            self.assertFalse(connection.msetnx({"customized_ttl:mnxfoo": "bar", "no_ttl:mnxfoo": "bar"}, mnxfoo2="bar"))

            # Test msetnx successfully
            connection.delete("customized_ttl:mnxfoo", "no_ttl:mnxfoo", "mnxfoo2")
            self.assertTrue(connection.msetnx({"customized_ttl:mnxfoo": "bar", "no_ttl:mnxfoo": "bar"}, mnxfoo2="bar"))
            self.assertIsNotNone(connection.get("customized_ttl:mnxfoo"))
            self.assertGreaterEqual(100, connection.ttl("customized_ttl:mnxfoo"))
            if isinstance(connection, rediscluster.StrictRedisCluster):
                self.assertEqual(-1, connection.ttl("no_ttl:mnxfoo"))
            else:
                self.assertIsNone(connection.ttl("no_ttl:mnxfoo"))
            self.assertIsNotNone(connection.ttl("mnxfoo2"))
            self.addCleanup(connection.delete, "customized_ttl:mnxfoo", "no_ttl:mnxfoo", "mnxfoo2")

    def test_incr(self):
        for connection in self.connections:
            connection.set("customized_ttl:mincfoo", 1)
            time.sleep(2)
            self.assertEqual(2, connection.incr("customized_ttl:mincfoo"))

            # We will set the new ttl after incr
            self.assertLess(98, connection.ttl("customized_ttl:mincfoo"))

            # Test incrby
            time.sleep(2)
            self.assertEqual(4, connection.incrby("customized_ttl:mincfoo", 2))
            self.assertLess(98, connection.ttl("customized_ttl:mincfoo"))

            # Test incrbyfloat
            time.sleep(2)
            self.assertEqual(4.5, connection.incrbyfloat("customized_ttl:mincfoo", 0.5))
            self.assertLess(98, connection.ttl("customized_ttl:mincfoo"))
            self.addCleanup(connection.delete, "customized_ttl:mincfoo")

    def test_zadd(self):
        for connection in self.connections:
            if isinstance(connection, rediscluster.StrictRedisCluster):
                self.assertEqual(1, connection.zadd("customized_ttl:sortedset", 1, "one"))
            else:
                self.assertEqual(1, connection.zadd("customized_ttl:sortedset", "one", 1))
            self.assertGreaterEqual(100, connection.ttl("customized_ttl:sortedset"))

            time.sleep(2)
            if isinstance(connection, rediscluster.StrictRedisCluster):
                self.assertEqual(2, connection.zadd("customized_ttl:sortedset", 2, "two", 3, "three"))
            else:
                self.assertEqual(2, connection.zadd("customized_ttl:sortedset", "two", 2, "three", 3))
            self.assertLess(98, connection.ttl("customized_ttl:sortedset"))
            self.assertEqual(["one", "two", "three"], connection.zrange("customized_ttl:sortedset", 0, -1))
            self.addCleanup(connection.delete, "customized_ttl:sortedset")

    def test_sadd(self):
        for connection in self.connections:
            self.assertEqual(1, connection.sadd("customized_ttl:set", "one"))
            self.assertGreaterEqual(100, connection.ttl("customized_ttl:set"))

            time.sleep(2)
            self.assertEqual(2, connection.sadd("customized_ttl:set", "two", "three"))
            self.assertLess(98, connection.ttl("customized_ttl:set"))
            self.assertEqual(sorted(["one", "two", "three"]), sorted(connection.smembers("customized_ttl:set")))
            self.addCleanup(connection.delete, "customized_ttl:set")

    def test_pipeline(self):
        with self.cluster_redis.pipeline() as pipe:
            pipe.set("customized_ttl:pipe_v1", 1)
            pipe.execute()

        for connection in self.connections:
            with connection.pipeline() as pipe:
                pipe.set("customized_ttl:pipe_v1", 1)
                pipe.set("customized_ttl:pipe_v2", 2)
                self.assertEqual([True, True], pipe.execute())
                self.assertLessEqual(connection.ttl("customized_ttl:pipe_v1"), 100)
                self.assertLess(98, connection.ttl("customized_ttl:pipe_v2"))
                pipe.__del__()
                self.assertEqual(0, len(pipe))

            time.sleep(2)
            with connection.pipeline() as pipe:
                pipe.incr("customized_ttl:pipe_v1", 2)
                pipe.getset("customized_ttl:pipe_v1", 5)
                pipe.sadd("customized_ttl:pipe_s1", "one")
                pipe.smembers("customized_ttl:pipe_s1")
                self.assertEqual(4, len(pipe))
                pipe_name = "StrictClusterPipeline" if isinstance(pipe, rediscluster.StrictRedisCluster) else "Pipeline"
                self.assertEqual(pipe_name, str(pipe))
                self.assertEqual([3, '3', 1, set(["one"])], pipe.execute())
                self.assertLess(98, connection.ttl("customized_ttl:pipe_v1"))
                self.assertLess(98, connection.ttl("customized_ttl:pipe_s1"))
            self.assertEqual(0, len(pipe))

            pipe_clean = connection.pipeline(transaction=False)
            pipe_clean.delete("customized_ttl:pipe_v1")
            pipe_clean.delete("customized_ttl:pipe_v2")
            pipe_clean.delete("customized_ttl:pipe_s1")
            self.addCleanup(pipe_clean.execute)
