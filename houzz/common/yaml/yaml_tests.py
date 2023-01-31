from __future__ import absolute_import
import datetime
import unittest

import yaml.constructor
import yaml.representer

from . import load, dump, safe_dump, safe_load, pretty_dump


class TestClass(object):
    def __eq__(self, other):
        return isinstance(other, TestClass)


class YamlTests(unittest.TestCase):
    def setUp(self):
        self.date_hour_string = "{first_time: 2014-09-05T06}\n"
        self.date_hour = {'first_time': datetime.datetime(2014, 9, 5, 6)}

        self.date_string = "{first_time: 2014-09-04}\n"
        self.date = {'first_time': datetime.date(2014, 9, 4)}

        self.pretty_date_string = "first_time: 2014-09-04\n"

    def test_date_hour_load(self):
        self.assertEqual(
            self.date_hour, load(self.date_hour_string))

    def test_date_hour_dump(self):
        self.assertEqual(
            self.date_hour_string,  dump(self.date_hour))

    def test_date_load(self):
        self.assertEqual(
            self.date,  load(self.date_string))

    def test_date_dump(self):
        self.assertEqual(self.date_string,  dump(self.date))

    def test_safe_load(self):
        self.assertEqual(
            self.date,  safe_load(self.date_string))
        self.assertEqual(
            self.date_hour,  safe_load(self.date_hour_string))

    def test_safe_load_is_safe(self):
        dump1 = dump(TestClass())
        self.assertEqual(TestClass(),  load(dump1))
        with self.assertRaises(yaml.constructor.ConstructorError):
            safe_load(dump1)

    def test_safe_dump(self):
        self.assertEqual(
            self.date_string,  safe_dump(self.date))
        self.assertEqual(
            self.date_hour_string,  safe_dump(self.date_hour))

    def test_safe_dump_is_safe(self):
        with self.assertRaises(yaml.representer.RepresenterError):
            safe_dump(TestClass())

    def test_pretty_dump(self):
        self.assertEqual(
            self.pretty_date_string,  pretty_dump(self.date))
