#!/usr/bin/env python
from __future__ import absolute_import
import time
import unittest

from houzz.common.logger.perf_monitor import PerfMonitor, get_perf_count, get_perf_count_without_dur


class TestClass(object):
    def __eq__(self, other):
        return isinstance(other, TestClass)


class PerfMonitorTests(unittest.TestCase):
    def setUp(self):
        import os
        real = os.getenv('PERF_REAL')
        if real:
            self.perf_mon = PerfMonitor.set_instance(host='logger.sock')
        else:
            self.perf_mon = PerfMonitor.set_instance(disable_metrics=True)

    def test_counter_total(self):
        with self.perf_mon.get_perf('test_counter_total') as counter:
            counter.count_total()
            self.assertEquals(1, counter.total)

    def test_counter_error(self):
        with self.perf_mon.get_perf('test_counter_error') as counter:
            counter.count_error()
            self.assertEquals(1, counter.error)

    def test_counter(self):
        with self.perf_mon.get_perf('test_counter') as counter:
            counter.count_total()
            counter.count_total()
            counter.count_error()
            self.assertEquals(2, counter.total)
            self.assertEquals(1, counter.error)

    def test_period(self):
        with self.perf_mon.get_perf('test_period') as counter:
            counter.count_total()
            counter.start_period()
            time.sleep(1)
            counter.end_period()
            counter.count_total()
            counter.count_error()
            self.assertEquals(2, counter.total)
            self.assertEquals(1, counter.error)
            self.assertNotEqual(None, counter.dur_sec)

    def test_auto_period(self):
        with self.perf_mon.get_perf('test_auto_period') as counter:
            counter.count_total()
            time.sleep(1)

            self.assertEquals(1, counter.total)

            self.assertNotEqual(None, counter.dur_sec)

    def test_counter_decoration(self):
        @get_perf_count('test_counter_decoration')
        def test1():
            pass

        test1()

    def test_counter_only_decoration(self):
        @get_perf_count_without_dur('test_counter_only_decoration')
        def test1():
            pass
        test1()
