#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import threading
import unittest

from houzz.common.metric.components.controller import Controller
from six.moves import range

class ControllerTest(unittest.TestCase):

    def test_controller_singleton_thread_safe(self):

        all_controllers = set()

        def create(*args, **kwargs):
            s = Controller(*args, **kwargs)
            all_controllers.add(id(s))

        all_threads = []
        for i in range(1000):
            t = threading.Thread(target=create, args=(i,))  # param i will be ignored
            all_threads.append(t)
            t.start()
        for t in all_threads:
            t.join()

        self.assertEqual(len(all_controllers), 1)
