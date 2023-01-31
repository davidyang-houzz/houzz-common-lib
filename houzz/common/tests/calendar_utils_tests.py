#!/usr/bin/env python
from __future__ import absolute_import
__author__ = 'jonathan'

import unittest
from datetime import timedelta

from houzz.common import calendar_utils


class CalendarTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_current_dom(self):
        # November 9th pst, Nov 10th GMT
        day1 = calendar_utils.get_current_dom(1541835205, calendar_utils.PST_TZ)
        self.assertEqual(day1, 9)

        # November 1st PST
        day2 = calendar_utils.get_current_dom(1541140405, calendar_utils.PST_TZ)
        self.assertEqual(day2, 1)

        # May 31st PST
        day3 = calendar_utils.get_current_dom(1527750025, calendar_utils.PST_TZ)
        self.assertEqual(day3, 31)


    def test_get_datetime_from_string(self):
        ts1 = calendar_utils.datetime_to_ts(
            calendar_utils.get_datetime_from_string('2018-10-10', calendar_utils.PST_TZ))
        ts2 = calendar_utils.datetime_to_ts(
            calendar_utils.get_datetime_from_string('2018-12-12', calendar_utils.PST_TZ))

        # Non DST
        self.assertEqual(1539154800, ts1)

        # DST
        self.assertEqual(1544601600, ts2)

    def test_get_beginning_of_day(self):
        # no offset
        ts1 = calendar_utils.datetime_to_ts(
            calendar_utils.get_beginning_of_day(1541200308, calendar_utils.PST_TZ))
        self.assertEqual(1541142000, ts1)

        # offset
        ts2 = calendar_utils.datetime_to_ts(
            calendar_utils.get_beginning_of_day(1541200308, calendar_utils.PST_TZ, -timedelta(days=1)))
        self.assertEqual(ts2, 1541055600)

        # DST
        ts3 = calendar_utils.datetime_to_ts(
            calendar_utils.get_beginning_of_day(1541422983, calendar_utils.PST_TZ, -timedelta(days=2)))
        self.assertEqual(ts3, 1541228400)

    def test_first_day_of_current_month(self):
        # october
        dt1 = calendar_utils.get_first_day_of_current_month(1541004319, calendar_utils.PST_TZ)
        ts1 = calendar_utils.datetime_to_ts(dt1)
        self.assertEqual(1538377200, ts1)

        # December
        dt2 = calendar_utils.get_first_day_of_current_month(1544009582, calendar_utils.PST_TZ)
        ts2 = calendar_utils.datetime_to_ts(dt2)
        self.assertEqual(1543651200, ts2)

    def test_last_day_of_current_month(self):
        # October
        dt1 = calendar_utils.get_last_day_of_current_month(1541004319, calendar_utils.PST_TZ)
        ts1 = calendar_utils.datetime_to_ts(dt1)
        self.assertEqual(1540969200, ts1)

        # December
        dt2 = calendar_utils.get_last_day_of_current_month(1544009582, calendar_utils.PST_TZ)
        ts2 = calendar_utils.datetime_to_ts(dt2)
        self.assertEqual(1546243200, ts2)

    def test_get_date_time_range(self):
        '''
        Verify we get the start of each day from november 1st to november 7th
        '''
        self.maxDiff = None
        range1 = [
                calendar_utils.datetime_to_ts(dt) for dt in
                calendar_utils.get_date_time_range(1541055600, 1541646773, timedelta(days=1), calendar_utils.PST_TZ)
        ]
        self.assertEqual([1541055600, 1541142000, 1541228400, 1541314800, 1541404800, 1541491200, 1541577600], range1)

    def test_first_day_of_previous_month(self):
        # december 2018
        dt1 = calendar_utils.get_first_day_of_previous_month(1543651200, calendar_utils.PST_TZ)
        ts1 = calendar_utils.datetime_to_ts(dt1)
        self.assertEqual(1541055600, ts1)  # november 2018

        # january 2019
        dt2 = calendar_utils.get_first_day_of_previous_month(1546329600, calendar_utils.PST_TZ)
        ts2 = calendar_utils.datetime_to_ts(dt2)
        self.assertEqual(1543651200, ts2)  # december 2018

