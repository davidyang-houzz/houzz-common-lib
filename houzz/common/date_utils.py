#!/usr/bin/python

''' Collection of date utility functions.
'''

from __future__ import absolute_import
from __future__ import print_function
import calendar

from datetime import datetime, timedelta
from six.moves import range


def date_get_first_month_day(dd=datetime.today()):
    ''' given a datetime, return a datetime object for the first day of the month of the given date.
    '''
    return dd.replace(day=1)


def date_get_last_month_day(dd=datetime.today()):
    ''' given a datetime, return a datetime object for last day of the month of given date.
    '''
    return dd.replace(day=get_last_month_day(dd))


def get_last_month_day(dd=datetime.today()):
    ''' given a datetime, return the last month day as an integer.
    '''
    return calendar.monthrange(dd.year, dd.month)[1]


def get_month_delta(dd_from, dd_to):
    ''' given 2 datetime objects, return number of months from dd_from to dd_to.
    '''
    return (dd_to.year - dd_from.year) * 12 + dd_to.month - dd_from.month


def date_get_month_delta(original=datetime.today(), month_delta=None):
    ''' given original datetime, return a datetime object with month delta applied.
        Use negative delta values for previous months.
    '''
    result = original
    if month_delta > 0:
        for _i in range(month_delta):
            result = result.replace(day=get_last_month_day(result)) + timedelta(days=1)
        result = result.replace(day=min(original.day, get_last_month_day(result)))
    elif month_delta < 0:
        for _i in range(abs(month_delta)):
            result = result.replace(day=1) - timedelta(days=1)
        result = result.replace(day=min(original.day, get_last_month_day(result)))
    return result


def _test():
    ''' testing. '''
    dd = datetime(2013, 4, 18)
    assert date_get_first_month_day(dd) == datetime(2013, 4, 1)
    assert date_get_last_month_day(dd) == datetime(2013, 4, 30)
    assert get_month_delta(dd, datetime(2013, 5, 1)) == 1
    assert get_month_delta(dd, datetime(2013, 3, 1)) == -1
    assert get_month_delta(datetime(2013, 4, 1), datetime(2013, 5, 1)) == 1
    assert get_month_delta(datetime(2013, 4, 1), datetime(2013, 1, 1)) == -3
    assert get_month_delta(datetime(2012, 10, 1), datetime(2013, 1, 1)) == 3

    assert date_get_month_delta(dd, 1) == datetime(2013, 5, 18)
    assert date_get_month_delta(dd, 2) == datetime(2013, 6, 18)
    assert date_get_month_delta(dd, 9) == datetime(2014, 1, 18)
    assert date_get_month_delta(dd, -1) == datetime(2013, 3, 18)
    assert date_get_month_delta(dd, -2) == datetime(2013, 2, 18)
    assert date_get_month_delta(dd, -9) == datetime(2012, 7, 18)
    assert date_get_month_delta(datetime(2013, 2, 28), -1) == datetime(2013, 1, 28)
    assert date_get_month_delta(datetime(2013, 3, 31), -1) == datetime(2013, 2, 28)
    assert date_get_month_delta(datetime(2013, 3, 31), -2) == datetime(2013, 1, 31)

    print("All tests passed.")

if __name__ == '__main__':
    _test()
