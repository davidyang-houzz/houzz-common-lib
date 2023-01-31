from __future__ import absolute_import
__author__ = 'jonathan'

'''
python is a terrible language for dst

https://tommikaikkonen.github.io/timezones/
http://pytz.sourceforge.net/
'''

import calendar
import time
from datetime import datetime, timedelta
from pytz import timezone, utc

PST_TZ = timezone('US/Pacific')


# def is_dst(ts, tz):
#     now = utc.localize(datetime.fromtimestamp(ts))
#     return now.astimezone(tz).dst() != timedelta(0)


def get_days_in_month(ts, tz=PST_TZ):
    dt = datetime.fromtimestamp(ts, tz)
    (_, days_in_month) = calendar.monthrange(dt.year, dt.month)

    return days_in_month


def ts_to_ms(ts):
    return ts * 1000


def ms_to_ts(ms):
    return ms / 1000


def datetime_to_ts(dt):
    '''
    :param dt: datetime to convert to ts
    :return: timestamp in milliseconds
    '''

    return time.mktime(dt.timetuple())


def get_current_dom(ts, tz):
    return datetime.fromtimestamp(ts, tz).day


def get_datetime_from_ts(ts, tz):
    return datetime.fromtimestamp(ts, tz)


def get_datetime_from_string(str, tz):
    '''
    :param str: String in form at yyyy-mm-dd
    :param tz: timezone for datetime
    :return: datetime object corresponding to the beginning of that day in PST
    '''

    return tz.localize(datetime.strptime(str, '%Y-%m-%d'))


def get_string_from_datetime(dt):
    '''
    :param dt: datetime object to convert to string format
    :return: datetime in string format
    '''

    return dt.strftime('%Y-%m-%d')


def get_beginning_of_day(ts, tz=PST_TZ, offset=None):
    '''
    :param ts: current timestamp
    :param tz: time zone
    :param offset: timedeta of offset in days/months/years/etc
    :return: datetime of the beginning of the current day
    '''

    dt = datetime.fromtimestamp(ts).replace(hour=0, minute=0, second=0)

    if offset:
        dt += offset

    return tz.localize(dt)


def get_first_day_of_current_month(ts, tz):
    '''
    :param ts: current timestamp
    :param tz: time zone
    :return: datetime corresponding to first DOM of that day
    '''

    return tz.localize(datetime.fromtimestamp(ts).replace(day=1, hour=0, minute=0, second=0))


def get_first_day_of_previous_month(ts, tz):
    '''
    :param ts: current timestamp
    :param tz: time zone
    :return:  datetime corresponding to the first day of the month preceding the current month
    '''

    first_day_of_current_month = get_first_day_of_current_month(ts, tz)
    return get_first_day_of_current_month(datetime_to_ts(first_day_of_current_month - timedelta(days=1)), tz)


def get_last_day_of_current_month(ts, tz):
    '''
    :param ts: current timestamp
    :param tz: time zone
    :return: datetime corresponding to beginning of day of last day in that month for the timezone
    '''

    first_dom = get_first_day_of_current_month(ts, tz)
    days_in_month = calendar.monthrange(first_dom.year, first_dom.month)[1]
    return tz.localize(datetime(year=first_dom.year, month=first_dom.month, day=days_in_month))


def get_date_time_range(start_ts, end_ts, step, tz):
    '''
    Returns a list corresponding to the start timestamp of

    :param start_ts: start of period to search for (inclusive)
    :param end_ts: end of period (inclusive)
    :param step: timedelta step
    :param tz: timezone of start dates
    :return: list of starts of days in between start ts and end ts
    '''

    res = list()
    curr_date = datetime.fromtimestamp(start_ts)
    end_date = datetime.fromtimestamp(end_ts)

    while curr_date < end_date:
        res.append(get_beginning_of_day(datetime_to_ts(curr_date), tz))
        curr_date += step

    return res
