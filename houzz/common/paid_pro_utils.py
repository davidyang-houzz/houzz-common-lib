#!/usr/bin/env python

'''Utils related to paid pro features.'''

from __future__ import absolute_import
from collections import defaultdict
import copy
from datetime import timedelta
from six.moves import map

import logging

from houzz.common import mysql_utils
from houzz.common import redis_utils
from houzz.common import config
import six
from six.moves import zip_longest

logger = logging.getLogger(__name__)
Config = config.get_config()


PHOTO_CAMPAIGN_NAME_PREFIX = 'PaidProPhotoPage:'
AD_TRACKING_PREFIX = 'adTracking'
PRO_TRACKING_PREFIX = 'proTracking'
BROWSE_PHOTO_TOP_RIGHT_PLACEMENT_ID = 60001
BROWSE_PHOTO_PLACEMENT_ID = 60002
IPAD_BROWSE_PHOTO_PLACEMENT_ID = 60018
IPHONE_BROWSE_PHOTO_PLACEMENT_ID = 60021
LIGHTBOX_PHOTO_BROWSE_PLACEMENT_ID = 60057

ADVERTISER_TYPE_UNKNOWN = 0
ADVERTISER_TYPE_BRAND = 1
ADVERTISER_TYPE_PRO_PLUS = 2

EVENT_UNKNOWN = 'non'
EVENT_IMPRESSION = 'imp'
EVENT_CLICK = 'clk'
EVENT_BUZZ = 'buz'
EVENT_EMAIL = 'eml'
PROTRACKING_EVENT_LIST = [EVENT_IMPRESSION, EVENT_CLICK]
EVENT_ENUM_UNKNOWN = 0
EVENT_ENUM_IMPRESSION = 2
EVENT_ENUM_CLICK = 3
EVENT_ENUM_BUZZ = 4
EVENT_ENUM_EMAIL = 5
EVENT_ENUM_NAME_MAP = {
    EVENT_ENUM_UNKNOWN: EVENT_UNKNOWN,
    EVENT_ENUM_IMPRESSION: EVENT_IMPRESSION,
    EVENT_ENUM_CLICK: EVENT_CLICK,
    EVENT_ENUM_BUZZ: EVENT_BUZZ,
    EVENT_ENUM_EMAIL: EVENT_EMAIL,
}
EVENT_NAME_ENUM_MAP = dict((v, k) for (k, v) in six.iteritems(EVENT_ENUM_NAME_MAP))

PAGE_UNKNOWN = 'non'
PAGE_PHOTO = 'brw'
PAGE_PROFESSIONAL = 'pro'
PAGE_API_PHOTO = 'abr'
PAGE_API_PROFESSIONAL = 'apr'
PAGE_PROFILE = 'ppf'
PAGE_LIGHTBOX = 'lit'
PAGE_PROFESSIONAL_BANNER_B = 'pbb'
PAGE_PROFESSIONAL_BANNER_C = 'pbc'
PAGE_NEWSLETTER = 'pnl'
PROTRACKING_PAGE_LIST = [
    PAGE_PHOTO, PAGE_PROFESSIONAL, PAGE_API_PHOTO, PAGE_API_PROFESSIONAL, PAGE_PROFILE,
    PAGE_LIGHTBOX, PAGE_PROFESSIONAL_BANNER_B, PAGE_PROFESSIONAL_BANNER_C, PAGE_NEWSLETTER,
]
PAGE_ENUM_UNKNOWN = 0
PAGE_ENUM_PHOTO = 1
PAGE_ENUM_PROFESSIONAL = 2
PAGE_ENUM_API_PHOTO = 3
PAGE_ENUM_API_PROFESSIONAL = 4
PAGE_ENUM_PROFILE = 5
PAGE_ENUM_LIGHTBOX = 6
PAGE_ENUM_PROFESSIONAL_BANNER_B = 7
PAGE_ENUM_PROFESSIONAL_BANNER_C = 8
PAGE_ENUM_NEWSLETTER = 9
PAGE_ENUM_NAME_MAP = {
    PAGE_ENUM_UNKNOWN: PAGE_UNKNOWN,
    PAGE_ENUM_PHOTO: PAGE_PHOTO,
    PAGE_ENUM_PROFESSIONAL: PAGE_PROFESSIONAL,
    PAGE_ENUM_API_PHOTO: PAGE_API_PHOTO,
    PAGE_ENUM_API_PROFESSIONAL: PAGE_API_PROFESSIONAL,
    PAGE_ENUM_PROFILE: PAGE_PROFILE,
    PAGE_ENUM_LIGHTBOX: PAGE_LIGHTBOX,
    PAGE_ENUM_PROFESSIONAL_BANNER_B: PAGE_PROFESSIONAL_BANNER_B,
    PAGE_ENUM_PROFESSIONAL_BANNER_C: PAGE_PROFESSIONAL_BANNER_C,
    PAGE_ENUM_NEWSLETTER: PAGE_NEWSLETTER,
}
PAGE_NAME_ENUM_MAP = dict((v, k) for (k, v) in six.iteritems(PAGE_ENUM_NAME_MAP))
LOCATION_BROWSE_PAGE = 'bphoto'
LOCATION_LIGHTBOX = 'lightbox'
LOCATION_LIST = [LOCATION_BROWSE_PAGE, LOCATION_LIGHTBOX]
LOCATION_PAGE_MAP = {
    LOCATION_BROWSE_PAGE: PAGE_PHOTO,
    LOCATION_LIGHTBOX: PAGE_LIGHTBOX,
}

PRO_PHOTO_PAGE_PLACEMENT_LIST = [
    BROWSE_PHOTO_TOP_RIGHT_PLACEMENT_ID,
    BROWSE_PHOTO_PLACEMENT_ID,
    IPAD_BROWSE_PHOTO_PLACEMENT_ID,
    IPHONE_BROWSE_PHOTO_PLACEMENT_ID,
    LIGHTBOX_PHOTO_BROWSE_PLACEMENT_ID,
]
MOBILE_PLACEMENT_PAGE_MAP = {
    IPAD_BROWSE_PHOTO_PLACEMENT_ID: PAGE_API_PHOTO,
    IPHONE_BROWSE_PHOTO_PLACEMENT_ID: PAGE_API_PHOTO,
}

OBJECT_UNKNOWN = 'non'
OBJECT_PHOTO = 'pho'
OBJECT_PROFILE = 'fil'
OBJECT_EXTERNAL_LINK = 'elk'
PROTRACKING_OBJECT_LIST = [OBJECT_PHOTO, OBJECT_PROFILE, OBJECT_EXTERNAL_LINK]
OBJECT_ENUM_UNKNOWN = 0
OBJECT_ENUM_PHOTO = 1
OBJECT_ENUM_PROFILE = 2
OBJECT_ENUM_EXTERNAL_LINK = 3
OBJECT_ENUM_NAME_MAP = {
    OBJECT_ENUM_UNKNOWN: OBJECT_UNKNOWN,
    OBJECT_ENUM_PHOTO: OBJECT_PHOTO,
    OBJECT_ENUM_PROFILE: OBJECT_PROFILE,
    OBJECT_ENUM_EXTERNAL_LINK: OBJECT_EXTERNAL_LINK,
}
OBJECT_NAME_ENUM_MAP = dict((v, k) for (k, v) in six.iteritems(OBJECT_ENUM_NAME_MAP))

SPONSOR_UNKNOWN = 'non'
SPONSOR_ORGANIC = 'org'
SPONSOR_PROMOTED = 'pay'
PROTRACKING_SPONSOR_LIST = [SPONSOR_ORGANIC, SPONSOR_PROMOTED]
SPONSOR_ENUM_UNKNOWN = 0
SPONSOR_ENUM_ORGANIC = 1
SPONSOR_ENUM_PROMOTED = 2
SPONSOR_ENUM_NAME_MAP = {
    SPONSOR_ENUM_UNKNOWN: SPONSOR_UNKNOWN,
    SPONSOR_ENUM_ORGANIC: SPONSOR_ORGANIC,
    SPONSOR_ENUM_PROMOTED: SPONSOR_PROMOTED,
}
SPONSOR_NAME_ENUM_MAP = dict((v, k) for (k, v) in six.iteritems(SPONSOR_ENUM_NAME_MAP))

GEO_UNKNOWN = '/'

CREATIVE_TYPE_PRO_PHOTO = 5
CREATIVE_TYPE_PRO_BANNER = 6
PRO_CREATIVE_TYPES = [CREATIVE_TYPE_PRO_PHOTO, CREATIVE_TYPE_PRO_BANNER]

SQL_GET_BUZZ_COUNT_BY_HOUSE_DAY = '''
SELECT house_id, DATE(created) AS day, COUNT(*) AS num FROM c2.buzz
WHERE house_id in (%s) AND created>='%s' AND created<'%s'
GROUP BY house_id, day ORDER BY house_id, day
'''
SQL_GET_ALL_PRO_PHOTOS = '''
SELECT h.owner_id, h.house_id FROM c2.houses h WHERE h.owner_id in (%s)
'''
SQL_GET_ALL_PRO_USER_IDS = '''
SELECT DISTINCT user_id FROM c2.paid_professionals
'''
SQL_INSERT_PRO_STATS = '''
insert into c2.paid_professional_stats (user_id, house_id, day, geo, cost_type, page,
impression, click, profile_impression, profile_click, buzz, site_click, email) values (
%(user_id)s, %(house_id)s, %(day)s, %(geo)s, %(cost_type)s, %(page)s, %(impression)s,
%(click)s, %(profile_impression)s, %(profile_click)s, %(buzz)s, %(site_click)s, %(email)s)
'''
SQL_DELETE_PRO_STATS = '''
delete from c2.paid_professional_stats where day >= %(start)s and day <= %(end)s
'''
SQL_GET_EMAIL_PRO_COUNT_BY_USER_DAY = '''
SELECT e.to_user_id AS user_id, DATE(e.created) AS day, COUNT(*) AS num
FROM c2.email_logs e INNER JOIN c2.email_content_logs c
ON e.content_id=c.email_content_id
WHERE e.to_user_id in (%s) AND e.created>='%s' AND e.created<'%s' AND c.type=10
GROUP BY user_id, day ORDER BY user_id, day
'''
SQL_GET_PROFESSIONAL_NAME_BY_USER  = '''
SELECT user_id, name FROM professionals WHERE user_id in (%s)
'''
SQL_GET_USER_NAME_BY_USER  = '''
SELECT user_id, name FROM users WHERE user_id in (%s)
'''
SQL_GET_PRO_PLUS_START_DATE_BY_USER  = '''
SELECT user_id, DATE(created) AS day FROM paid_professionals WHERE user_id in (%s)
'''
SQL_GET_PACKAGE_LOCATIONS_BY_USER  = '''
SELECT psi.user_id, smm.name FROM pro_subscription_info psi JOIN sub_metro_mappings smm ON
psi.sub_metro_id=smm.sub_metro_id
WHERE psi.status=1 and psi.user_id in (%s)
GROUP BY psi.user_id, smm.name ORDER BY psi.user_id, smm.name;
'''
SQL_GET_PACKAGE_NUMBERS_BY_USER = '''
SELECT user_id, count(*) FROM pro_subscription_info
WHERE status=1 and user_id in (%s)
GROUP BY user_id
'''
SQL_GET_ACTIVE_PRO_USERS = '''
SELECT user_id FROM paid_professionals WHERE pro_status=1
'''

def grouper(n, iterable, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)

cached_geo_conversion = {}
def get_geo_key(geo):
    '''Normalize geo name.
    >>> get_geo_key('/US/CA/530')
    '/US/CA'
    >>> get_geo_key('/US/CA/')
    '/US/CA'
    >>> get_geo_key('/EU/UK')
    '/EU/UK'
    >>> get_geo_key('/EU/')
    '/EU'
    >>> get_geo_key('/123')
    '/'
    >>> get_geo_key('')
    '/'
    '''
    if geo not in cached_geo_conversion:
        compo = [_f for _f in geo.split('/') if _f]
        if compo and compo[-1].isdigit():
            compo.pop()
        new_geo = '/%s' % ('/'.join(compo))
        cached_geo_conversion[geo] = new_geo
    return cached_geo_conversion[geo]

def store_stats(stats):
    '''Store aggregated data to MySQL.'''
    logger.info('start store stats. stats_len:%s', len(stats))
    logger.debug('stats: %s', stats)
    default_params = {
        'user_id': 0,
        'house_id': 0,
        'day': '',
        'geo': '',
        'cost_type': 0,
        'page': 0,
        'impression': 0,
        'click': 0,
        'profile_impression': 0,
        'profile_click': 0,
        'buzz': 0,
        'site_click': 0,
        'email': 0,
    }
    rows = []
    for key, val in six.iteritems(stats):
        params = copy.copy(default_params)
        (params['day'], params['user_id'], params['house_id'],
                params['geo'], cost, page) = key
        params['cost_type'] = SPONSOR_NAME_ENUM_MAP[cost]
        params['page'] = PAGE_NAME_ENUM_MAP[page]
        (params['impression'], params['click'], params['profile_impression'],
                params['profile_click'], params['buzz'],
                params['site_click'], params['email']) = val
        rows.append(params)
    mysql_utils.run_bulk_update(SQL_INSERT_PRO_STATS, rows)
    logger.info('end store stats. stats_len:%s', len(stats))

def combine_pro_stats(pro_stats, result):
    # pro_stats key on (day, pro_id, geo, cost, event, page, obj)
    # output key on (day, pro_id, house_id, geo, cost, page)
    # combined metric is (imp, click, profile_imp, profile_clk, buzz, site_click, email)
    empty_metric = [0, 0, 0, 0, 0, 0, 0]
    for key, val in six.iteritems(pro_stats):
        (day, pro_id, geo, cost, event, page, obj) = key
        new_key = (day, pro_id, 0, geo, cost, page)
        if new_key not in result:
            result[new_key] = copy.copy(empty_metric)
        if event == EVENT_IMPRESSION:
            if obj == OBJECT_PHOTO:
                result[new_key][0] += val
            elif obj == OBJECT_PROFILE:
                result[new_key][2] += val
            else:
                raise ValueError('unknown object type: %s, for event: %s' % (obj, event))
        elif event == EVENT_CLICK:
            if obj == OBJECT_PHOTO:
                result[new_key][1] += val
            elif obj == OBJECT_PROFILE:
                result[new_key][3] += val
            elif obj == OBJECT_EXTERNAL_LINK:
                result[new_key][5] += val
            else:
                raise ValueError('unknown object type: %s, for event: %s' % (obj, event))
        elif event == EVENT_BUZZ:
            result[new_key][4] += val
        elif event == EVENT_EMAIL:
            result[new_key][6] += val
        else:
            raise ValueError('unknown event type: %s' % event)

def combine_house_stats(house_stats, result):
    # house_stats key on (day, pro_id, house_id, event)
    # output key on (day, pro_id, house_id, geo, cost, page)
    # combined metric is (imp, click, profile_imp, profile_clk, buzz, site_click, email)
    empty_metric = [0, 0, 0, 0, 0, 0, 0]
    for key, val in six.iteritems(house_stats):
        (day, pro_id, house_id, event) = key
        new_key = (day, pro_id, house_id, GEO_UNKNOWN, SPONSOR_UNKNOWN, PAGE_UNKNOWN)
        if new_key not in result:
            result[new_key] = copy.copy(empty_metric)
        if event == EVENT_IMPRESSION:
            result[new_key][0] += val
        elif event == EVENT_CLICK:
            result[new_key][1] += val
        elif event == EVENT_BUZZ:
            result[new_key][4] += val
        else:
            raise ValueError('unknown event type: %s' % event)

def combine_stats(
        pro_stats_ad, pro_stats_pro, house_stats_pro,
        pro_stats_buzz, house_stats_buzz,
        pro_stats_email):
    '''Combine different metrics on the same key together.'''
    logger.info('start combine stats')
    result = {}
    combine_pro_stats(pro_stats_ad, result)
    combine_pro_stats(pro_stats_pro, result)
    combine_pro_stats(pro_stats_buzz, result)
    combine_pro_stats(pro_stats_email, result)
    combine_house_stats(house_stats_pro, result)
    combine_house_stats(house_stats_buzz, result)
    logger.info('end combine stats')
    return result

def aggregate_adtracking_stats(redis_conn, start, end, creative_to_user_houses, pro_user_ids):
    '''Use adtracking for profile click on browse photo page.'''
    logger.info('start adtracking aggregation on date range [%s, %s]', start, end)
    # pro_stats key on (day, pro_id, geo, cost, event, page, obj)
    pro_stats = defaultdict(int)
    current = start
    pipe = redis_conn.pipeline()
    while current <= end:
        logger.info('work on day: %s', current)
        timestamp = current.strftime('%s')
        # Example adtracking key:
        # 'adTracking:clk:daily:1361174400:creative:52757:bphoto:60002:geo' (browse photo)
        # 'adTracking:clk:daily:1361174400:creative:52760:bphoto:60001:geo' (browse top-right)
        # 'adTracking:clk:daily:1361174400:creative:52757:lightbox:60002:geo' (lightbox)
        # 'adTracking:clk:daily:1342508400:creative:51969:bphoto:60018:geo' (ipad browse)
        # 'adTracking:clk:daily:1342508400:creative:51969:bphoto:60021:geo' (iphone browse)
        #
        # It corresponds to 'proTracking:clk:brw:fil:pay' or
        #                   'proTracking:clk:lit:fil:pay'
        event = EVENT_CLICK
        cost = SPONSOR_PROMOTED
        keys = []
        for placement_id in PRO_PHOTO_PAGE_PLACEMENT_LIST:
            for creative_id in creative_to_user_houses:
                for location in LOCATION_LIST:
                    if location == LOCATION_LIGHTBOX and placement_id in MOBILE_PLACEMENT_PAGE_MAP:
                        # No 'lightbox' on mobile
                        continue
                    keys.append('%s:%s:daily:%s:creative:%s:%s:%s:geo' % (
                        AD_TRACKING_PREFIX, event, timestamp, creative_id, location, placement_id))
        logger.info('event:%s, num_of_possible_keys:%s', event, len(keys))
        logger.debug('event:%s, possible_keys:\n%s', event, '\n'.join(keys))

        for keys_batch in grouper(redis_utils.QUERY_BATCH_SIZE, keys):
            # group into smaller chunks to reduce load on redis
            query_keys = [_f for _f in keys_batch if _f]
            logger.info('redis pipeline query. num_of_keys: %s', len(query_keys))
            for key in query_keys:
                pipe.zrange(key, 0, -1, withscores=True, score_cast_func=int)
            pipe_result = pipe.execute()
            for key, geo_stats in map(None, query_keys, pipe_result):
                if geo_stats:
                    (_d0, _d1, _d2, _d3, _d4, id_str, location, placement_id, _d8) = key.split(':')
                    placement_id = int(placement_id)
                    if placement_id in MOBILE_PLACEMENT_PAGE_MAP:
                        page = MOBILE_PLACEMENT_PAGE_MAP[placement_id]
                    else:
                        page = LOCATION_PAGE_MAP[location]
                    creative_id = int(id_str)
                    pro_id, _house_ids = creative_to_user_houses[creative_id]
                    # count the click differently for pro and brand advertisers
                    if pro_id in pro_user_ids:
                        obj = OBJECT_PROFILE
                    else:
                        obj = OBJECT_PHOTO
                    for geo, stat in geo_stats:
                        geo = get_geo_key(geo)
                        pro_stats[(current, pro_id, geo, cost, event, page, obj)] += stat
        current += timedelta(days=1)
    logger.info('end adtracking aggregation on date range [%s, %s]', start, end)
    return pro_stats

def aggregate_protracking_stats(redis_conn, start, end, house_to_pro):
    '''Aggregate on protracking stats.
    Example redis proTracking keys:
    'proTracking:clk:brw:fil:org:1347865200:123',
    'proTracking:clk:brw:pho:org:1347865200:123',
    'proTracking:clk:brw:pho:pay:1347865200:123',
    'proTracking:clk:lit:fil:org:1347865200:123',
    'proTracking:clk:lit:pho:org:1347865200:123',
    'proTracking:clk:lit:pho:pay:1347865200:123',
    'proTracking:clk:pro:fil:org:1347865200:123',
    'proTracking:clk:pro:fil:pay:1347865200:123',
    'proTracking:imp:brw:pho:org:1347865200:123',
    'proTracking:imp:brw:pho:pay:1347865200:123',
    'proTracking:imp:lit:pho:org:1347865200:123',
    'proTracking:imp:lit:pho:pay:1347865200:123',
    'proTracking:imp:pro:fil:org:1347865200:123',
    'proTracking:imp:pro:fil:pay:1347865200:123',
    'proTracking:clk:ppf:elk:pay:1347865200:123',
    'proTracking:imp:abr:pho:org:1363158000:685567',
    'proTracking:imp:abr:pho:pay:1344668400:52346',
    'proTracking:clk:abr:fil:org:1363158000:135',
    'proTracking:clk:apr:fil:org:1363158000:135',
    'proTracking:clk:apr:fil:pay:1363158000:135',
    '''
    logger.info('start protracking aggregation on date range [%s, %s]', start, end)
    # pro_stats key on (day, pro_id, geo, cost, event, page, obj)
    # Need event in the key to separate imp v.s. click.
    # Need obj in the key to separate profile imp/click from photo imp/click.
    pro_stats = defaultdict(int)
    # house_stats key on (day, pro_id, house_id, event)
    house_stats = defaultdict(int)

    all_ids = set(house_to_pro.keys()) | set(house_to_pro.values())
    current = start
    pipe = redis_conn.pipeline()
    while current <= end:
        logger.info('work on day: %s', current)
        timestamp = current.strftime('%s')
        for event in PROTRACKING_EVENT_LIST:
            keys = []
            for page in PROTRACKING_PAGE_LIST:
                for obj in PROTRACKING_OBJECT_LIST:
                    for cost in PROTRACKING_SPONSOR_LIST:
                        # collect all the possible redis keys
                        for one_id in all_ids:
                            keys.append('%s:%s:%s:%s:%s:%s:%s' % (
                                PRO_TRACKING_PREFIX, event, page, obj, cost, timestamp, one_id))
            logger.info('event:%s, num_of_possible_keys:%s', event, len(keys))
            logger.debug('event:%s, possible_keys:\n%s', event, '\n'.join(keys))

            for keys_batch in grouper(redis_utils.QUERY_BATCH_SIZE, keys):
                # group into smaller chunks to reduce load on redis
                query_keys = [_f for _f in keys_batch if _f]
                logger.info('redis pipeline query. num_of_keys: %s', len(query_keys))
                for key in query_keys:
                    pipe.zrange(key, 0, -1, withscores=True, score_cast_func=int)
                pipe_result = pipe.execute()
                for key, geo_stats in map(None, query_keys, pipe_result):
                    if geo_stats:
                        (_d0, _d1, page, obj, cost, _d5, id_str) = key.split(':')
                        id_num = int(id_str)
                        if obj == OBJECT_PHOTO:
                            pro_id = house_to_pro.get(id_num, 0)
                            house_id = id_num
                        else:
                            pro_id = id_num
                            house_id = 0
                        for geo, stat in geo_stats:
                            geo = get_geo_key(geo)
                            if obj == OBJECT_PHOTO:
                                house_stats[(current, pro_id, house_id, event)] += stat
                            pro_stats[(current, pro_id, geo, cost, event, page, obj)] += stat
        current += timedelta(days=1)
    logger.info('end protracking aggregation on date range [%s, %s]', start, end)
    return (pro_stats, house_stats)

def aggregate_buzz_stats(start, end, pro_to_houses):
    '''Get add-to-ideabook stats from MySQL.'''
    logger.info('start buzz aggregation on date range [%s, %s]', start, end)
    # pro_stats key on (day, pro_id, geo, cost, event, page, obj)
    pro_stats = defaultdict(int)
    # house_stats key on (day, pro_id, house_id, event)
    house_stats = defaultdict(int)
    geo = GEO_UNKNOWN
    cost = SPONSOR_UNKNOWN
    event = EVENT_BUZZ
    obj = OBJECT_UNKNOWN
    page = PAGE_UNKNOWN

    current = start
    while current <= end:
        for pro_id, house_ids in six.iteritems(pro_to_houses):
            query = SQL_GET_BUZZ_COUNT_BY_HOUSE_DAY % (
                    ','.join(str(i) for i in house_ids), current, current+timedelta(days=1))
            for rows in mysql_utils.run_query(query, chunk_size=1000):
                for row in rows:
                    (house_id, _day, count) = row
                    pro_stats[(current, pro_id, geo, cost, event, page, obj)] += count
                    house_stats[(current, pro_id, house_id, event)] += count
        current += timedelta(days=1)
    logger.info('end buzz aggregation on date range [%s, %s]', start, end)
    return (pro_stats, house_stats)

def aggregate_email_stats(start, end, pros):
    '''Get email-pro on profile page stats from MySQL.'''
    logger.info('start email-pro aggregation on date range [%s, %s]', start, end)
    # pro_stats key on (day, pro_id, geo, cost, event, page, obj)
    pro_stats = defaultdict(int)
    geo = GEO_UNKNOWN
    cost = SPONSOR_PROMOTED
    event = EVENT_EMAIL
    obj = OBJECT_UNKNOWN
    page = PAGE_UNKNOWN

    current = start
    while current <= end:
        query = SQL_GET_EMAIL_PRO_COUNT_BY_USER_DAY % (
                ','.join(str(i) for i in pros), current, current+timedelta(days=1))
        for rows in mysql_utils.run_query(query, chunk_size=1000):
            for row in rows:
                (user_id, _day, count) = row
                pro_stats[(current, user_id, geo, cost, event, page, obj)] += count
        current += timedelta(days=1)
    logger.info('end email-pro aggregation on date range [%s, %s]', start, end)
    return pro_stats

def get_user_houses_maps(user_ids):
    '''Get both user_id => [house_id,] map, and house_id => user_id map.'''
    logger.info('start get_user_houses_maps')
    pro_to_houses = defaultdict(list)
    house_to_pro = {}
    if user_ids:
        query = SQL_GET_ALL_PRO_PHOTOS % (','.join(str(i) for i in user_ids))
        for rows in mysql_utils.run_query(query, chunk_size=1000):
            for row in rows:
                (user_id, house_id) = row
                pro_to_houses[user_id].append(house_id)
                house_to_pro[house_id] = user_id
    logger.info('end get_user_houses_maps. pro_len:%s, house_len:%s',
            len(pro_to_houses), len(house_to_pro))
    return (pro_to_houses, house_to_pro)

def get_paid_pro_user_ids():
    '''Get user_id of all the paid pros.'''
    logger.info('start get_paid_pro_user_ids')
    pro_user_ids = set([])
    query = SQL_GET_ALL_PRO_USER_IDS
    for rows in mysql_utils.run_query(query, chunk_size=1000):
        for row in rows:
            pro_user_ids.add(row[0])
    logger.info('end get_paid_pro_user_ids. users_len:%s', len(pro_user_ids))
    return pro_user_ids

def get_ad_id_mapping(redis_conn, possible_keys, str_val_convert=int):
    '''Return parent_id => [child_id,] for ad objects.'''
    logger.debug('get_ad_id_mapping. possible_keys: %s', possible_keys)
    mapping = {}
    all_children_ids = []
    pipe = redis_conn.pipeline()
    for key in possible_keys:
        pipe.type(key)
    pipe_result = pipe.execute()
    keys = []
    # find all existing keys
    for key, val in map(None, possible_keys, pipe_result):
        if val:
            keys.append(key)

    if not keys:
        return (mapping, set(all_children_ids))
    val_type = redis_conn.type(keys[0])

    if val_type == 'string':
        func = pipe.get
    elif val_type == 'set':
        func = pipe.smembers
    elif val_type == 'none':
        # key may not exist. ie, campaign impression cap may not always defined. Treat it as string
        func = pipe.get
        val_type = 'string'
    else:
        raise ValueError('Unsupported redis value type: %s' % val_type)
    for key in keys:
        func(key)
    pipe_result = pipe.execute()
    for key, val in map(None, keys, pipe_result):
        if val:
            parent_id = int(key.split(':')[1])
            if val_type == 'string':
                if key.endswith(':houseId'):  # maybe comma delimited
                    house_ids = [str_val_convert(i.strip()) for i in val.split(',')]
                    mapping[parent_id] = house_ids
                    all_children_ids.extend(house_ids)
                else:
                    mapping[parent_id] = [str_val_convert(val)]
                    all_children_ids.append(str_val_convert(val))
            else:
                ids = [int(v) for v in val]
                mapping[parent_id] = ids
                all_children_ids.extend(ids)
    logger.debug('get_ad_id_mapping. mapping:%s', mapping)
    logger.debug('get_ad_id_mapping. all_children_ids:%s', set(all_children_ids))
    return (mapping, set(all_children_ids))

def get_advertiser_to_user_mapping(redis_conn):
    '''Get advertiser_id => user_id for all advertisers.'''
    all_advertisers = [int(i) for i in redis_conn.smembers('lists:advertiser:all')]
    advertiser_to_user, _all_users = get_ad_id_mapping(
            redis_conn, ['advertiser:%d:userId' % i for i in all_advertisers])
    return dict((k, v[0]) for k, v in six.iteritems(advertiser_to_user))

def get_user_to_advertiser_mapping(redis_conn):
    '''Get user_id => advertiser_id for all advertisers.'''
    advertiser_to_user = get_advertiser_to_user_mapping(redis_conn)
    return dict((v, k) for k, v in six.iteritems(advertiser_to_user))

def get_user_to_advertiser_type_mapping(redis_conn):
    '''Get user_id => advertiser_type for all advertisers.'''
    advertiser_to_user = get_advertiser_to_user_mapping(redis_conn)
    possible_keys = ['advertiser:%d:advertiserType' % i for i in advertiser_to_user]
    pipe = redis_conn.pipeline()
    for key in possible_keys:
        pipe.get(key)
    pipe_result = pipe.execute()
    user_to_type = {}
    for key, val in map(None, possible_keys, pipe_result):
        if val:
            advertiser_id = int(key.split(':')[1])
            user_to_type[advertiser_to_user[advertiser_id]] = int(val)
    return user_to_type

def get_creative_to_user_and_houses_map(redis_conn):
    '''Get creative_id => (user_id, [house_id,]) for all ads
    (ProPhoto, ProBanner, Photo, Banner, etc).'''
    logger.info('start get_creative_to_user_and_houses_map')
    result = {}
    # get all advertisers
    all_advertisers = [int(i) for i in redis_conn.smembers('lists:advertiser:all')]
    # get advertiser => user
    advertiser_to_user, _all_users = get_ad_id_mapping(
            redis_conn, ['advertiser:%d:userId' % i for i in all_advertisers])
    # get advertiser => campaigns
    advertiser_to_campaigns, all_campaigns = get_ad_id_mapping(
            redis_conn, ['advertiser:%d:campaignIds' % i for i in all_advertisers])
    # get campaign => adGroups
    campaign_to_adgroups, all_adgroups = get_ad_id_mapping(
            redis_conn, ['campaign:%d:adGroupIds' % i for i in all_campaigns])
    # get adGroup => creatives
    adgroup_to_creatives, all_creatives = get_ad_id_mapping(
            redis_conn, ['adGroup:%d:creativeIds' % i for i in all_adgroups])
    # get creative => creative_type
    creative_to_type, _all_types = get_ad_id_mapping(
            redis_conn, ['creative:%d:creativeType' % i for i in all_creatives])
    # get creative => house_ids
    creative_to_houses, _all_houses = get_ad_id_mapping(
            redis_conn, ['creative:%d:houseId' % i for i in all_creatives])

    # get creative => user
    for advertiser_id, user_ids in six.iteritems(advertiser_to_user):
        user_id = user_ids[0]
        for campaign_id in advertiser_to_campaigns.get(advertiser_id, []):
            for adgroup_id in campaign_to_adgroups.get(campaign_id, []):
                for creative_id in adgroup_to_creatives.get(adgroup_id, []):
                    for creative_type in creative_to_type.get(creative_id, []):
                        if creative_to_houses.get(creative_id) or creative_type in PRO_CREATIVE_TYPES:
                            # we only care about pro-ads or photo ads from brand advertisers
                            house_ids = creative_to_houses.get(creative_id, [])
                            result[creative_id] = (user_id, house_ids)
    logger.debug('creative_to_user:%s', result)
    logger.info('end get_creative_to_user_and_houses_map. creative_len:%s', len(result))
    return result

def get_adgroup_to_user_map(redis_conn):
    '''Get adgroup_id => user_id for all ads (ProPhoto, ProBanner, Photo, Banner, etc).'''
    logger.info('start get_adgroup_to_user_map')
    result = {}
    # get all advertisers
    all_advertisers = [int(i) for i in redis_conn.smembers('lists:advertiser:all')]
    # get advertiser => user
    advertiser_to_user, _all_users = get_ad_id_mapping(
            redis_conn, ['advertiser:%d:userId' % i for i in all_advertisers])
    # get advertiser => campaigns
    advertiser_to_campaigns, all_campaigns = get_ad_id_mapping(
            redis_conn, ['advertiser:%d:campaignIds' % i for i in all_advertisers])
    # get campaign => adGroups
    campaign_to_adgroups, _all_adgroups = get_ad_id_mapping(
            redis_conn, ['campaign:%d:adGroupIds' % i for i in all_campaigns])

    # get adgroup => user
    for advertiser_id, user_ids in six.iteritems(advertiser_to_user):
        user_id = user_ids[0]
        for campaign_id in advertiser_to_campaigns.get(advertiser_id, []):
            for adgroup_id in campaign_to_adgroups.get(campaign_id, []):
                result[adgroup_id] = user_id
    logger.debug('adgroup_to_user:%s', result)
    logger.info('end get_adgroup_to_user_map. adgroup_len:%s', len(result))
    return result

def delete_stats_between_date_range(start, end):
    logger.info('start delete stats on date range [%s, %s]', start, end)
    params = {
        'start': start,
        'end': end,
    }
    mysql_utils.run_update(SQL_DELETE_PRO_STATS, params, db_params=Config.MYSQL_DB_PARAMS)
    logger.info('end delete stats on date range [%s, %s]', start, end)

def get_promoted_photo_impression_cap(redis_conn, user_ids, user_to_advertiser=None):
    logger.info('start get_promoted_photo_impression_cap. num_users: %s',
            len(user_ids))
    logger.debug('user_ids: %s', user_ids)
    user_to_imp_cap = {}
    if user_to_advertiser is None:
        user_to_advertiser = get_user_to_advertiser_mapping(redis_conn)
    advertiser_to_user = dict((v, k) for (k, v) in six.iteritems(user_to_advertiser))
    advertiser_to_campaigns, all_campaigns = get_ad_id_mapping(
            redis_conn,
            ['advertiser:%d:campaignIds' % user_to_advertiser[u]
                for u in user_ids if u in user_to_advertiser])
    campaign_to_names, _all_campaign_names = get_ad_id_mapping(
            redis_conn,
            ['campaign:%d:campaignName' % c for c in all_campaigns],
            str_val_convert=lambda x: x)  # don't try to convert name to integer
    campaign_to_caps, _all_campaign_caps = get_ad_id_mapping(
            redis_conn,
            ['campaign:%d:impressions' % c for c in all_campaigns])
    for advertiser_id, campaign_ids in six.iteritems(advertiser_to_campaigns):
        imp_cap = 0
        for campaign_id in campaign_ids:
            campaign_name = campaign_to_names.get(campaign_id, [''])[0]
            if campaign_name.startswith(PHOTO_CAMPAIGN_NAME_PREFIX):
                # each pro+ account only has one web campaign
                imp_cap = campaign_to_caps.get(campaign_id, [0])[0]
                break
        user_id = advertiser_to_user[advertiser_id]
        user_to_imp_cap[user_id] = imp_cap
    logger.info('end get_promoted_photo_impression_cap. num_users: %s, found_cap: %s',
            len(user_ids), len(user_to_imp_cap))
    return user_to_imp_cap

def get_program_start_for_users(user_ids):
    res = {}
    query = SQL_GET_PRO_PLUS_START_DATE_BY_USER % ','.join(str(u) for u in user_ids)
    for rows in mysql_utils.run_query(query, chunk_size=1000):
        for row in rows:
            user_id, start_day = row
            res[user_id] = start_day
    return res

def get_active_pro_users():
    res = []
    query = SQL_GET_ACTIVE_PRO_USERS
    for rows in mysql_utils.run_query(query, chunk_size=1000):
        for row in rows:
            res.append(row[0])
    return res

def get_active_package_locations_for_users(user_ids):
    res = defaultdict(list)
    query = SQL_GET_PACKAGE_LOCATIONS_BY_USER  % ','.join(str(u) for u in user_ids)
    for rows in mysql_utils.run_query(query, chunk_size=1000):
        for row in rows:
            user_id, loc_name = row
            res[user_id].append(loc_name)
    return res

def get_active_package_numbers_for_users(user_ids):
    res = {}
    query = SQL_GET_PACKAGE_NUMBERS_BY_USER  % ','.join(str(u) for u in user_ids)
    for rows in mysql_utils.run_query(query, chunk_size=1000):
        for row in rows:
            user_id, num_pkgs = row
            res[user_id] = num_pkgs
    return res

def get_user_name_for_users(user_ids):
    res = {}
    query = SQL_GET_USER_NAME_BY_USER % ','.join(str(u) for u in user_ids)
    for rows in mysql_utils.run_query(query, chunk_size=1000):
        for row in rows:
            user_id, user_name = row
            res[user_id] = user_name
    return res

def get_professional_name_for_users(user_ids):
    res = {}
    query = SQL_GET_PROFESSIONAL_NAME_BY_USER % ','.join(str(u) for u in user_ids)
    for rows in mysql_utils.run_query(query, chunk_size=1000):
        for row in rows:
            user_id, prof_name = row
            res[user_id] = prof_name
    return res
