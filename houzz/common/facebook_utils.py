#!/usr/bin/python

'''
Collection of facebook related utility functions.
'''

from __future__ import absolute_import
import copy
import json
import logging
import time

from .config import get_config
from datetime import datetime
from . import mysql_utils
from . import redis_utils
from . import url_utils
import six

logger = logging.getLogger(__name__)
Config = get_config()

MAIN_DB_PARAMS = copy.deepcopy(Config.MYSQL_DB_PARAMS)

SQL_INSERT_SOCIAL_STATS = '''
insert into c2.social_stats (stat_type, object_type, object_id, stat, created, data)
values (%(stat_type)s, %(object_type)s, %(object_id)s, %(stat)s, NOW(), %(data)s)
'''

SQL_GET_SOCIAL_STATS = '''
SELECT object_id, stat
 FROM social_stats
 WHERE stat_type = %(stat_type)s
   AND object_type = %(object_type)s
   AND object_id IN (%(object_ids)s)
   AND created < '%(end_date)s'
 ORDER BY created desc
'''


def fetch_fb_likes(base_url, object_urls):
    ''' fetch fb likes given a base url and dict of ids to corresponding url.
        returns a dictionary of ids -> share numbers.
    '''
    results = {}
    url = ''
    batch_size = 10
    logger.info('base_url: ' + base_url)
    for url_batch in redis_utils.grouper(batch_size, object_urls):
        url_to_id = {}
        for object_id in url_batch:
            if object_id:
                results[object_id] = 0
                url_to_id[object_urls[object_id]] = object_id
        try:
            get_params = {
                'format': 'json',
                'urls': ','.join([object_urls[object_id] for object_id in url_batch if object_id]),
            }
            logger.debug('get_params: %s', get_params)
            (code, result) = url_utils.fetch_url(base_url, get_params, return_type='json')
            if code == 200:
                logger.debug('fb fetch likes result: %s', result)
                for r in result:
                    if 'total_count' in r and 'url' in r and r['url'] in url_to_id:
                        object_id = url_to_id[r['url']]
                        results[object_id] = r['total_count']
        except:
            logger.exception('Failed to fetch fb likes: ' + url_batch)
        finally:
            time.sleep(1)
    return results


def bulk_store_gallery_likes(gallery_likes_map):
    ''' perform bulk store to database of a dictionary of gallery_id to num_facebook_likes.
    '''
    logger.debug('begin bulk store gallery likes')
    social_stats_table_params = []
    for gallery_id, likes in six.iteritems(gallery_likes_map):
        if likes == 0:
            continue
        params = {
            'stat_type': 1,                # C2SocialStat::SOCIAL_STAT_TYPE_FB_LIKE
            'object_type': 1,            # C2EntityTypes::GALLERY
            'object_id': gallery_id,
            'stat': likes,
            'data': None,
        }
        social_stats_table_params.append(params)

    mysql_utils.run_bulk_update(SQL_INSERT_SOCIAL_STATS, social_stats_table_params)
    logger.debug('end bulk store gallery likes')


def bulk_get_gallery_fb_likes(gallery_ids, end_date=datetime.today()):
    ''' perform bulk lookup of list of gallery ids.
        returns result as a dictionary of gallery_id to num_facebook_likes.
        it only handles up to probably around 100k gallery ids at one time.
    '''
    logger.debug('begin bulk lookup gallery likes')
    social_stats = {}
    if len(gallery_ids) == 0:
        return social_stats

    query = SQL_GET_SOCIAL_STATS
    params = {
        'stat_type': 1,   # FB likes
        'object_type': 1, # gallery
        'end_date': end_date.strftime("%Y-%m-%d"),
        'object_ids': ','.join([str(id) for id in gallery_ids])
    }
    query = query % params
    for rows in mysql_utils.run_query(query, chunk_size=1000, **MAIN_DB_PARAMS):
        for row in rows:
            (object_id, stat) = row
            if str(object_id) not in social_stats:
                social_stats[str(object_id)] = stat

    logger.debug('end bulk lookup gallery likes, size: %d', len(social_stats))
    return social_stats

