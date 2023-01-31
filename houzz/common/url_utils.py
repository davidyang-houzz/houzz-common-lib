#!/usr/bin/env python

''' routines for fetching url data.
'''

from __future__ import absolute_import
import json
import logging
import requests
import subprocess
import time


logger = logging.getLogger(__name__)


def rate_limited(max_per_second):
    ''' decorator taken from:
         http://stackoverflow.com/questions/667508/whats-a-good-rate-limiting-algorithm
    '''
    min_interval = 1.0 / float(max_per_second)
    def decorate(func):
        ''' rate limited decorator '''
        last_time_called = [0.0]
        def rate_limited_function(*args, **kargs):
            ''' wrapper function '''
            elapsed = time.clock() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            ret = func(*args, **kargs)
            last_time_called[0] = time.clock()
            return ret
        return rate_limited_function
    return decorate

def fetch_url(url, get_params=None, post_params=None, post_json=None, headers={}, timeout=120, verify=True, return_type=None):
    ''' fetch the given url.  if params are given, it'll be sent as post.
        return (code, result)
    '''
    code = False
    results = None
    if not url:
        return code, results
    try:
        logger.info('url: %s', url)
        if get_params:
            response = requests.get(url, params=get_params, headers=headers, timeout=timeout,
                                    verify=verify)
        else:
            logger.debug('post params: {}, {}', post_params, post_json)
            response = requests.post(url, params=get_params, data=post_params, json=post_json, headers=headers, timeout=timeout, verify=verify)
        code = response.status_code
        logger.info('response code: %s encoding: %s', code, response.encoding)
        if return_type == 'json':
            results = response.json()
        else:
            results = response.content
    except:
        logger.exception('Failed to fetch type %s for url: %s', return_type, url)
    return code, results


def get_geoip(ip):
    ''' get geoip by calling geoiplookup on command line.
    '''
    def _get_output(lines):
        ''' example output for geoiplookup 114.245.88.170:
            GeoIP Country Edition: CN, China
            GeoIP City Edition, Rev 1: CN, 22, Beijing, N/A, 39.928902, 116.388298, 0, 0
            GeoIP City Edition, Rev 0: CN, 22, Beijing, N/A, 39.928902, 116.388298
        '''
        result = {}
        array = lines.split('\n')
        if len(array) > 0:
            result['country'] = array[0].split(':')[1].strip()
        if len(array) > 1:
            values = array[1].split(':')[1].split(',')
            if len(values) > 4:
                result['city'] = ''.join(values[0:4])
            if len(values) > 6:
                result['lat'] = values[4].strip()
                result['lon'] = values[5].strip()
        return result

    output = ''
    cmd = '/usr/local/bin/geoiplookup'
    try:
        logger.debug('get_geoip cmd: %s ip: %s', cmd, ip)
        p = subprocess.Popen([cmd, ip], stdout=subprocess.PIPE)
        output = _get_output(p.communicate()[0])
    except:
        logger.exception('exception when running cmd: %s', cmd)
    return output
