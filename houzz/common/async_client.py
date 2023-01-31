#!/usr/bin/env python

from __future__ import absolute_import
__author__ = 'jesse'

import requests

import logging
import socket
import json

from houzz.common import config

DEFAULT_RQ_HOST = 'localhost'
DEFAULT_RQ_PORT = 8191
DEFAULT_TTR = 120
DEFAULT_PRIORITY = 2 ** 31

Config = config.get_config()

class AsyncClient(object):
    def __init__(self, host=None, port=None,connect_timeout=socket.getdefaulttimeout()):
        self._connect_timeout = connect_timeout
        if host is not None:
            self.host = host
        else:
            self.host = Config.DAEMON_PARAMS_ASYNC_CLIENT.RQ_HOST \
                    if 'DAEMON_PARAMS_ASYNC_CLIENT' in Config else DEFAULT_RQ_HOST
        if port is not None:
            self.port = port
        else:
            self.port = Config.DAEMON_PARAMS_ASYNC_CLIENT.RQ_PORT \
                    if 'DAEMON_PARAMS_ASYNC_CLIENT' in Config else DEFAULT_RQ_PORT
        self.func = None
        self.tube = 'batch'
        self.connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        pass

    def close(self):
        pass

    def using(self, func, tube=None):
        self.func = func
        self.tube = tube or self.tube

    def put(self, body,
            priority=DEFAULT_PRIORITY, delay=0,
            ttr=DEFAULT_TTR, extra_params=None, logfile=None):
        """Put a job into the current tube.
        Returns job id."""
        assert isinstance(body, str), \
            'Job body must be a str instance'

        param = {'description': self.func,
                 'func': self.func,
                 'kwargs': {'job': body},
                 'timeout': ttr}
        if logfile:
            param['kwargs']['log'] = logfile
        if extra_params:
            param.update(extra_params)
        data = json.dumps({
            'job': param,
            'tube': self.tube,
            'delay': delay})
        resp = requests.post(
            'http://{}:{}/queue'.format(self.host, self.port),
            data,
            timeout=self._connect_timeout)

        jid = None
        if resp.status_code == 200:
            jid = resp.json()['jid']
        else:
            logging.error('Failed put job {}'.format(resp.text))
        return jid

    def handle_orphaned_job(self, jid, worker_name):
        """
        Call endpoint to handle orphaned job. Returns job id.
        """

        data = json.dumps({
            'job_id': jid,
            'worker_name': worker_name
        })
        resp = requests.post(
            'http://{}:{}/orphaned_job'.format(self.host, self.port),
            data,
            timeout=self._connect_timeout)

        if resp.status_code != 200:
            logging.error('Failed handle timeout job {}'.format(resp.text))
        return jid

    def put_job(self, jid, delay=0):
        """Put a job into the current tube.
        Returns job id."""
        data = json.dumps({
            'job_id': jid,
            'delay': delay
        })
        resp = requests.post(
            'http://{}:{}/queue_job'.format(self.host, self.port),
            data,
            timeout=self._connect_timeout)

        if resp.status_code != 200:
            logging.error('Failed put job {}'.format(resp.text))
        return jid

    def get_job(self, jid):
        data = json.dumps({'jid': jid})
        resp = requests.post(
            'http://{}:{}/query_job'.format(self.host, self.port),
            data,
            timeout=self._connect_timeout)

        if resp.status_code != 200:
            logging.error('Failed to query job {}'.format(resp.text))
            return None
        return resp.json()
