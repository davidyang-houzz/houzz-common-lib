# -*- coding: utf-8 -*-
# @Author: Zviki Cohen
# @Date:   2016-09-20 10:07:47
# @Last Modified by:   Zviki Cohen
# @Last Modified time: 2016-09-20 14:07:54
from __future__ import absolute_import
import logging
import requests

logger = logging.getLogger(__name__)


class PagerDuty(object):

    def __init__(self, service_key, client_name=None, client_url=None):
        self._service_key = service_key
        self._client_name = client_name or 'Houzz Backend'
        self._client_url = client_url or 'https://control.houzz.net'

    _PD_EVENT_ENDPOINT = 'https://events.pagerduty.com/generic/' +\
        '2010-04-15/create_event.json'

    def trigger_incident(self, description, details=None, contexts=None):
        payload = {
            'event_type': 'trigger',
            'description': description,
            'client_name': self._client_name,
            'client_url': self._client_url,
        }
        if details:
            payload['details'] = details
        if details:
            payload['contexts'] = contexts
        return self._invoke_pd(payload)

    def resolve_incident(self, incident_key):
        payload = {
            'event_type': 'resolve',
            'incident_key': incident_key,
        }
        return self._invoke_pd(payload)

    def _invoke_pd(self, payload):
        result = None
        payload['service_key'] = self._service_key
        logger.info('PagerDuty creating event, payload: %s' % (payload, ))
        resp = requests.post(self._PD_EVENT_ENDPOINT, json=payload)
        logger.info('PagerDuty response: %s' % (resp, ))
        if resp.status_code == requests.codes.ok:
            resp_data = resp.json()
            if resp_data["status"] == "success":
                result = resp_data['incident_key']
                logger.info('PagerDuty created event: %s' % (result, ))
        if result:
            return result
        else:
            raise Exception(
                'Failed to call PagerDuty api, resp: %s' % (resp, ))


# if __name__ == '__main__':
#     p = PagerDuty(service_key='...')
#     p.trigger_incident('test', details={'name': 'nothing', })
