import requests
import logging
import json

from pyeloqua.bulk import Bulk
from pyeloqua.error_handling import EloquaServerError, EloquaValidationError
from pyeloqua.pyeloqua import POST_HEADERS

log = logging.getLogger(__name__)


class Eloqua(Bulk):

    OPTED_OUT_MAPPING = {
        0: ('Global Unsubscribe', "{{GlobalSubscribe}}", "/contacts/syncActions/6063"),
        2: ('Advertising Promotions', "{{EmailGroup[11]}}", "/contacts/syncActions/5650"),
        4: ('Houzz Events', "{{EmailGroup[10]}}", "/contacts/syncActions/5657"),
        8: ('News & Education', "{{EmailGroup[9]}}", "/contacts/syncActions/5658"),
        16: ('Houzz Professional Research Surveys', "{{EmailGroup[12]}}", "/contacts/syncActions/5659"),
        32: ('Houzz Trade Program', "{{EmailGroup[13]}}", "/contacts/syncActions/5649"),
        64: ('Ivy Marketing & Promotions', "{{EmailGroup[14]}}", "/contacts/syncActions/5648")
    }

    def __init__(self, username=None, password=None, company=None, test=False, redis=None, client_id=None, client_secret=None):
        Bulk.__init__(self, username, password, company, test, redis, client_id, client_secret)

    def _elq_error(self, request):
        """
        Deal with error codes on requests
        Eloqua serves 400 on validation errors; otherwise, general Exception
        is good enough

        :param Request request: object output from Requests
        """

        # deal with validation errors
        if request.status_code == 400:
            try:
                content = request.json()
            except Exception:  # pylint: disable=broad-except
                content = request.text
            raise EloquaValidationError(request.status_code, content)

        if request.status_code >= 500:
            raise EloquaServerError(request.status_code)

        else:
            request.raise_for_status()

    def get_rest(self, url):
        url = self.rest_base + url
        req = self.get(url=url, auth=self.auth)
        self._elq_error(req)
        return req.json()

    def get_bulk(self, url):
        url = self.bulk_base + url
        req = self.get(url=url, auth=self.auth)
        self._elq_error(req)
        return req.json()

    def http_put(self, url, data):
        url = self.rest_base + url
        req = self.put(url=url, data=data, auth=self.auth)
        self._elq_error(req)
        return req.json()

    def http_post(self, url, data=None):
        url = self.bulk_base + url
        req = self.post(url=url, data=data, auth=self.auth)
        self._elq_error(req)
        return req.json()

    def get_activity_fields_group_by_activity_type(self, filters=None):
        res = self.get_bulk("/activities/fields")
        activity_types = {}
        for field in res.get("items"):
            name = field.get("internalName")
            statement = field.get("statement")
            if name and statement:
                for activity_type in field.get("activityTypes"):
                    if activity_type not in activity_types:
                        activity_types[activity_type] = []
                    activity_types[activity_type].append({"name":  name, "statement": statement})
        return activity_types

    def get_email_groups(self):
        return self.get_bulk('emailGroups')

    def opt_out_sync_action(self, opt_out_option, status=True):
        sync_action = {
            "name": "Bulk Sync optout Action",
            "fields": {
                "Email": "{{Contact.Field(C_EmailAddress)}}"
            },
            "identifierFieldName": "Email",
            "syncActions": [
                {
                    "action": "setStatus",
                    "destination": opt_out_option,
                    "status": "unsubscribed" if status else "subscribed"
                }
            ],
            "isSyncTriggeredOnImport": "true"
        }
        url = self.bulk_base + '/contacts/syncActions'

        res = self.post(url=url, auth=self.auth, data=json.dumps(
            sync_action, ensure_ascii=False).encode('utf8'), headers=POST_HEADERS)

        self._elq_error(res)
        return res.json()

    def get_contact_by_email(self, email):
        res = self.get_rest('data/contacts?search=C_EmailAddress=' + email)
        if res.get('total', 0) == 0:
            return {}
        return res.get('elements')[0]

    def get_opt_out_group(self, email, group_id):
        contact = self.get_contact_by_email(email)
        if 'id' in contact:
            res = self.get_rest('data/contact/%s/email/group/%d/subscription' % (contact['id'], group_id))
            return res
        else:
            return None

    def sync_export_data(self, limit=None, offset=0):
        endpoint = self.job_def['uri'] + '/data'
        url_base = self.bulk_base + endpoint + '?limit={limit}&offset={offset}'
        url = url_base.format(limit=limit, offset=offset)
        res = self.get(url=url, auth=self.auth)
        self._elq_error(res)
        return res.json()

    def post_sync_action(self, email, uri):
        url = self.bulk_base + uri + '/data'
        data = [{"Email": email}]
        log.info('post url [%s] with data [%s]', url, data)
        res = self.post(url=url, auth=self.auth, data=json.dumps(
            data, ensure_ascii=False).encode('utf8'), headers=POST_HEADERS)
        self._elq_error(res)
        return res.json()

    def retrieve_sync_status(self, url):
        logs = self.get_bulk('syncs/85810/logs')
        return logs

    def retrieve_sync_actions(self):
        return self.get_bulk('contacts/syncActions')
