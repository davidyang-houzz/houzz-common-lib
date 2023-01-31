from __future__ import absolute_import
from __future__ import print_function
import requests
import logging
import socket
import json
import uuid

from houzz.common import config
import six

logger = logging.getLogger(__name__)

Config = config.get_config()


class RQClient(object):

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 8191
    DEFAULT_TTR = 120
    DEFAULT_TUBE = 'batch'

    def __init__(self, host=None, port=None, connect_timeout=None,
                 use_func=None, use_tube=None):
        self._connect_timeout = connect_timeout or socket.getdefaulttimeout()
        # self._host = host or self.DEFAULT_HOST
        # self._port = port or self.DEFAULT_PORT
        if host is not None:
            self._host = host
        else:
            self._host = Config.DAEMON_PARAMS_ASYNC_CLIENT.RQ_HOST \
                if 'DAEMON_PARAMS_ASYNC_CLIENT' in Config else self.DEFAULT_HOST

        if port is not None:
            self._port = port
        else:
            self._port = Config.DAEMON_PARAMS_ASYNC_CLIENT.RQ_PORT \
                if 'DAEMON_PARAMS_ASYNC_CLIENT' in Config else self.DEFAULT_PORT

        self._use_func = use_func
        self._use_tube = use_tube or self.DEFAULT_TUBE

    def put(self, body, func=None, tube=None, delay=0, ttr=None,
            extra_params=None):
        """Put a job into the current tube.
        Returns job id."""
        if not isinstance(body, six.string_types):
            body = json.dumps(body)
        description = extra_params.get('description', None) \
            if extra_params else None
        if description is None:
            description = func or self._use_func
        param = {'description': description,
                 'func': func or self._use_func,
                 'kwargs': {'job': body},
                 'timeout': ttr or self.DEFAULT_TTR}
        if extra_params:
            param.update(extra_params)
        data = json.dumps({
            'job': param,
            'tube': tube or self._use_tube,
            'delay': delay})
        return self._do_enqueue('queue', data)

    def put_job(self, jid, delay=0):
        """Put a job into the current tube.
        Returns job id."""
        data = json.dumps({
            'job_id': jid,
            'delay': delay
        })
        return self._do_enqueue('queue_job', data)

    def _do_enqueue(self, cmd_path, data):
        resp = requests.post(
            'http://%s:%s/%s' % (self._host, self._port, cmd_path, ),
            data,
            timeout=self._connect_timeout)
        new_jid = None
        if resp.status_code == 200:
            new_jid = resp.json()['jid']
            logger.debug(
                'Successfully posted job, data: %s, jid: %s' %
                (data, new_jid, ))
        else:
            logger.error('Failed put job {}'.format(resp.text))
        return new_jid

    def get_job(self, jid):
        data = json.dumps({'jid': jid})
        resp = requests.post(
            'http://{}:{}/query_job'.format(self._host, self._port),
            data,
            timeout=self._connect_timeout)

        if resp.status_code != 200:
            logger.error('Failed to query job {}'.format(resp.text))
            return None
        return resp.json()

    def _post_call(self, url=None, dict=None, json_data=None):
        if url is None:
            return False
        try:
            post_body = json.dumps(dict)
            resp = requests.post(url=url, data=post_body, json=json_data,
                                 timeout=self._connect_timeout)
            if resp.status_code != 200:
                logger.error('Failed to sendout request to : {}, status code is : {}'.format(url,
                                                                                             resp.status_code))
                return False
            return resp.json()
        except Exception:
            logger.exception('Failed to sendout request to : {}'.format(url))
            return False

    def _get_call(self, url):
        if url is None:
            return False
        try:
            resp = requests.get(url=url, timeout=self._connect_timeout)
            if resp.status_code != 200:
                logger.error('Failed to sendout request to : {}, status code is : {}'.format(url,
                                                                                             resp.status_code))
                return False
            return resp.json()
        except Exception:
            logger.exception('Failed to sendout request to : {}'.format(url))
            return False

    def _delete_call(self, url):
        if url is None:
            return False
        try:
            resp = requests.delete(url=url, timeout=self._connect_timeout)
            if resp.status_code != 200:
                logger.error('Failed to sendout request to : {}, status code is : {}'.format(url,
                                                                                             resp.status_code))
                return False
            return resp.json()
        except Exception:
            logger.exception('Failed to sendout request to : {}'.format(url))
            return False

    def get_ready_job_ids(self, queue_name=None):
        if queue_name is None:
            return False
        url = 'http://{}:{}/job_ids/ready/{}.json'.format(self._host, self._port, queue_name)
        result = self._get_call(url)
        job_ids = result['job_ids'] if 'job_ids' in result else []
        return job_ids

    def get_ready_jobs(self, queue_name=None, mutex_info=False):
        if queue_name is None:
            return False
        url = 'http://{}:{}/jobs/ready/{}.json'.format(self._host, self._port, queue_name)
        if mutex_info:
            url += '?mutex=true'
        return self._get_call(url)

    def get_wip_jobs(self, queue_name=None):
        if queue_name is None:
            return False
        url = 'http://{}:{}/jobs/wip/{}.json'.format(self._host, self._port, queue_name)
        return self._get_call(url)

    def get_finished_jobs(self, queue_name=None):
        if queue_name is None:
            return False
        url = 'http://{}:{}/jobs/finished/{}.json'.format(self._host, self._port, queue_name)
        return self._get_call(url)

    def get_failed_jobs(self, queue_name=None):
        if queue_name is None:
            return False
        url = 'http://{}:{}/jobs/failed/{}.json'.format(self._host, self._port,
                                                        queue_name)
        return self._get_call(url)

    def get_delayed_jobs(self, queue_name=None, mutex_info=False):
        if queue_name is None:
            return False
        url = 'http://{}:{}/jobs/delayed/{}.json'.format(self._host, self._port,
                                                         queue_name)
        if mutex_info:
            url += '?mutex=true'
        return self._get_call(url)

    def get_queue_info(self, queue_name=None):
        if queue_name is None:
            return False
        url = 'http://{}:{}/info/{}.json'.format(self._host, self._port, queue_name)
        return self._get_call(url)

    def create_delay_job(self, job_function=None, job_kwargs=None, delay=0, owner=None,
                         log_dir=None,
                         description=None, priority=1, queue_name='default', result_ttl=500,
                         timeout=None):
        job_parameters = {}
        if job_function is not None:
            job_parameters['fun'] = job_function
        if job_kwargs is not None:
            job_parameters['kwargs'] = job_kwargs
        job_parameters['delay'] = delay
        if owner is not None:
            job_parameters['owner'] = owner
        if log_dir is not None:
            job_parameters[log_dir] = log_dir
        if description is not None:
            job_parameters['description'] = description
        job_parameters['priority'] = priority
        job_parameters['queue_name'] = queue_name
        job_parameters['result_ttl'] = result_ttl
        job_parameters['timeout'] = timeout

    def create_cron_job(self, job_function=None, job_kwargs=None, schedule=None, owner=None,
                        log_dir=None,
                        description=None, priority=1, queue_name='default', result_ttl=500,
                        timeout=None,
                        is_enabled=True, allow_cron_job_overlap=True):

        job_parameters = {}
        if job_function is not None:
            job_parameters['fun'] = job_function
        if job_kwargs is not None:
            job_parameters['kwargs'] = job_kwargs
        if schedule is not None:
            job_parameters['schedule'] = schedule
        if owner is not None:
            job_parameters['owner'] = owner
        if log_dir is not None:
            job_parameters[log_dir] = log_dir
        if description is not None:
            job_parameters['description'] = description
        job_parameters['priority'] = priority
        job_parameters['queue_name'] = queue_name
        job_parameters['result_ttl'] = result_ttl
        job_parameters['timeout'] = timeout
        job_parameters['phantom_id'] = str(uuid.uuid4())
        job_parameters['job_id'] = str(uuid.uuid4())
        job_parameters['is_enabled'] = is_enabled
        job_parameters['allow_cron_job_overlap'] = allow_cron_job_overlap
        job_parameters['type'] = 'cron'

        url = 'http://{}:{}/add_cron_job'.format(self._host, self._port)
        return self._post_call(url, job_parameters)

    def requeue_job(self, id=None, auto=False):
        if id is not None:
            url = 'http://{}:{}/job/requeue_job'.format(self._host, self._port)
            dict = {'job_id': id, 'auto': auto}
            return self._post_call(url, dict)
        else:
            return False

    def enqueue_job(self, id=None, queue_name=None):
        if id is not None:
            url = 'http://{}:{}/job/enqueue_job'.format(self._host, self._port)
            dict = {'job_id': id, 'queue_name': queue_name}
            return self._post_call(url, dict)
        else:
            return False

    def cancel_job(self, id=None):
        if id is not None:
            url = 'http://{}:{}/job/cancel_job'.format(self._host, self._port)
            dict = {'job_id': id}
            return self._post_call(url, dict)
        else:
            return False

    def copy_job(self, id, queue_name):
        url = 'http://{}:{}/job/copy_job'.format(self._host, self._port)
        return self._post_call(url, {'jid': id, 'new_queue': queue_name})

    def orphaned_job(self, id=None, worker_name=None, exc_info=None):
        if id is not None:
            url = 'http://{}:{}/orphaned_job'.format(self._host, self._port)
            dict = {'job_id': id, 'worker_name': worker_name, 'exc_info': exc_info}
            return self._post_call(url, dict)
        else:
            return False

    def get_job_info(self, id=None):
        if id is not None:
            url = 'http://{}:{}/job/{}.json'.format(self._host, self._port, id)
            return self._get_call(url)
        else:
            return False

    def get_scheduled_jobs_versions(self):
        url = 'http://{}:{}/scheduled_jobs/versions.json'.format(self._host, self._port)
        return self._get_call(url)

    def get_scheduled_jobs_version(self, version):
        url = 'http://{}:{}/scheduled_jobs/versions/{}.json'.format(self._host, self._port, version)
        return self._get_call(url)

    def get_daemonworkers(self):
        url = 'http://{}:{}/daemonworkers.json'.format(self._host, self._port)
        return self._get_call(url)

    def get_queues(self):
        url = 'http://{}:{}/queues.json'.format(self._host, self._port)
        return self._get_call(url)

    def empty_queue(self, queue_name=None, queue_status='ready'):
        if queue_name is not None:
            url = 'http://{}:{}/queue/empty'.format(self._host, self._port)
            dict = {'queue_name': queue_name, 'queue_status': queue_status}
            return self._post_call(url, dict)
        else:
            return False

    def requeue_all(self, queue_name=None, queue_status='ready'):
        if queue_name is not None:
            print('call requeue all for queue : {}'.format(queue_name))
            url = 'http://{}:{}/queue/requeue_all'.format(self._host, self._port)
            dict = {'queue_name': queue_name, 'queue_status': queue_status}
            return self._post_call(url, dict)
        else:
            return False

    def delete_queue(self, queue_name=None):
        if queue_name is not None:
            url = 'http://{}:{}/queue/delete'.format(self._host, self._port)
            dict = {'queue_name': queue_name}
            return self._post_call(url, dict)
        else:
            return False

    def set_daemon_worker_status(self, hostname=None, queues=None, status=1):
        if hostname is not None and queues is not None:
            url = 'http://{}:{}/put_daemon_worker'.format(self._host, self._port)
            dict = {'hostname': hostname, 'queues': queues, 'daemon_worker_status': status}
            return self._post_call(url, dict)
        else:
            return False

    def get_queue_group(self, queue_name):
        url = 'http://{}:{}/queue_group?queue_name={}'.format(self._host, self._port, queue_name)
        return self._get_call(url).get('queue_group')

    def get_debug_pods(self, user_email):
        url = 'http://{}:{}/debug_pod?user_email={}'.format(self._host, self._port, user_email)
        return self._get_call(url)

    def create_debug_pod(self, queue_group, user_email):
        url = 'http://{}:{}/debug_pod'.format(self._host, self._port)
        return requests.post(url, {'queue_group': queue_group, 'user_email': user_email}).json()

    def delete_debug_pod(self, job_name):
        url = 'http://{}:{}/debug_pod?job_name={}'.format(self._host, self._port, job_name)
        return self._delete_call(url)
