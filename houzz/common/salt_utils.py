#!/usr/bin/python

from __future__ import absolute_import
import logging
import os
import re
import time
import zipfile

import salt.client

from houzz.common import operation_utils, ssh_utils

logger = logging.getLogger(__name__)


MASTER_SALT_ROOT_DIR = '/srv/salt'
MASTER_SALT_PATH_PREFIX = 'salt://'
MASTER_SALT_CONFIG_FILE = '/etc/salt/master'
MAX_NUMBER_OF_TRIES = 5
SLEEP_SECONDS_BETWEEN_RETRY = 2


class HouzzSaltExecutionException(Exception):
    pass

def salt_check_retcode_callback(ret, _server, _in_param, _out_param):
    """return success"""
    return ret and 'retcode' in ret and not ret['retcode']


def salt_check_result_callback(ret, _server, _in_param, _out_param):
    """return success"""
    if not isinstance(ret, dict):
        return False
    if len(ret) == 1 and isinstance(list(ret.values())[0], dict):
        return list(ret.values())[0].get('result', False)
    return ret.get('result', False)


def salt_get_stdout_callback(ret, _server, _in_param, cbp):
    """return success"""
    if not ret or not 'retcode' in ret or ret['retcode']:
        return False
    else:
        cbp.extend(ret['stdout'].splitlines())
        return True


def get_salt_path_from_os_path(os_path):
    '''convert salt path to os path.
    >>> get_salt_path_from_os_path('/srv/salt/a')
    'salt://a'
    >>> get_salt_path_from_os_path('/srv/salt/a/')
    'salt://a'
    >>> get_salt_path_from_os_path('/srv/salt')
    'salt://'
    >>> get_salt_path_from_os_path('/srv/salt/')
    'salt://'
    >>> get_salt_path_from_os_path('/srv/salt//')
    'salt://'
    '''
    assert os_path.startswith(MASTER_SALT_ROOT_DIR), 'path "%s" need to be under "%s"' % (
            os_path, MASTER_SALT_ROOT_DIR)
    sub_path = re.sub(r'^/+', '', os_path[len(MASTER_SALT_ROOT_DIR):])
    if not sub_path:
        return MASTER_SALT_PATH_PREFIX
    return os.path.join(MASTER_SALT_PATH_PREFIX, os.path.normpath(sub_path))


def get_os_path_from_salt_path(salt_path):
    '''convert os path to salt path.
    >>> get_os_path_from_salt_path('salt://a')
    '/srv/salt/a'
    >>> get_os_path_from_salt_path('salt://a/')
    '/srv/salt/a'
    >>> get_os_path_from_salt_path('salt://')
    '/srv/salt'
    >>> get_os_path_from_salt_path('salt:///')
    '/srv/salt'
    '''
    assert salt_path.startswith(MASTER_SALT_PATH_PREFIX), 'path "%s" need to be under "%s"' % (
            salt_path, MASTER_SALT_PATH_PREFIX)
    sub_path = re.sub(r'^/+', '', salt_path[len(MASTER_SALT_PATH_PREFIX):])
    if not sub_path:
        return MASTER_SALT_ROOT_DIR
    return os.path.join(MASTER_SALT_ROOT_DIR, os.path.normpath(sub_path))


@operation_utils.execute_func_with_retry(max_num_tries=3, sleep_seconds_between_retry=1)
def scp_files_from_minion(server, file_paths, dest_dir, ssh_key_path,
        ssh_user='alon', ssh_port=22, timeout=30):
    '''pull remote files from server (salt minion) to salt master.'''
    logger.info('start scp_files_from_minion:%s. src_files:%s. dest_dir:%s',
            server, file_paths, dest_dir)
    try:
        ssh_client = ssh_utils.create_ssh_client(
                server, ssh_port, ssh_user, ssh_key_path)
        ssh_utils.scp_get(
                ssh_client, file_paths, dest_dir, timeout)
    except:
        logger.exception('scp_files_from_minion crashed')
        raise
    finally:
        if ssh_client:
            ssh_client.close()
    logger.info('end scp_files_from_minion:%s. src_files:%s. dest_dir:%s',
            server, file_paths, dest_dir)


class SaltClient(object):
    def __init__(self, conf_file):
        self.conf_file = conf_file
        self.refresh_salt_client()

    def refresh_salt_client(self):
        if hasattr(self, 'client'):
            del self.client
        self.client = salt.client.LocalClient(self.conf_file)

    def run(self, tgt, fun, arg_list=(), timeout=5, tgt_type='list', kwarg=None):
        params = [tgt, fun, arg_list, timeout, tgt_type, kwarg]

        logger.info('make salt call. arg_list:%s, params:%s', arg_list, params)
        full_ret = self.client.cmd_full_return(tgt, fun, arg_list, timeout,
                                               tgt_type, kwarg=kwarg)
        return full_ret

    def execute_cmd_with_retry(self, server_list, callback,
            callback_input, callback_output, msg,
            max_num_tries, sleep_seconds_between_retry, *params):
        # make a copy of the server_list, since we will change the list in case of
        # failure / retry
        servers = server_list[:]
        num_of_tries = 0
        servers_success = []
        servers_fail = []
        while num_of_tries < max_num_tries:
            num_of_tries += 1
            del servers_success[:]
            del servers_fail[:]
            res = self.run(servers, *params)
            for server in servers:
                ret = {}
                if server in res:
                    ret = res[server]['ret']
                    logger.debug('type of ret: %s, ret: %s', type(ret), ret)
                success = callback(ret, server, callback_input, callback_output)
                error_msg = {}
                if success:
                    servers_success.append(server)
                else:
                    servers_fail.append(server)
                    error_msg[server] = {
                        'stderr': isinstance(ret, dict) and ret.get('stderr') or None,
                        'stdout': isinstance(ret, dict) and ret.get('stdout') or None,
                    }

            if servers_fail:
                logger.error('trial no.%s: servers failed on %s: %s',
                        num_of_tries, msg, servers_fail)
                for server in error_msg:
                    logger.error('stdout from %s: %s', server, error_msg[server]['stdout'])
                    logger.error('stderr from %s: %s', server, error_msg[server]['stderr'])
            if servers_success:
                logger.info('trial no.%s: servers succeed on %s: %s',
                        num_of_tries, msg, servers_success)
            if not servers_fail:
                return
            servers[:] = servers_fail  # just retry on the failed servers
            if sleep_seconds_between_retry:
                time.sleep(sleep_seconds_between_retry)
            self.refresh_salt_client()
        raise HouzzSaltExecutionException(
                '%s: succeeded on %s, failed on %s. params: %s' % (
                    msg, servers_success, servers_fail, params))

    def test_ping(self, pttn, tgt_type='glob'):
        minions = list(self.run(pttn, 'test.ping', [], 1, tgt_type).keys())
        return minions

    def find_alive(self, pttns, tgt_type='glob'):
        servers = []
        for pttn in pttns:
            res = self.test_ping(pttn, tgt_type)
            if res:
                servers.extend(res)
        return sorted(servers)

    def push_files_to_minions(self, servers, file_paths, dest_dir, local_working_dir,
            pkg_file_name, timeout=30):
        '''zip up all the files, salt cp.get_file to the dest_dir on minion servers, then unzip.'''
        # package up all the files
        if not pkg_file_name.lower().endswith('.zip'):
            pkg_file_name = '%s.zip' % pkg_file_name
        pkg_file_path = os.path.join(MASTER_SALT_ROOT_DIR, local_working_dir, pkg_file_name)
        salt_src_path = get_salt_path_from_os_path(pkg_file_path)
        with zipfile.ZipFile(pkg_file_path, 'w', zipfile.ZIP_DEFLATED) as pkg_file:
            for src_path in file_paths:
                pkg_file.write(src_path, os.path.basename(src_path))

        # copy the pkg file over to minions
        dest_path = os.path.join(dest_dir, pkg_file_name)
        data = {
                'fun': 'file.managed',
                'name': dest_path,
                'source': salt_src_path,
                'makedirs': True,
        }
        params = ['state.single', [], timeout, 'list', data]
        self.execute_cmd_with_retry(
                servers, salt_check_result_callback, None, None, 'push-files-to-minion',
                MAX_NUMBER_OF_TRIES, SLEEP_SECONDS_BETWEEN_RETRY, *params)

        # unpackage the pkg file on the minions
        params = ['cmd.run_all',
                ['cd %s && unzip -o %s && rm -f %s' % (dest_dir, pkg_file_name, pkg_file_name)],
                timeout, 'list',]
        self.execute_cmd_with_retry(
                servers, salt_check_retcode_callback, None, None, 'unzip-pkg-file',
                MAX_NUMBER_OF_TRIES, SLEEP_SECONDS_BETWEEN_RETRY, *params)

        # remove the temporary package file
        os.unlink(pkg_file_path)

    def get_file_list_on_minion(self, server, path_glob_pttn, timeout=10):
        params = [
                'cmd.run_all', ['if [ "$(ls -1t %s 2>/dev/null)" ]; then ls -1t %s; fi' % (
                    path_glob_pttn, path_glob_pttn)],
                timeout, 'list',]
        callback_param = []
        self.execute_cmd_with_retry(
                [server], salt_get_stdout_callback, None, callback_param,
                'get-file-list', MAX_NUMBER_OF_TRIES, SLEEP_SECONDS_BETWEEN_RETRY, *params)
        logger.info('get_src_file_list run success. file list on %s: %s',
                server, callback_param)
        return callback_param
