#!/usr/bin/python

from __future__ import absolute_import
from __future__ import print_function
from functools import wraps
import glob
import grp
import gzip
import logging
import os
import pwd
import re
import subprocess
import time
import socket
import hashlib
import zipfile
import errno

from . import config
import six

# On local box, make sure you comment out "skip-networking" in
# /usr/local/zend/mysql/data/my.cnf. Otherwise, you can't connect

DEFAULT_MAX_NUM_TRIES = 3
DEFAULT_SLEEP_SECONDS_BETWEEN_RETRY = 1

logger = logging.getLogger(__name__)
Config = config.get_config()

class SystemCallException(Exception):
    pass


class FunctionCallException(Exception):
    pass


def execute_func_with_retry(*params, **kparams):
    """Use this decorator to retry the wrapped function call."""
    dtries = kparams.get('max_num_tries', DEFAULT_MAX_NUM_TRIES)
    dsleep = kparams.get('sleep_seconds_between_retry',
                         DEFAULT_SLEEP_SECONDS_BETWEEN_RETRY)
    dexcb = kparams.get('exception_callback', None)
    dsucb = kparams.get('success_callback', None)

    def actual_decorator(func):
        '''Need extra layer to handle invoking \
                execute_func_with_retry without ().'''

        @wraps(func)
        def with_retry(*pargs, **kwargs):
            if 'max_num_tries' in kwargs:
                max_num_tries = kwargs['max_num_tries']
            else:
                max_num_tries = dtries
            if 'sleep_seconds_between_retry' in kwargs:
                sleep_seconds_between_retry = \
                    kwargs['sleep_seconds_between_retry']
            else:
                sleep_seconds_between_retry = dsleep
            if 'exception_callback' in kwargs:
                exception_callback = kwargs['exception_callback']
            else:
                exception_callback = dexcb
            if 'success_callback' in kwargs:
                success_callback = kwargs['success_callback']
            else:
                success_callback = dsucb
            num_of_tries = 0
            while num_of_tries < max_num_tries:
                num_of_tries += 1
                try:
                    ret = func(*pargs, **kwargs)
                except Exception as e:
                    logger.error('trial no.%s - %s call failed with msg: %s',
                                 num_of_tries, func.__name__, e)
                    if exception_callback:
                        logger.error('calling callback to \
                                     handle the exception')
                        exception_callback(e, *pargs, **kwargs)
                    if num_of_tries < max_num_tries:
                        time.sleep(sleep_seconds_between_retry)
                else:
                    logger.info('trial no.%s - %s call succeed',
                                num_of_tries, func.__name__)
                    if success_callback:
                        logger.info('calling callback after \
                                    success function call')
                        success_callback(ret, *pargs, **kwargs)
                    return ret
            raise FunctionCallException('Too many failed tries (%s) on %s' %
                                        (num_of_tries, func.__name__))
        return with_retry
    if params and callable(params[0]):
        return actual_decorator(params[0])
    return actual_decorator


@execute_func_with_retry
def test_execute_func_with_retry_0():
    return 1/0


@execute_func_with_retry()
def test_execute_func_with_retry_1():
    return 1/0


@execute_func_with_retry(max_num_tries=2, sleep_seconds_between_retry=2)
def test_execute_func_with_retry_2():
    return 1/0


@execute_func_with_retry(sleep_seconds_between_retry=2)
def test_execute_func_with_retry_3():
    return 1/0


def dummy_exception_callback(ex, *pargs, **kwargs):
    print('ex: %s, pargs: %s, kwargs: %s' % (ex, pargs, kwargs))


@execute_func_with_retry(exception_callback=dummy_exception_callback)
def test_execute_func_with_exception_callback():
    return 1/0


def dummy_success_callback(res, *pargs, **kwargs):
    print('res: %s, pargs: %s, kwargs: %s' % (res, pargs, kwargs))


@execute_func_with_retry(success_callback=dummy_success_callback)
def test_execute_func_with_success_callback(x):
    return x/1


@execute_func_with_retry
def make_system_call(cmd, stdout_regex, stderr_regex=r'^$',
                     retcode_expected=0, **_kwargs):
    logger.info('execute local command: %s', cmd)
    p = subprocess.Popen(cmd.split(' '),
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_data, stderr_data = p.communicate()
    logger.debug('stdout: %s', stdout_data)
    logger.debug('stderr: %s', stderr_data)
    stdout_data = ' '.join(stdout_data.split())
    stderr_data = ' '.join(stderr_data.split())
    if p.returncode != retcode_expected:
        logger.error('stderr: %s', stderr_data)
        raise SystemCallException(
            'cmd: %s retcode: %s. expected: %s' % (
                cmd, p.returncode, retcode_expected))
    if not re.match(stdout_regex, stdout_data):
        logger.error('stdout: %s', stdout_data)
        raise SystemCallException(
            'cmd: %s stdout: %s. expected: %s' % (
                cmd, stdout_data, stdout_regex))
    if not re.match(stderr_regex, stderr_data):
        logger.error('stderr: %s', stderr_data)
        raise SystemCallException(
            'cmd: %s stderr: %s. expected: %s' % (
                cmd, stderr_data, stderr_regex))
    logger.info('done local command: %s', cmd)


def create_dir_if_missing(dir_name, mode=0o755, user=None, group=None):
    # chown is only run when the dir is missing. No chown on existing dir.
    if not os.path.exists(dir_name):
        try:
            os.makedirs(dir_name, mode)
            if user:
                uid = pwd.getpwnam(user).pw_uid
            else:
                uid = os.getuid()
            if group:
                gid = grp.getgrnam(group).gr_gid
            else:
                gid = os.getgid()
            os.chown(dir_name, uid, gid)
        except OSError as e:
            # On production os.makedirs may fail due to file already exists even os.path.exists passes
            if e.errno != errno.EEXIST:
                raise

def get_dns_name():
    return socket.gethostname()

def get_fqdn_name():
    return socket.getfqdn()






def touch(file_path, timestamp=None):
    fobj = None
    try:
        fobj = open(file_path, 'a')
        if timestamp is None:
            os.utime(file_path, None)
        else:
            os.utime(file_path, (timestamp, timestamp))
    except Exception as e:
        raise e
    finally:
        if fobj and isinstance(fobj, file):
            fobj.close()


def hasBeenCopied(file):
    copied_file_path = '%s.copied' % file
    if os.path.isfile(copied_file_path):
        return copied_file_path
    return None


def createCopiedFile(file, no_dry_run):
    createDoneFiles([file], no_dry_run, done_ext='.copied')


def hasBeenFlumed(file):
    copied_file_path = '%s.flumed' % file
    if os.path.isfile(copied_file_path):
        return copied_file_path
    return None


def createFlumedFile(file, no_dry_run):
    createDoneFiles([file], no_dry_run, done_ext='.flumed')


def createDoneFiles(files, no_dry_run, done_ext='.done'):
    result = []
    for f in files:
        done_file_path = '%s%s' % (f, done_ext)
        if no_dry_run:
            touch(done_file_path)
        logger.info('%stouch done file: %s', not no_dry_run and 'dry_run: ' or '',
                    done_file_path)
        result.append(done_file_path)
    return result


def get_md5(fp, chunk_bytes=8192):
    with open(fp, 'rb') as fh:
        m = hashlib.md5()
        while True:
            data = fh.read(chunk_bytes)
            if not data:
                break
            m.update(data)
        return m.hexdigest()


def dict_to_sorted_list(key_val, comp=None, key=None, reverse=False):
    return sorted(((k, v) for (k, v) in six.iteritems(key_val)), comp, key, reverse)


def remove_outdated_files(working_dir, file_name_filter, retention_in_day, dry_run):
    logger.info("Going to run remove_outdated_files with working_dir={0}, file_name_filter={1},"
                "threshold_in_day={2}, dry_run={3}".format(working_dir, file_name_filter, retention_in_day, dry_run))
    now = time.time()
    files = glob.glob(os.path.join(working_dir, file_name_filter))
    for f in files:
        file_path = os.path.join(working_dir, f)
        if os.stat(file_path).st_mtime < now - retention_in_day * 86400:
            if os.path.isfile(file_path):
                logger.info("Going to remove file {0}".format(file_path))
                if not dry_run:
                    os.remove(file_path)
                    logger.info("Removed file {0}".format(file_path))


def remove_files(file_list, no_dry_run):
    logger.info('total number of files to remove: %s', len(file_list))
    for f in file_list:
        logger.info('%sremove file: %s', not no_dry_run and 'dry_run: ' or '', f)
        if no_dry_run:
            os.remove(f)


def unzip_file(zipped_filename, out_dir):
    """Perform the unzip action, return list of unzipped filenames
    """
    zipped_file = zipfile.ZipFile(zipped_filename)
    unzipped_files = []
    # Write the file(s) to specified directory
    for _i, name in enumerate(zipped_file.namelist()):
        full_path_name = os.path.join(out_dir, name)
        f = open(full_path_name, 'wb')
        unzipped_files.append(full_path_name)
        f.write(zipped_file.read(name))
        f.flush()
        f.close()
    return unzipped_files


def gzip_file(in_path, out_path='', compress_level=9):
    '''compress file, and return path for the resulted file.'''
    f_in = open(in_path, 'rb')
    if not out_path:
        out_path = '%s.gz' % in_path
    f_out = gzip.open(out_path, 'wb', compresslevel=compress_level)
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    return out_path


def gunzip_file(gzipped_filename, out_dir):
    """ Perform the gunzip action, return uncompressed file
    """
    gunzipped_filename = os.path.join(out_dir, gzipped_filename[:-3])
    with gzip.open(gzipped_filename, 'rb') as infile:
        with open(gunzipped_filename, 'wb') as outfile:
            outfile.writelines(infile)
    return gunzipped_filename
