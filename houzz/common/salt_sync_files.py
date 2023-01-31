#!/usr/bin/env python

from __future__ import absolute_import
from datetime import datetime
import glob
import logging
import optparse
import os
import shutil
import socket
import sys
import traceback
import uuid
import zipfile
from subprocess import call

from houzz.common import config
from houzz.common import email_utils
from houzz.common import mysql_utils
from houzz.common import operation_utils
from houzz.common import salt_utils

logger = logging.getLogger(__name__)
CHECKSUM_FILE_EXTENSION = 'checksum'
CURRENT_BRANCH = 'current'
Config = config.get_config()
INSERT_PUSH_RECORD_SQL = '''
insert into c2_myisam.web_cache_push_logs (created, status, filename, checksum, message)
values (%(created)s, %(status)s, %(filename)s, %(checksum)s, %(message)s)
'''


class HouzzSaltSyncFileException(Exception):
    pass


class SaltFileSyncer(object):
    def __init__(self):
        pass

    def generate_local_cache_values(self, timeout=520):
        '''generate local cache values from source server'''
        logger.info('start generate_local_cache_values on %s with timeout %d', self.src_server, timeout)
        '''explicitly set the timeout to be 5 minutes'''
        ''' A potential issue: if createLocalCache cannot finish within the timeout, sync process will retry.
            Since we don't allow 2 createLocalCache processes running in parallel, it will return immediately as success (return code to be 0).
            Sync process will move on without crashing, while the creation process is still going. In this case, syncer
            will fetch a partial checksum list, and compare it with local state. As a result, some files will not be pushed.
        '''
        params = ['cmd.run_all',
                  ['cd %s && %s -f %s >> /home/clipu/c2/log/localCacheSync_%s.log && %s -f %s --arg "%s" >> /home/clipu/c2/log/localCacheSync_%s.log 2>&1' % (
                      self.run_local_cache_creation_folder, self.execution_engine, self.local_cache_creation_script, self.branch, self.execution_engine, self.local_cache_creation_script, "-XK", self.branch)],
                  timeout, 'list', ]
        callback_param = []
        self.salt_client.execute_cmd_with_retry(
            [self.src_server], salt_utils.salt_get_stdout_callback, None, callback_param,
            'generate_local_cache_values', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)
        logger.info('End generate_local_cache_values on %s', self.src_server)

    def generate_custom_domain_list(self, timeout=120):
        '''generate custom domain list from source server'''
        logger.info('start generate_custom_domain_list on %s with timeout %d', self.src_server, timeout)
        '''explicitly set the timeout to be 2 minutes'''
        '''if domainlist folder hasn't been created under c2, create one first'''
        params_mkdir = ['cmd.run_all',
                        ['if ! test -d %s;then mkdir %s; fi' % (self.src_dir, self.src_dir)],
                        timeout, 'list', ]
        callback_param = []
        self.salt_client.execute_cmd_with_retry(
            [self.src_server], salt_utils.salt_get_stdout_callback, None, callback_param,
            'generate-custom-domain-list', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params_mkdir)
        params = ['cmd.run_all',
                  ['cd %s && %s %s >> /home/clipu/c2/log/domainListSync.log 2>&1' % (
                      self.run_domain_list_creation_folder, self.execution_engine, self.domain_list_creation_script)],
                  timeout, 'list', ]
        callback_param = []
        self.salt_client.execute_cmd_with_retry(
            [self.src_server], salt_utils.salt_get_stdout_callback, None, callback_param,
            'generate-custom-domain-list', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)
        logger.info('End generate-custom-domain-list on %s', self.src_server);

    def get_remote_checksums(self, timeout=10):
        '''generate file_name => checksum map.'''
        result = {}
        logger.info('start get_remote_checksums on %s', self.src_server)
        # get src files to deploy
        fp_list = self.salt_client.get_file_list_on_minion(
            self.src_server, os.path.join(self.src_dir, self.checksum_glob_pttn))
        fn_list = (os.path.splitext(os.path.basename(i))[0] for i in fp_list if
                   i.endswith(CHECKSUM_FILE_EXTENSION))

        for fn in fn_list:
            src_path = os.path.join(self.src_dir, '%s.%s' % (fn, CHECKSUM_FILE_EXTENSION))
            params = [
                'cmd.run_all', ['cat %s' % src_path],
                timeout, 'list', ]
            callback_param = []
            self.salt_client.execute_cmd_with_retry(
                [self.src_server], salt_utils.salt_get_stdout_callback, None, callback_param,
                'get-remote-checksum', salt_utils.MAX_NUMBER_OF_TRIES,
                salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)
            assert len(callback_param) == 1, 'Unknown response on checksum: %s' % callback_param
            result[fn] = callback_param[0].strip()
        logger.debug('remote checksums: %s', operation_utils.dict_to_sorted_list(result))
        logger.info('end get_remote_checksums on %s', self.src_server)
        return result

    def get_local_checksums(self):
        '''get checksums from last successful run.'''
        result = {}
        logger.info('start get_local_checksums')
        for checksum_fp in glob.glob('%s/%s' % (self.local_live_dir, self.checksum_glob_pttn)):
            fn = os.path.splitext(os.path.basename(checksum_fp))[0]
            with open(checksum_fp, 'r') as fi:
                result[fn] = fi.read().strip()
        logger.info('end get_local_checksums')
        return result

    def deploy_files_through_s3(self, servers, kind='localcache', timeout=30, env='prod'):

        logger.info("Deploy Files Through S3 Environment")
        logger.info(env)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        if kind == 'localcache':
            pkg_name = 'localcache_{}.zip'.format(timestamp)
        elif kind == 'domainlist':
            pkg_name = 'domainlist_{}.zip'.format(timestamp)
        elif kind == 'domainlistv2':
            pkg_name = 'domainlistv2_{}.zip'.format(timestamp)
        else:
            logger.error('kind: {} is not supported.'.format(kind))
            return

        logger.info("Uploading Package")
        # 1. src_server: zip cache folder and upload to s3
        upload_pkg_cmd = 'python %s -c upload_pkg -b %s -k %s -i %s -e %s' % \
                         (self.updater_script, self.bucket_name, pkg_name, kind, env)
        params = ['cmd.run_all',
                  ['cd %s && %s' % (self.run_updater_folder, upload_pkg_cmd)],
                  timeout, 'list', ]
        callback_param = []
        # upload to cache/
        self.salt_client.execute_cmd_with_retry(
            [self.src_server], salt_utils.salt_get_stdout_callback, None, callback_param,
            'zip-cache-dir-and-upload-to-s3', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)

        logger.info("Downloading Package")
        # 2. minions: download from s3
        download_pkg_cmd = 'python %s -c download_pkg -b %s -k %s -i %s -e %s' % (
            self.updater_script, self.bucket_name, pkg_name, kind, env)
        params = ['cmd.run_all',
                  ['cd %s && %s' % (self.run_updater_folder, download_pkg_cmd)],
                  timeout, 'list', ]
        callback_param = []
        self.salt_client.execute_cmd_with_retry(
            servers, salt_utils.salt_get_stdout_callback, None, callback_param,
            'download-pkg-from-s3', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)

        logger.info("Unzip Package")
        # 3. minions: unzip pkg
        unzip_pkg_cmd = 'python %s -c unzip_pkg -p %s -i %s -e %s' % (self.updater_script,
                                                                pkg_name, kind, env)
        params = ['cmd.run_all',
                  ['cd %s && %s' % (self.run_updater_folder, unzip_pkg_cmd)],
                  timeout, 'list', ]
        callback_param = []
        self.salt_client.execute_cmd_with_retry(
            servers, salt_utils.salt_get_stdout_callback, None, callback_param,
            'unzip-pkg', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)

        logger.info("Verify Checksum")
        # 4. minions: verify checksum
        verify_cksum_cmd = 'python %s -c verify_checksum -p %s -i %s -e %s' % (self.updater_script,
                                                                         pkg_name, kind, env)
        params = ['cmd.run_all',
                  ['cd %s && %s' % (self.run_updater_folder, verify_cksum_cmd)],
                  timeout, 'list', ]
        callback_param = []
        self.salt_client.execute_cmd_with_retry(
            servers, salt_utils.salt_get_stdout_callback, None, callback_param,
            'verify-checksum', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)

        logger.info("Backup Files")
        # 5. minions: backup existing files
        backup_cmd = 'python %s -c backup_files -i %s -e %s' % (self.updater_script, kind, env)
        params = ['cmd.run_all',
                  ['cd %s && %s' % (self.run_updater_folder, backup_cmd)],
                  timeout, 'list', ]
        callback_param = []
        self.salt_client.execute_cmd_with_retry(
            servers, salt_utils.salt_get_stdout_callback, None, callback_param,
            'backup-local-cache-files', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)

        logger.info("Deploy Files")
        # 6. minions: deploy files
        deploy_files_cmd = 'python %s -c deploy_files -p %s -i %s -e %s' % (self.updater_script, pkg_name, kind, env)
        params = ['cmd.run_all',
                  ['cd %s && %s' % (self.run_updater_folder, deploy_files_cmd)],
                  timeout, 'list', ]
        callback_param = []
        try:
            self.salt_client.execute_cmd_with_retry(
                servers, salt_utils.salt_get_stdout_callback, None, callback_param,
                'deploy-local-cache-files', salt_utils.MAX_NUMBER_OF_TRIES,
                salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)

        except Exception as e1:
            e1_msg = ('FAILED to update files on dest servers. '
                      'Try to restore from backup. Exception: %s' % e1)
            logger.exception(e1_msg)
            logger.info("Rolling back...")
            restore_from_backup_cmd = 'python %s -c rollback -i %s -e %s' % (self.updater_script, kind, env)
            params = ['cmd.run_all',
                      ['cd %s && %s' % (self.run_updater_folder, restore_from_backup_cmd)],
                      timeout, 'list', ]
            callback_param = []
            try:
                # 7. minions (optional): rollback from backup location
                self.salt_client.execute_cmd_with_retry(
                    servers, salt_utils.salt_get_stdout_callback, None, callback_param,
                    'restore-from-backup-location', salt_utils.MAX_NUMBER_OF_TRIES,
                    salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)
            except Exception as e2:
                e2_msg = ('FAILED to restore from backup. '
                          'Send alert email. Exception: %s' % e2)
                logger.exception(e2_msg)
                # 8. failed to restore files from backup: send alert email
                subject = 'Corrupted files on remote servers. Need fix ASAP!'
                body = 'Go to %s and restore from backup dir: %s\ne1_msg:%s\ne2_msg:%s' % (
                    servers, self.remote_backup_dir, e1_msg, e2_msg)
                email_utils.send_text_email(Config.DAEMON_PARAMS.ALERT_EMAIL_FROM,
                                            Config.SALT_FILE_SYNC_PARAMS_LOCAL_CACHE.ALERT_EMAIL_TO,
                                            subject, body, Config.DAEMON_PARAMS.MAIL_PASSWORD)
                raise
            else:
                raise
        finally:
            # 9. minions: do clean up jobs
            logger.info("Clean up")
            cleanup_cmd = 'python %s -c cleanup -p %s -i %s -e %s' % (
                self.updater_script, pkg_name, kind, env)
            params = ['cmd.run_all',
                      ['cd %s && %s' % (self.run_updater_folder, cleanup_cmd)],
                      timeout, 'list', ]
            callback_param = []
            self.salt_client.execute_cmd_with_retry(
                servers, salt_utils.salt_get_stdout_callback, None, callback_param,
                'cleanup', salt_utils.MAX_NUMBER_OF_TRIES,
                salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)

    def deploy_files(self, fn_to_deploy, servers, timeout=60):
        '''deploy file to servers.'''
        if os.path.exists(self.local_working_dir):
            shutil.rmtree(self.local_working_dir)
        os.makedirs(self.local_working_dir)
        os.chmod(self.local_working_dir, 0o777)

        # 3A. scp new files from src server
        salt_utils.scp_files_from_minion(
            self.src_server,
            [os.path.join(self.src_dir, '%s%s' % (fn, ext))
             for fn in fn_to_deploy for ext in ('', '.' + CHECKSUM_FILE_EXTENSION)],
            self.local_working_dir, self.private_key_path)

        # 3B. verify calculated checksum match with the content in checksum file
        for fn in fn_to_deploy:
            fp = os.path.join(self.local_working_dir, fn)
            checksum_fp = '%s.%s' % (fp, CHECKSUM_FILE_EXTENSION)
            with open(checksum_fp, 'r') as fi:
                checksum = fi.read().strip()
            calculated = operation_utils.get_md5(fp)
            assert calculated == checksum, 'md5 not matching. calculated:%s, checksum file:%s' % (
                calculated, checksum)
            logger.info('calculated checksum matches. file:%s, calculated:%s. checksum file:%s',
                        fp, calculated, checksum)

        # 3C. scp new files over to tmp location on dest servers
        self.salt_client.push_files_to_minions(
            servers,
            [os.path.join(self.local_working_dir, '%s%s' % (fn, ext))
             for fn in fn_to_deploy for ext in ('', '.' + CHECKSUM_FILE_EXTENSION)],
            self.remote_working_dir, self.local_working_dir, self.pkg_file_name)

        # 3D. cp existing files to backup location on dest servers
        params = [
            'cmd.run_all',
            ['mkdir -p %s && mkdir -p %s && mkdir -p %s && rm -rf %s/* && '
             '(if [ "$(ls -A %s/%s 2>/dev/null)" ]; then find %s/ -maxdepth 1 -name "%s" -type f -exec cp -af {} %s/ \; ; fi)' % (
                 self.dest_dir, self.remote_working_dir, self.remote_backup_dir, self.remote_backup_dir,
                 self.dest_dir, self.glob_pttn, self.dest_dir, self.glob_pttn, self.remote_backup_dir)],
            timeout, 'list', ]
        self.salt_client.execute_cmd_with_retry(
            servers, salt_utils.salt_check_retcode_callback, None, None,
            'backup-existing-files', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)

        # 3E. mv new files to dir on dest servers
        params = [
            'cmd.run_all',
            ['mkdir -p %s && if [ "$(ls -A %s 2>/dev/null)" ]; then mv -f %s/* %s/; fi' % (
                self.dest_dir, self.remote_working_dir, self.remote_working_dir, self.dest_dir)],
            timeout, 'list', ]
        try:
            self.salt_client.execute_cmd_with_retry(
                servers, salt_utils.salt_check_retcode_callback, None, None,
                'update-files', salt_utils.MAX_NUMBER_OF_TRIES,
                salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)
        except Exception as e1:
            e1_msg = ('FAILED to update files on dest servers. '
                      'Try to restore from backup. Exception: %s' % e1)
            logger.exception(e1_msg)
            # 3F. failed to update files: restore from backup'ed version
            params = [
                'cmd.run_all',
                ['mkdir -p %s && if [ "$(ls -A %s 2>/dev/null)" ]; then mv -f %s/%s %s/; fi' % (
                    self.dest_dir, self.remote_backup_dir, self.remote_backup_dir,
                    self.glob_pttn, self.dest_dir)],
                timeout, 'list', ]
            try:
                self.salt_client.execute_cmd_with_retry(
                    servers, salt_utils.salt_check_retcode_callback, None, None,
                    'restore-from-backup', salt_utils.MAX_NUMBER_OF_TRIES,
                    salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)
            except Exception as e2:
                e2_msg = ('FAILED to restore from backup. '
                          'Send alert email. Exception: %s' % e2)
                logger.exception(e2_msg)
                # 3G. failed to restore files from backup: send alert email
                subject = 'Corrupted files on remote servers. Need fix ASAP!'
                body = 'Go to %s and restore from backup dir: %s\ne1_msg:%s\ne2_msg:%s' % (
                    servers, self.remote_backup_dir, e1_msg, e2_msg)
                email_utils.send_text_email(Config.DAEMON_PARAMS.ALERT_EMAIL_FROM,
                                            Config.SALT_FILE_SYNC_PARAMS_LOCAL_CACHE.ALERT_EMAIL_TO,
                                            subject, body, Config.DAEMON_PARAMS.MAIL_PASSWORD)
                raise
            else:
                raise

    def update_local_checksums(self, use_s3=False, timeout=30, env='prod'):
        logger.info('start update_local_checksums')
        sql_params = []
        created = datetime.now()
        if not use_s3:
            for fn in os.listdir(self.local_working_dir):
                fp = os.path.join(self.local_working_dir, fn)
                if os.path.isfile(fp):
                    if fn.endswith(CHECKSUM_FILE_EXTENSION):
                        with open(fp, 'r') as fi:
                            checksum = fi.read().strip()
                        r_params = {
                            'created': created,
                            'status': 0,
                            'filename': os.path.splitext(fn)[0] if self.branch == CURRENT_BRANCH else
                            os.path.splitext(fn)[0] + '_' + self.branch,
                            'checksum': checksum,
                            'message': '',
                        }
                        # record the push in MySQL
                        mysql_utils.run_update(INSERT_PUSH_RECORD_SQL, r_params)
                        sql_params.append(r_params)
                    # move to current version of cache dir under salt
                    shutil.move(fp, os.path.join(self.local_live_dir, fn))

            # store all cache files when this successful push happened as a version in backup dir
            version = created.strftime('%Y%m%d%H%M%S')
            for fn in os.listdir(self.local_live_dir):
                src_path = os.path.join(self.local_live_dir, fn)
                if os.path.isfile(src_path):
                    backup_fn = '%s.%s' % (fn, version)
                    dest_path = os.path.join(self.local_archive_dir, backup_fn)
                    shutil.copy2(src_path, dest_path)
                    logger.info('backup new version of cache files. src:%s. dest:%s',
                                src_path, dest_path)
        else:
            # we update local checksum from src_server by comparing local cache dir and backup dir
            update_local_checksums_cmd = 'python %s -c update_local_checksums -d %s -e %s' % (
                self.updater_script, self.branch, env)
            params = ['cmd.run_all',
                      ['cd %s && %s' % (self.run_updater_folder, update_local_checksums_cmd)],
                      timeout, 'list', ]
            callback_param = []
            # update_local_checksums_server cannot src_server because src_server always run createLocalCacheValues at very beginning
            # and backup cache files later. In this case, no diff will be found between /c2/cache and /c2/cache_backup
            self.salt_client.execute_cmd_with_retry(
                [self.update_local_checksums_server], salt_utils.salt_get_stdout_callback, None, callback_param,
                'update-local-checksums', salt_utils.MAX_NUMBER_OF_TRIES,
                salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)
        logger.info('end update_local_checksums')

    def bust_apc_cache(self, timeout=60):
        logger.info('start to bust-apc-cache-on-minions, with apc_test_mode: %s', self.apc_test_mode)
        params = ['cmd.run_all',
                  ['cd %s && %s' % (self.apc_working_dir, 'python update_apc_cache.pex -c apc_clear')],
                  timeout, 'list', ]
        params_for_jukwaa = ['cmd.run_all',
                             [
                                 'for port in 8095 8096; do curl http://127.0.0.1:$port/internalToolsEntry.php?c=apc_clear; done;'],
                             timeout, 'list', ]
        callback_param = []
        servers = []
        jukwaa_servers = []
        if self.apc_test_mode:
            servers.append('hweb99.houzz.com')
        else:
            for server in self.dest_servers:
                if server.strip().lower().startswith('jukwaa'):
                    jukwaa_servers.append(server)
                else:
                    servers.append(server)
        self.salt_client.execute_cmd_with_retry(
            servers, salt_utils.salt_get_stdout_callback, None, callback_param,
            'bust-apc-cache-on-minions', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)
        if len(jukwaa_servers) >= 1:
            logger.info('Bust APC cache for jukwaa servers')
            self.salt_client.execute_cmd_with_retry(
                jukwaa_servers, salt_utils.salt_get_stdout_callback, None, callback_param,
                'bust-apc-cache-on-minions', salt_utils.MAX_NUMBER_OF_TRIES,
                salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params_for_jukwaa)
        logger.info('End bust-apc-cache-on-minions')

    def delete_translation_cache(self, timeout=60):
        logger.info('start to delete-translation-cache-on-minions, with apc_test_mode: %s', self.apc_test_mode)
        params = ['cmd.run_all',
                  ['cd %s && %s' % (self.apc_working_dir, 'python update_apc_cache.pex -c apc_translation_delete')],
                  timeout, 'list', ]
        params_for_jukwaa = ['cmd.run_all',
                             [
                                 'for port in 8095 8096; do curl http://127.0.0.1:$port/internalToolsEntry.php?c=apc_translation_delete; done;'],
                             timeout, 'list', ]
        callback_param = []
        servers = []
        jukwaa_servers = []
        if self.apc_test_mode:
            servers.append('web-server-05f42f5c293b650d2.web-production.houzz.net')
        else:
            for server in self.dest_servers:
                if server.strip().lower().startswith('jukwaa'):
                    jukwaa_servers.append(server)
                else:
                    servers.append(server)
        self.salt_client.execute_cmd_with_retry(
            servers, salt_utils.salt_get_stdout_callback, None, callback_param,
            'delete-translation-cache-on-minions', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)
        if len(jukwaa_servers) >= 1:
            logger.info('Delete translation cache for jukwaa servers')
            self.salt_client.execute_cmd_with_retry(
                jukwaa_servers, salt_utils.salt_get_stdout_callback, None, callback_param,
                'delete-translation-cache-on-minions', salt_utils.MAX_NUMBER_OF_TRIES,
                salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params_for_jukwaa)
        logger.info('End delete-translation-cache-on-minions')

    def record_to_redis(self, servers, timeout=30, env='prod'):
        logger.info('start record_to_redis')
        record_to_redis_cmd = 'python %s -c report_to_redis -e %s' % (self.updater_script, env)
        params = ['cmd.run_all',
                  ['cd %s && %s' % (self.run_updater_folder, record_to_redis_cmd)],
                  timeout, 'list', ]
        callback_param = []
        self.salt_client.execute_cmd_with_retry(
            servers, salt_utils.salt_get_stdout_callback, None, callback_param,
            'record_to_redis', salt_utils.MAX_NUMBER_OF_TRIES,
            salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)
        logger.info('end record_to_redis')

    def sync_files(self, sync_mode=False, use_s3=False, env='prod'):
        '''Define the procedures of sync_file, will be overwritten by super class'''
        pass

    def process(self, sync_mode=False, use_s3=False, env='prod'):
        try:
            self.sync_files(sync_mode, use_s3, env)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            message = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            # also log the crash in mySQL
            row_params = {
                'created': datetime.now(),
                'status': 1,
                'filename': '',
                'checksum': '',
                'message': message,
            }
            new_message = ''
            try:
                mysql_utils.run_update(INSERT_PUSH_RECORD_SQL, row_params)
            except:
                logger.exception('Can NOT record the failure in DB')
                exc_type, exc_value, exc_traceback = sys.exc_info()
                new_message = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            finally:
                email_body = 'Initial Error: %s\n' % message
                if new_message:
                    email_body += 'Last Error: %s\n' % new_message
                subject = '%s crashed on %s' % (__file__, socket.gethostname())
                email_utils.send_text_email(Config.DAEMON_PARAMS.ALERT_EMAIL_FROM,
                                            Config.SALT_FILE_SYNC_PARAMS_LOCAL_CACHE.ALERT_EMAIL_TO,
                                            subject, email_body, Config.DAEMON_PARAMS.MAIL_PASSWORD)
            raise HouzzSaltSyncFileException('%s\n%s' % (message, new_message))
