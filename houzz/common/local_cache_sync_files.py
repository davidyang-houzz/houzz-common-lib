#!/usr/bin/env python
'''Tool on salt master to sync files between remote servers.

General process:
    0.  Check if the script is running in synchronized mode. If so, run remote cmd
        to generate local cache values from src server.
    1.  get new checksums from src server
    2.  check new checksums against local 'last successful run' copy
    3.  if checksums different: Push to all dest servers
        3A. scp new files from src server
        3B. verify calculated checksum match with the content in checksum file
        3C. scp files over to working location on dest servers
        3D. cp existing files to backup location on dest servers
        3E. mv updated files to dest dir on dest servers
        3F. if failed to update files: restore from backup'ed version
        3G. if failed to restore files from backup: send alert email
    4.  If translation files (file name starts with TranslationsCache) get changed, bust APC cache.
        This logic should be moved out of generic salt_sync_file class since it is unique for local cache.
    5.  update the local 'last successful run' checksums, store all files under this
        version to archive dir with version number, and record the push in DB
'''

from __future__ import absolute_import
import atexit
import logging
import os
import signal
import socket
import sys
import uuid
from datetime import datetime

from houzz.common import (
    config,
    email_utils,
    operation_utils,
    process_utils,
    salt_sync_files,
    salt_utils
)
from houzz.common.logger import slogger
import six

CHECKSUM_FILE_EXTENSION = 'checksum'
TRANSLATION_FILE_PREFIX = 'TranslationsCache'
logger = logging.getLogger(__name__)
Config = config.get_config()


class LocalCacheFileSyncer(salt_sync_files.SaltFileSyncer):
    def __init__(self,
                 src_server_pttn,
                 dest_servers_pttns,
                 dest_servers_pttns_type,
                 update_local_checksums_servers_pttns,
                 src_dir,
                 dest_dir,
                 glob_pttn,
                 local_live_dir,
                 local_archive_dir,
                 local_working_dir,
                 remote_working_dir,
                 remote_backup_dir,
                 salt_conf_file,
                 private_key_path,
                 min_num_dest_servers,
                 run_local_cache_creation_folder,
                 run_local_cache_updater_folder,
                 execution_engine,
                 local_cache_creation_script,
                 local_cache_updater_script,
                 bucket_name,
                 apc_working_dir,
                 pkg_file_name=None,
                 force_refresh=False,
                 branch='current',
                 apc_test_mode=True):
        # some sanity check on parameters
        for dir_path in (local_live_dir, local_working_dir):
            assert dir_path.startswith(salt_utils.MASTER_SALT_ROOT_DIR)
        assert os.path.basename(glob_pttn) == glob_pttn, 'glob_pttn should only contain basename'
        assert not os.path.splitext(glob_pttn)[-1] == '.%s' % CHECKSUM_FILE_EXTENSION, (
                'glob_pttn should not end with %s' % CHECKSUM_FILE_EXTENSION)
        self.salt_client = salt_utils.SaltClient(salt_conf_file)
        src_servers = self.salt_client.find_alive([src_server_pttn])
        if len(src_servers) != 1:
            msg = '"%s" should only match 1 src server, but found %s' % (
                src_server_pttn, len(src_servers))
            subject = '%s crashed on %s' % (__file__, socket.gethostname())
            email_utils.send_text_email(Config.DAEMON_PARAMS.ALERT_EMAIL_FROM,
                                        Config.SALT_FILE_SYNC_PARAMS_LOCAL_CACHE.ALERT_EMAIL_TO,
                                        subject, msg, Config.DAEMON_PARAMS.MAIL_PASSWORD)
            raise Exception(msg)
        self.src_server = src_servers[0]
        dest_servers = self.salt_client.find_alive(dest_servers_pttns, dest_servers_pttns_type)
        assert len(dest_servers) >= min_num_dest_servers, (
                '"%s" of type "%s" should match at least %s servers, but found %s' % (
            dest_servers_pttns, dest_servers_pttns_type, min_num_dest_servers, len(dest_servers)))
        self.dest_servers = dest_servers
        # use update_local_checksums_servers_pttns if specified, otherwise use src_server
        update_local_checksums_servers = []
        if update_local_checksums_servers_pttns:
            update_local_checksums_servers = self.salt_client.find_alive([update_local_checksums_servers_pttns])
        if len(update_local_checksums_servers) == 0:
            update_local_checksums_servers = src_servers
        if len(update_local_checksums_servers) == 0:
            msg = '"%s" should match at least 1 src server, but found %s' % (
                update_local_checksums_servers_pttns, len(update_local_checksums_servers))
            subject = '%s crashed on %s' % (__file__, socket.gethostname())
            email_utils.send_text_email(Config.DAEMON_PARAMS.ALERT_EMAIL_FROM,
                                        Config.SALT_FILE_SYNC_PARAMS_LOCAL_CACHE.ALERT_EMAIL_TO,
                                        subject, msg, Config.DAEMON_PARAMS.MAIL_PASSWORD)
            raise Exception(msg)
        self.update_local_checksums_server = update_local_checksums_servers[0]
        self.src_dir = src_dir
        self.dest_dir = dest_dir
        self.glob_pttn = glob_pttn
        # file has to have checksum counterpart
        self.checksum_glob_pttn = '%s.%s' % ( glob_pttn, CHECKSUM_FILE_EXTENSION)
        self.local_live_dir = local_live_dir
        self.local_archive_dir = local_archive_dir
        self.local_working_dir = local_working_dir
        self.remote_working_dir = remote_working_dir
        self.remote_backup_dir = remote_backup_dir
        self.run_local_cache_creation_folder = run_local_cache_creation_folder
        self.run_updater_folder = run_local_cache_updater_folder
        self.execution_engine = execution_engine
        self.local_cache_creation_script = local_cache_creation_script
        self.updater_script = local_cache_updater_script
        self.bucket_name = bucket_name
        self.private_key_path = private_key_path
        self.force_refresh = force_refresh
        self.branch = branch
        self.apc_working_dir = apc_working_dir
        self.apc_test_mode = apc_test_mode
        if pkg_file_name:
            self.pkg_file_name = pkg_file_name
        else:
            self.pkg_file_name = str(uuid.uuid4())

    def sync_files(self, sync_mode=False, use_s3=False, env='prod'):

        logger.info('BEGIN process.')
        logger.info('Sync Files Environment')
        logger.info(env)
        operation_utils.create_dir_if_missing(self.local_live_dir)
        operation_utils.create_dir_if_missing(self.local_working_dir)
        operation_utils.create_dir_if_missing(self.local_archive_dir)

        fn_to_deploy = []
        # 0. if running in sync_mode, force src server to generate local cache files
        if sync_mode:
            # backup the cache folder on src_server
            backup_cmd = 'python %s -c backup_files -i %s -e %s' % (self.updater_script, 'localcache', env)
            params = ['cmd.run_all',
                      ['cd %s && %s' % (self.run_updater_folder, backup_cmd)],
                      30, 'list', ]
            callback_param = []
            self.salt_client.execute_cmd_with_retry(
                [self.src_server], salt_utils.salt_get_stdout_callback, None, callback_param,
                'backup-local-cache-files', salt_utils.MAX_NUMBER_OF_TRIES,
                salt_utils.SLEEP_SECONDS_BETWEEN_RETRY, *params)

            self.generate_local_cache_values()
        if not use_s3:
            # 1.  get new checksums from generator server
            fn_to_checksum_remote = self.get_remote_checksums()
            if self.force_refresh:
                fn_to_deploy = list(fn_to_checksum_remote.keys())
            else:
                # 2.  check new checksums against local 'last successful run' copy
                fn_to_checksum_local = self.get_local_checksums()
                logger.debug('local checksums: %s', operation_utils.dict_to_sorted_list(fn_to_checksum_local))

                for fn in fn_to_checksum_remote:
                    if fn_to_checksum_local.get(fn, None) == fn_to_checksum_remote[fn]:
                        continue
                    else:
                        fn_to_deploy.append(fn)
                        logger.info('Found cache file:%s different.', fn)

            # 3.  checksums different: Push to all web servers
            if fn_to_deploy:
                logger.info('Number of files to push: %s. files: %s',
                            len(fn_to_deploy), fn_to_deploy)
            else:
                logger.info('All cache files are up-to-date. No need for push')
                return

            logger.info('Try to deploy files to servers: %s', self.dest_servers)
            self.deploy_files(fn_to_deploy, self.dest_servers)

            # 4.  if translation files get changed, bust APC cache.
            need_to_bust_apc = False
            for fn in fn_to_deploy:
                if fn.startswith(TRANSLATION_FILE_PREFIX):
                    need_to_bust_apc = True
                    break
            if need_to_bust_apc:
                # self.bust_apc_cache()
                self.delete_translation_cache()
        else:
            self.deploy_files_through_s3(self.dest_servers, kind='localcache', env=env)
        # 5.  update the local 'last successful run' checksums and record the push in DB
        self.update_local_checksums(use_s3, env=env)

        # 6. if use_s3, report stats to redis
        if use_s3:
            self.record_to_redis(self.dest_servers, env=env)
        logger.info('DONE process.')


@atexit.register
def exit_handler(signum=None, frame=None):
    if signum:
        logger.info('call exit_handler, signum: {}'.format(signum))
    # remove file
    try:
        logger.info('remove signal file: {}'.format(SIGNAL_FILE))
        os.remove(SIGNAL_FILE)
    except OSError:
        pass


def main(params, job_name, sync_mode=False, use_s3=False, env='prod', verbose=False, force=False):
    slogger.setUpLogger('/home/clipu/c2svc/log', job_name, level=verbose and logging.DEBUG or logging.INFO)

    logger.info("Local Cache Sync Files Environment:")
    logger.info(env)

    # make sure only one instance is running.
    process_utils.run_only_one_instance(job_name)

    for sig in [signal.SIGINT, signal.SIGQUIT, signal.SIGTERM, signal.SIGILL, signal.SIGABRT]:
        signal.signal(sig, exit_handler)

    for branch, code_path in six.iteritems(params.CODE_PATH):
        try:
            logger.info('Start {} sync process.'.format(code_path))
            src_dir = params.SRC_DIR.format(code_path)
            dest_dir = params.DEST_DIR.format(code_path)
            local_live_dir = params.LOCAL_LIVE_DIR.format(branch)
            local_archive_dir = params.LOCAL_ARCHIVE_DIR.format(branch)
            local_working_dir = params.LOCAL_WORKING_DIR.format(branch)
            remote_working_dir = params.REMOTE_WORKING_DIR.format(branch)
            remote_backup_dir = params.REMOTE_BACKUP_DIR.format(code_path)
            run_local_cache_creation_folder = params.get('RUN_LOCAL_CACHE_CREATION_FOLDER', '').format(code_path)
            apc_working_dir = params.get('APC_WORKING_DIR', '').format(code_path)
            apc_test_mode = params.get('APC_TEST_MODE', '').strip().lower() == 'true'
            use_s3 = params.get('USE_S3', '').strip().lower() == 'true'
            syncer = LocalCacheFileSyncer(
                params.SRC_SERVER_PATTERN,
                params.DEST_SERVERS_PATTERNS,
                params.DEST_SERVERS_PATTERNS_TYPE,
                params.UPDATE_LOCAL_CHECKSUMS_SERVER_PATTERN,
                src_dir,
                dest_dir,
                params.FILENAME_GLOB_PATTERN,
                local_live_dir,
                local_archive_dir,
                local_working_dir,
                remote_working_dir,
                remote_backup_dir,
                params.MASTER_SALT_CONFIG_FILE,
                params.PRIVATE_KEY_PATH,
                params.MIN_NUMBER_OF_DEST_SERVERS,
                run_local_cache_creation_folder,
                params.get('RUN_LOCAL_CACHE_UPDATER_FOLDER', ''),
                params.get('EXECUTION_ENGINE', ''),
                params.get('LOCAL_CACHE_CREATION_SCRIPT', ''),
                params.get('LOCAL_CACHE_UPDATER_SCRIPT', ''),
                params.get('BUCKET_NAME', ''),
                apc_working_dir,
                branch=branch,
                force_refresh=force,
                apc_test_mode=apc_test_mode
            )
            syncer.process(sync_mode, use_s3, env)
            logger.info('Done {} sync process.'.format(code_path))
        except:
            logger.exception('We crashed')
            sys.exit(1)
