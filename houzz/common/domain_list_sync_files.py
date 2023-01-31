#!/usr/bin/env python
'''Tool on salt master to sync files between remote servers.

General process:
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
'''
from __future__ import absolute_import
from datetime import datetime
import os
import sys
import optparse
import logging
import uuid

from houzz.common import salt_sync_files
from houzz.common.logger import slogger
from houzz.common import operation_utils
from houzz.common import process_utils
from houzz.common import salt_utils
import six

logger = logging.getLogger(__name__)
CHECKSUM_FILE_EXTENSION = 'checksum'


class DomainListFileSyncer(salt_sync_files.SaltFileSyncer):
    def __init__(self,
                 src_server_pttn,
                 dest_servers_pttns,
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
                 run_domain_list_creation_folder,
                 execution_engine,
                 domain_list_creation_script,
                 run_domain_list_updater_folder,
                 domain_list_updater_script,
                 bucket_name,
                 pkg_file_name=None,
                 force_refresh=False,
                 branch='current',
                 kind='domainlist',
        ):
        # some sanity check on parameters
        for dir_path in (local_live_dir, local_working_dir):
            assert dir_path.startswith(salt_utils.MASTER_SALT_ROOT_DIR)
        assert os.path.basename(glob_pttn) == glob_pttn, 'glob_pttn should only contain basename'
        assert not os.path.splitext(glob_pttn)[-1] == '.%s' % CHECKSUM_FILE_EXTENSION, (
                'glob_pttn should not end with %s' % CHECKSUM_FILE_EXTENSION)
        self.salt_client = salt_utils.SaltClient(salt_conf_file)
        src_servers = self.salt_client.find_alive([src_server_pttn])
        assert len(src_servers) == 1, '"%s" should only match 1 src server, but found %s' % (
            src_server_pttn, len(src_servers))
        self.src_server = src_servers[0]
        dest_servers = self.salt_client.find_alive(dest_servers_pttns)
        assert len(dest_servers) >= min_num_dest_servers, (
                '"%s" should match at least %s servers, but found %s' % (
            dest_servers_pttns, min_num_dest_servers, len(dest_servers)))
        self.dest_servers = dest_servers
        self.src_dir = src_dir
        self.dest_dir = dest_dir
        self.glob_pttn = glob_pttn
        # file has to have checksum counterpart
        self.checksum_glob_pttn = '%s.%s' % (glob_pttn, CHECKSUM_FILE_EXTENSION)
        self.local_live_dir = local_live_dir
        self.local_archive_dir = local_archive_dir
        self.local_working_dir = local_working_dir
        self.remote_working_dir = remote_working_dir
        self.remote_backup_dir = remote_backup_dir
        self.run_domain_list_creation_folder = run_domain_list_creation_folder
        self.execution_engine = execution_engine
        self.domain_list_creation_script = domain_list_creation_script
        self.private_key_path = private_key_path
        self.force_refresh = force_refresh
        self.branch = branch
        self.run_updater_folder = run_domain_list_updater_folder
        self.updater_script = domain_list_updater_script
        self.kind=kind

        # Only for S3
        self.bucket_name = bucket_name

        # Only for SCP
        if pkg_file_name:
            self.pkg_file_name = pkg_file_name
        else:
            self.pkg_file_name = str(uuid.uuid4())

    def sync_files(self, sync_mode=False, use_s3=False, env='prod'):
        logger.info('BEGIN process.')
        operation_utils.create_dir_if_missing(self.local_live_dir)
        operation_utils.create_dir_if_missing(self.local_working_dir)
        operation_utils.create_dir_if_missing(self.local_archive_dir)

        fn_to_deploy = []
        # 0. if running in sync_mode, force src server to generate custom domain list
        if sync_mode:
            self.generate_custom_domain_list()

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
        else:
            self.deploy_files_through_s3(self.dest_servers, kind=self.kind, env=env)
        logger.info('DONE process.')


def main(params, job_name, sync_mode=False, kind='domainlist'):
    parser = optparse.OptionParser('usage: %prog [options]')
    parser.add_option(
        '-v', '--verbose', dest='verbose', action='store_true',
        help='log at DEBUG level')
    parser.add_option(
        '-f', '--force', dest='force', action='store_true',
        help='force to refresh the cache files')

    (options, args) = parser.parse_args()
    if len(args):
        parser.error('incorrect number of arguments')
    slogger.setUpLogger('/home/clipu/c2svc/log', job_name,
                        level=options.verbose and logging.DEBUG or logging.INFO)

    # make sure only one instance is running.
    process_utils.run_only_one_instance(job_name)

    for branch, code_path in six.iteritems(params.CODE_PATH):
        try:
            logger.info('Start {} sync process.'.format(code_path))
            src_dir = params.SRC_DIR.format(code_path)
            dest_dir = params.DEST_DIR.format(code_path)
            local_live_dir = params.LOCAL_LIVE_DIR.format(branch);
            local_archive_dir = params.LOCAL_ARCHIVE_DIR.format(branch)
            local_working_dir = params.LOCAL_WORKING_DIR.format(branch)
            remote_working_dir = params.REMOTE_WORKING_DIR.format(branch)
            remote_backup_dir = params.REMOTE_BACKUP_DIR.format(code_path)
            run_domain_list_creation_folder = params.get('RUN_DOMAIN_LIST_CREATION_FOLDER', '').format(code_path)
            use_s3 = params.get('USE_S3', '').strip().lower() == 'true'
            syncer = DomainListFileSyncer(
                params.SRC_SERVER_PATTERN,
                params.DEST_SERVERS_PATTERNS,
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
                run_domain_list_creation_folder,
                params.get('EXECUTION_ENGINE', ''),
                params.get('DOMAIN_LIST_CREATION_SCRIPT', ''),
                params.get('RUN_DOMAIN_LIST_UPDATER_FOLDER', ''),
                params.get('DOMAIN_LIST_UPDATER_SCRIPT', ''),
                params.get('BUCKET_NAME', ''),
                branch=branch,
                force_refresh=options.force,
                kind=kind,
            )
            syncer.process(sync_mode, use_s3)
            logger.info('Done {} sync process.'.format(code_path))
        except:
            logger.exception('We crashed')
            sys.exit(1)
