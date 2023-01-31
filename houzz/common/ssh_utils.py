# Refactored out of operation_utils.py on 2019-03-08.

from __future__ import absolute_import
from datetime import datetime
import os
import time

import houzz.common.scp as scp
import six

def create_ssh_client(server_ip, server_port, username, private_key_file, proxy_command=None):
    # NOTE: have to delay the import here because daemonization will close all the
    # open file handler. paramiko depends on a random number generator setup
    # during the import time. So it will be closed if import too early.
    # See issue report at https://github.com/paramiko/paramiko/issues/59
    import paramiko
    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    sock = None
    if proxy_command:
        sock = paramiko.ProxyCommand(proxy_command)
    ssh_client.connect(
        server_ip, server_port, username, key_filename=private_key_file, sock=sock)
    return ssh_client

def ssh_exec(ssh_client, cmd):
    # Execute general command
    return ssh_client.exec_command(cmd)

def scp_get(ssh_client, files, local_dir, scp_channel_timeout, transfer_rate=None):
    # Create the local dir
    if not os.path.exists(local_dir):
        os.mkdir(local_dir, 0o755)

    scp_client = scp.SCPClient(
        ssh_client.get_transport(), bandwidth=transfer_rate,
        socket_timeout=scp_channel_timeout)
    if isinstance(files, six.string_types): # in case a single file path is passed in
        files = [files]
    for f in files:
        scp_client.get(f, local_path=local_dir, preserve_times=True)

def scp_put(ssh_client, files, remote_dir, scp_channel_timeout, transfer_rate=None, mkdir_timeout=1):
    # Create the remote dir
    ssh_client.exec_command('mkdir -p %s' % (remote_dir))
    sftp = ssh_client.open_sftp()
    try:
        sftp.stat(remote_dir)
    except:
        # try again
        time.sleep(mkdir_timeout)
        ssh_client.exec_command('mkdir -p %s' % (remote_dir))

    scp_client = scp.SCPClient(
        ssh_client.get_transport(), bandwidth=transfer_rate,
        socket_timeout=scp_channel_timeout)
    scp_client.put(files, remote_path=remote_dir, preserve_times=True)

def ssh_chgrp(ssh_client, path_list, group):
    # change group of given list of files.
    ssh_client.exec_command('chgrp %s %s' % (group, ' '.join(path_list)))

def ssh_touch(ssh_client, path_list, mtime):
    # call touch on the given list of files.
    mtime_str = datetime.fromtimestamp(mtime).strftime("%c")
    ssh_client.exec_command('touch -d "%s" %s' % (mtime_str, ' '.join(path_list)))

def ssh_mkdir(ssh_client, path):
    ssh_client.exec_command('mkdir -p "%s"' % path)

def ssh_mv(ssh_client, source, target):
    ssh_client.exec_command('mv "%s" "%s"' % (source, target))

def ssh_cp(ssh_client, source, target):
    ssh_client.exec_command('cp "%s" "%s"' % (source, target))
