#!/usr/bin/env python
from __future__ import absolute_import
from collections import defaultdict
from datetime import datetime
import logging
import math
import os
import paramiko
import subprocess
import tempfile
import time
import boto3
import houzz.common.email_utils
import houzz.common.logger.slogger
import houzz.common.py3_util
import houzz.common.config
import six
from six.moves import range

logger = logging.getLogger(__name__)
Config = houzz.common.config.get_config()


class HouzzEC2Exception(Exception):
    pass


class OutOfSpaceException(HouzzEC2Exception):
    pass


def send_alert_email(subject, body):
    houzz.common.email_utils.send_text_email(
        Config.DAEMON_PARAMS.ALERT_EMAIL_FROM,
        Config.DAEMON_PARAMS.ALERT_EMAIL_TO,
        subject, body, Config.DAEMON_PARAMS.MAIL_PASSWORD)


def get_boto3_s3_client(access_key_id=None, access_key=None, host=None):

    access_key_id = access_key_id or Config.AWS_PARAMS.AWS_ACCESS_KEY_ID
    access_key = access_key or Config.AWS_PARAMS.AWS_SECRET_ACCESS_KEY
    host = host or Config.AWS_PARAMS.S3_DEFAULT_HOST
    return boto3.client(
        "s3",
        endpoint_url=host,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=access_key
    )


def get_boto3_s3_resource(access_key_id=None, access_key=None, host=None):

    access_key_id = access_key_id or Config.AWS_PARAMS.AWS_ACCESS_KEY_ID
    access_key = access_key or Config.AWS_PARAMS.AWS_SECRET_ACCESS_KEY
    host = host or Config.AWS_PARAMS.S3_DEFAULT_HOST
    return boto3.resource(
        "s3",
        endpoint_url=host,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=access_key
    )


def get_directory_size(dir_path):
    # return size in bytes. Same number as 'du -sb'
    total_size = os.path.getsize(dir_path)
    for item in os.listdir(dir_path):
        itempath = os.path.join(dir_path, item)
        if os.path.isfile(itempath):
            total_size += os.path.getsize(itempath)
        elif os.path.isdir(itempath):
            total_size += get_directory_size(itempath)
    return total_size


def get_files_size(file_path_list):
    # return size in bytes.
    total_size = 0
    for fp in file_path_list:
        assert os.path.isfile(fp), '%s is NOT regular file' % fp
        total_size += os.path.getsize(fp)
    return total_size


def calc_volume_size_needed(file_size_GB):
    # Leave 20% extra headroom for the volume to grow
    # du -sb only report the total size. It ignores the size wasted by file smaller
    # than one single block (4k) because one block can only hold one file.
    size_GB = int(math.ceil(file_size_GB * (
        1 + Config.AWS_PARAMS.VOLUME_EXTRA_HEADROOM_PERCENTAGE / 100.0)))
    return size_GB


def calc_volume_size_needed_for_dir(src_dir):
    return calc_volume_size_needed(get_directory_size(src_dir) / 1e9)


def get_mount_point(src_dir):
    """get mount_point for src_dir
    >>> b='/home/clipu/c2/images/0001'
    >>> get_mount_point('/Users/zhengliu/houzz/c2/images/0000')
    '/home/clipu/c2/images/0000'
    >>> get_mount_point('/Users/zhengliu/houzz/c2/files')
    '/home/clipu/c2/files'
    >>> get_mount_point('/Users/zhengliu/backup_logs')
    '/home/clipu/c2/backup_logs'
    """
    if '/c2/' in src_dir:
        return os.path.join(
            Config.AWS_PARAMS.RSYNC_ROOT_DIR,
            src_dir[src_dir.find('/c2/') + len('/c2/'):])
    else:
        return os.path.join(
            Config.AWS_PARAMS.RSYNC_ROOT_DIR,
            os.path.basename(src_dir))


def get_device_for_mount_point(src_dir):
    """map src_dir to device
    use /dev/sdz[1-15] for dirs not under c2/images.
    example src_dir: /home/clipu/c2/images/0000
    available old device range: /dev/sd[f-p][1-15]
    available new device range: /dev/xvd[f-p][1-15]
    >>> get_device_for_mount_point('/home/clipu/c2/images/0000')
    ('/dev/sdf1', '/dev/xvdf1')
    >>> get_device_for_mount_point('/home/clipu/c2/images/0015')
    ('/dev/sdg1', '/dev/xvdg1')
    >>> get_device_for_mount_point(Config.AWS_PARAMS.OTHER_ARCHIVE_DIRS[1])
    ('/dev/sdz2', '/dev/xvdz2')
    """
    if not os.path.basename(src_dir).isdigit():
        device_number = Config.AWS_PARAMS.OTHER_ARCHIVE_DIRS.index(src_dir) + 1
        return ('/dev/sdz%d' % device_number, '/dev/xvdz%d' % device_number)
    image_dir_number = int(os.path.basename(src_dir))
    device_letter = chr(ord('f') + image_dir_number / 15)
    device_number = image_dir_number % 15 + 1
    return ('%s%s%d' % (Config.AWS_PARAMS.DEVICE_OLD_PREFIX, device_letter, device_number),
            '%s%s%d' % (Config.AWS_PARAMS.DEVICE_NEW_PREFIX, device_letter, device_number))


def is_to_be_refreshed_volumes(v, categories=None):
    """New volume will be created and synced, then old one get deleted.
    Mainly used for data that not suited for incremental snapshot, such as big .gz file
    """
    # only refresh if the src_path is on this machine. i.e., you don't want
    # to refresh redis or mysql dump when you run on himage
    src_path = v.tags.get('src_path')
    resu = (src_path and os.path.exists(src_path) and
            src_path in Config.AWS_PARAMS.NEED_REFRESH_ARCHIVE_DIRS and
            (not categories or v.tags.get('category') in categories))
    logger.debug('check refresh. vol.id:%s, src_path:%s, refresh:%s', v.id, src_path, resu)
    return resu


def get_rsync_dest_path(dns_name, mount_point):
    """create rsync destination path
    >>> a='ec2-50-112-25-102.us-west-2.compute.amazonaws.com'
    >>> b='/home/clipu/c2/images/0001'
    >>> get_rsync_dest_path(a, b)
    'www-data@ec2-50-112-25-102.us-west-2.compute.amazonaws.com::c2/images/0001'
    >>> c='/home/clipu/c2/adfiles'
    >>> get_rsync_dest_path(a, c)
    'www-data@ec2-50-112-25-102.us-west-2.compute.amazonaws.com::c2/adfiles'
    """
    return '%s@%s::%s' % (
        Config.AWS_PARAMS.RSYNC_USER, dns_name,
        os.path.join(
            os.path.basename(Config.AWS_PARAMS.RSYNC_ROOT_DIR),
            mount_point[mount_point.find('/c2/') + len('/c2/'):])
    )


def rsync_dir(src_path, dest_path):
    logger.info('start rsync. src:%s, dest:%s', src_path, dest_path)
    # use tempfile in case output is too large to overun the buffer
    (fd, fn) = tempfile.mkstemp(dir='/tmp')
    cmd_args = [
        'rsync', '-av', '--stats', '--size-only',
        '--password-file=%s' % Config.AWS_PARAMS.RSYNC_PASSWORD_FILE_PATH,
        src_path + '/', dest_path]
    logger.info('running cmd: %s', ' '.join(cmd_args))
    p = subprocess.Popen(cmd_args, stderr=fd, stdout=fd)
    p.communicate()
    os.fdopen(fd).close()
    if p.returncode:
        log_func = logger.error
    else:
        log_func = logger.debug
    with open(fn, 'r') as f:
        log_func(f.read())
    os.unlink(fn)
    if int(p.returncode) == 12:
        raise OutOfSpaceException('rsync outofspace with returncode: %s.' % p.returncode)
    elif int(p.returncode) == 24:
        logger.info('ignore files-varnished error (code 24)')
    elif p.returncode:
        raise HouzzEC2Exception('rsync failed with returncode: %s.' % p.returncode)
    logger.info('done rsync. src:%s, dest:%s', src_path, dest_path)


def get_image_group_src_dirs(mount_point):
    """get all the image dirs in the shard volume
    >>> get_image_group_src_dirs('/image_group_000')
    ['/home/clipu/c2/images/0000', '/home/clipu/c2/images/0032', '/home/clipu/c2/images/0064', '/home/clipu/c2/images/0096', '/home/clipu/c2/images/0128', '/home/clipu/c2/images/0160', '/home/clipu/c2/images/0192']
    """
    res = []
    shard = int(mount_point.split('_')[-1])
    for i in range(Config.AWS_PARAMS.MAX_IMAGES_DIR_NUMBER / Config.AWS_PARAMS.IMAGE_GROUP_NUMBER_SHARDS + 1):
        image_dir_num = i * Config.AWS_PARAMS.IMAGE_GROUP_NUMBER_SHARDS + shard
        if (Config.AWS_PARAMS.MIN_IMAGES_DIR_NUMBER <= image_dir_num <=
                Config.AWS_PARAMS.MAX_IMAGES_DIR_NUMBER):
            src_path = '/home/clipu/c2/images/%04d' % image_dir_num
            res.append(src_path)
    return res


def rsync_volume(instance, volume, sync_dirs=None):
    changed = False
    if sync_dirs is None:
        sync_dirs = set([])
    if volume.tags.get('category') == 'image_group':
        for src_path in get_image_group_src_dirs(volume.tags.get('mount_point', '')):
            if os.path.exists(src_path) and src_path in sync_dirs:
                dest_path = get_rsync_dest_path(instance.dns_name, src_path)
                rsync_dir(src_path, dest_path)
                changed = True
    else:
        src_path = '%s' % volume.tags['src_path']
        if os.path.exists(src_path) and src_path in sync_dirs:
            dest_path = get_rsync_dest_path(instance.dns_name, volume.tags['mount_point'])
            rsync_dir(src_path, dest_path)
            changed = True
    return changed


# TODO(zheng): cache the connections, also close connections upone object out of scope
def get_ssh_client(
        instance, port=Config.AWS_PARAMS.SSH_PORT, user=Config.AWS_PARAMS.SSH_USER,
        key_path=Config.AWS_PARAMS.SSH_KEY_FILE_PATH):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(instance.ip_address, port, user, key_filename=key_path)
    return ssh_client


def run_ssh_command(ssh_client, cmd, raise_on_returncode=True, raise_on_stderr=True):
    """set raise_on_ to False if error is acceptable"""

    def raise_exception(retcode, out_str, err_str):
        raise HouzzEC2Exception('cmd:%s, return_code:%s, stdout:%s, stderr:%s' % (
            cmd, retcode, out_str, err_str))

    logger.info('run ssh command: %s', cmd)
    chan = ssh_client.get_transport().open_session()
    chan.exec_command(cmd)
    retcode = chan.recv_exit_status()
    _stdin = chan.makefile('wb', -1)
    stdout = chan.makefile('rb', -1)
    stderr = chan.makefile_stderr('rb', -1)
    out_str = stdout.read()
    err_str = stderr.read()
    logger.info('ssh ret_code:%s, stdout:%s, stderr:%s', retcode, out_str, err_str)
    if retcode or err_str:
        if (raise_on_returncode and retcode) or (raise_on_stderr and err_str):
            raise_exception(retcode, out_str, err_str)
        else:
            logger.info('ignore acceptable ssh command error')
    return (retcode, out_str, err_str)


def block_till_instance_ready(
        ec2_conn, instance,
        additional_sleep=Config.AWS_PARAMS.EC2_ADDITIONAL_SLEEP_SECONDS,
        retry_sleep=Config.AWS_PARAMS.EC2_SLEEP_BETWEEN_RETRY_SECONDS):
    time.sleep(additional_sleep)
    while not instance.update() == 'running':
        time.sleep(retry_sleep)
    while True:
        status_list = ec2_conn.get_all_instance_status([instance.id])
        if status_list and status_list[0].instance_status.status == 'ok':
            break
        time.sleep(retry_sleep)
    time.sleep(additional_sleep)


def block_till_volume_state(ec2_conn, volume, state, attach_status,
                            additional_sleep=Config.AWS_PARAMS.EC2_ADDITIONAL_SLEEP_SECONDS,
                            retry_sleep=Config.AWS_PARAMS.EC2_SLEEP_BETWEEN_RETRY_SECONDS):
    time.sleep(additional_sleep)
    while volume.update() != state or volume.attach_data.status != attach_status:
        time.sleep(retry_sleep)
    while True:
        status_list = ec2_conn.get_all_volume_status([volume.id])
        if status_list and status_list[0].volume_status.status == 'ok':
            break
        time.sleep(retry_sleep)
    time.sleep(additional_sleep)


def attach_and_mount_volume(
        ec2_conn, instance, volume,
        disk_format=Config.AWS_PARAMS.EBS_DISK_FORMAT,
        user=Config.AWS_PARAMS.RSYNC_USER,
        group=Config.AWS_PARAMS.RSYNC_USER,
        force_format=False):
    """attach and mount the volume"""
    logger.info('start attach volume: %s', volume)
    device_old = volume.tags['device_old']
    device_new = volume.tags['device_new']
    mount_point = volume.tags['mount_point']
    logger.info(
        'start attach and mount volume. instance:%s, volume:%s, device:%s, mount:%s',
        instance.id, volume.id, device_old, mount_point)
    # detach the new volume if it is attached to other instance
    if volume.attachment_state() == 'attached':
        if instance.id == volume.attach_data.instance_id:
            logger.info('volume is already attached. instance:%s, volume:%s',
                        instance.id, volume.id)
        else:
            umount_and_detach_volume(ec2_conn, volume)
    # detach other volume if it took what the new volume's mount point
    instance.update()
    if device_old in instance.block_device_mapping:
        vid = instance.block_device_mapping[device_old].volume_id
        if vid != volume.id:
            sv = ec2_conn.get_all_volumes([vid])[0]
            umount_and_detach_volume(ec2_conn, sv)
    # attach the new volume if it is not attached yet
    if volume.update() != 'in-use':
        volume.attach(instance.id, device_old)
        block_till_volume_state(ec2_conn, volume, 'in-use', 'attached')
    ssh_client = get_ssh_client(instance)
    run_ssh_command(ssh_client, 'sudo mkdir -p -m 755 %s' % mount_point)
    run_ssh_command(ssh_client, 'sudo chown -R %s:%s %s' % (user, group, mount_point))
    logger.info('mount volume. instance:%s, volume:%s', instance.id, volume.id)
    try:
        run_ssh_command(ssh_client, 'sudo mount %s %s' % (device_new, mount_point))
    except HouzzEC2Exception as e:
        if ' already mounted ' in str(e):
            # ghetto way to detect the disk was already mounted
            pass
        elif 'you must specify the filesystem type' in str(e) and (
                    force_format and volume.tags.get('formatted') == 'no'):
            # ghetto way to detect the disk was not formatted before
            logger.info('format disk:%s to type:%s', device_new, disk_format)
            run_ssh_command(
                ssh_client, 'sudo mkfs -t %s %s' % (disk_format, device_new),
                raise_on_stderr=False)
            run_ssh_command(ssh_client, 'sudo mount %s %s' % (device_new, mount_point))
            volume.remove_tag('formatted')
        else:
            ssh_client.close()
            raise
    # mount somehow not obeying the previous mount_point ownership
    run_ssh_command(ssh_client, 'sudo chown -R %s:%s %s' % (user, group, mount_point))

    # Add to fstab if it has not been, so reboot will auto-mount it
    fstab_line = '%s %s ext3 defaults 0 0' % (device_new, mount_point)
    run_ssh_command(
        ssh_client,
        'sudo grep %s /etc/fstab || ('
        '    sudo chmod 666 /etc/fstab && '
        '    sudo echo "%s" >> /etc/fstab && '
        '    sudo chmod 644 /etc/fstab)' % (device_new, fstab_line))
    # additional bind mount to create alias for the dirs. ie.
    # /home/clipu/c2/images/0000 points to /image_group_000/0000
    bindings = get_bind_mounts(volume)
    for src, dest in bindings:
        run_ssh_command(ssh_client, 'sudo mkdir -p -m 755 %s' % src)
        run_ssh_command(ssh_client, 'sudo chown -R %s:%s %s' % (user, group, src))
        run_ssh_command(ssh_client, 'sudo mkdir -p -m 755 %s' % dest)
        run_ssh_command(ssh_client, 'sudo chown -R %s:%s %s' % (user, group, dest))
        run_ssh_command(
            ssh_client,
            'sudo df -ha | grep %s || sudo mount -o bind %s %s' % (src, src, dest))
        fl = '%s %s none bind 0 0' % (src, dest)
        run_ssh_command(
            ssh_client,
            'sudo grep %s /etc/fstab || ('
            '    sudo chmod 666 /etc/fstab && '
            '    sudo echo "%s" >> /etc/fstab && '
            '    sudo chmod 644 /etc/fstab)' % (src, fl))
    ssh_client.close()
    logger.info(
        'done attach and mount volume. instance:%s, volume:%s, device:%s, mount:%s',
        instance.id, volume.id, device_old, mount_point)


def get_bind_mounts(volume):
    category = volume.tags.get('category')
    mount_point = volume.tags.get('mount_point')
    res = []
    if category == 'image_group':
        for dest in get_image_group_src_dirs(volume.tags.get('mount_point', '')):
            res.append((os.path.join(mount_point, os.path.basename(dest)), dest))
    elif category == 'data':
        res.append(('/mnt/solrAnswers', '/home/clipu/c2/solrAnswers'))
        res.append(('/mnt/solrGalleries', '/home/clipu/c2/solrGalleries'))
        res.append(('/mnt/solrSpaces', '/home/clipu/c2/solrSpaces'))
        res.append(('/mnt/solrUsers', '/home/clipu/c2/solrUsers'))
        res.append(('/mnt/tmp/innobackup', '/var/lib/mysql'))
    return res


def umount_volume(ec2_conn, volume):
    volume.update()
    device_old = volume.attach_data.device
    mount_point = volume.tags['mount_point']
    instance_id = volume.attach_data.instance_id
    if not instance_id or not device_old:
        logger.info('volume:%s not attached to any instance. no-op to umount', volume.id)
        return
    instance = ec2_conn.get_all_instances([instance_id])[0].instances[0]
    psc = get_ssh_client(instance)
    try:
        run_ssh_command(psc, 'sudo df -ha | grep %s' % mount_point)
    except HouzzEC2Exception:
        logger.info(
            'volume:%s attached to %s but not mounted. no-op to umount',
            instance.id, volume.id)
        return
    logger.info(
        'umount volume. instance:%s, volume:%s, device:%s, mount:%s',
        instance.id, volume.id, device_old, mount_point)
    run_ssh_command(psc, 'sudo umount -l %s' % mount_point, False, False)
    psc.close()
    volume.update()
    logger.info(
        'done unmount volume. instance:%s, volume:%s, device:%s, mount:%s',
        instance.id, volume.id, device_old, mount_point)


def umount_and_detach_volume(ec2_conn, volume):
    umount_volume(ec2_conn, volume)
    instance_id = volume.attach_data.instance_id
    if not instance_id:
        logger.info('volume:%s not attached to any instance. no-op to detach', volume.id)
        return
    volume.detach(True)
    block_till_volume_state(ec2_conn, volume, 'available', None)
    volume.update()
    logger.info('done detach volume. volume:%s', volume.id)


def create_instance(
        ec2_conn,
        image_id=Config.AWS_PARAMS.IMAGE,
        key_name=Config.AWS_PARAMS.KEY_NAME,
        instance_type=Config.AWS_PARAMS.INSTANCE_TYPE,
        placement=Config.AWS_PARAMS.ZONE,
        shutdown_behavior=Config.AWS_PARAMS.INSTANCE_SHUTDOWN_BEHAVIOR,
        elastic_ip='', user_data='', security_groups=None):
    logger.info(
        'START create instance. type:%s, image:%s',
        instance_type, image_id)
    if not security_groups:
        security_groups = Config.AWS_PARAMS.SECURITY_GROUPS
    reservation = ec2_conn.run_instances(
        image_id=image_id, key_name=key_name,
        user_data=user_data,
        security_groups=security_groups,
        instance_type=instance_type, placement=placement,
        instance_initiated_shutdown_behavior=shutdown_behavior)
    instance = reservation.instances[0]
    block_till_instance_ready(ec2_conn, instance)
    if elastic_ip:
        ec2_conn.associate_address(instance.id, elastic_ip)
    instance.update()
    logger.info('DONE create instance. type:%s, name:%s',
                instance.instance_type, instance.dns_name)
    return instance


def start_instance(ec2_conn, instance_id):
    logger.info('START start instance. instance_id:%s', instance_id)
    instance = ec2_conn.start_instances([instance_id])[0]
    block_till_instance_ready(ec2_conn, instance)
    logger.info('DONE start instance. instance_id:%s', instance_id)
    return instance


def attach_and_mount_volumes(
        ec2_conn, instance, volume_list,
        disk_format=Config.AWS_PARAMS.EBS_DISK_FORMAT,
        user=Config.AWS_PARAMS.RSYNC_USER,
        group=Config.AWS_PARAMS.RSYNC_USER,
        force_format=False):
    """attach and mount all volumes in volume_list"""
    logger.info('START attach all volumes: %s', volume_list)
    for v in volume_list:
        attach_and_mount_volume(ec2_conn, instance, v, disk_format, user, group, force_format)
    logger.info('DONE attach all volumes: %s', volume_list)


def get_category_to_volumes(ec2_conn):
    """return mapping of category -> [volumes]"""
    res = defaultdict(list)
    for v in ec2_conn.get_all_volumes():
        res[v.tags.get('category')].append(v)
    logger.info('category->volumes:%s', res)
    return res


def get_mount_point_to_snapshot_map(ec2_conn):
    """return mount_point => list[(create_time, snapshot)]. List is sorted by create_time (older one first).
    use volume's mount_piont as volume_key. This is because volume id for the some volume could change
    in case of crash/restart, or intentional volume recreation for 'data' volume. For image_group,
    we have 32 shards, mount_point can differentiate between them.
    """
    all_ss = ec2_conn.get_all_snapshots(owner='self')
    m2s = defaultdict(list)
    for ss in all_ss:
        ss_t = ss.tags.get('created')
        mount_point = ss.tags.get('mount_point')
        if ss_t and mount_point:
            m2s[mount_point].append((ss_t, ss))
    _ = [i.sort() for i in six.itervalues(m2s)]
    return m2s


def snapshot_volume(instance, volume):
    """snapshot the volume. Snapshot is NOT done yet after return"""
    category = volume.tags['category']
    device_old = volume.tags['device_old']
    mount_point = volume.tags['mount_point']
    logger.info(
        'start snapshot volume. instance:%s, volume:%s, device:%s, mount:%s',
        instance.id, volume.id, device_old, mount_point)
    timestamp = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    # decide not to detach before snapshoting. The chance of corruption is slim
    snapshot = volume.create_snapshot('%s|%s|%s' % (category, mount_point, timestamp))
    snapshot.add_tag('category', category)
    snapshot.add_tag('mount_point', mount_point)
    snapshot.add_tag('created', timestamp)
    logger.info(
        'done snapshot volume. instance:%s, volume:%s, device:%s, mount:%s, snapshot:%s',
        instance.id, volume.id, device_old, mount_point, snapshot.id)
    return snapshot


def snapshot_volumes(instance, volume_list):
    """snapshot all volumes in volume_list"""
    logger.info('START snapshot all volumes: %s', volume_list)
    outstanding_snapshots = []
    for v in volume_list:
        outstanding_snapshots.append(snapshot_volume(instance, v))
    logger.info(
        'DONE snapshot all volumes: %s. snapshots: %s',
        volume_list, outstanding_snapshots)
    return outstanding_snapshots


def finalize_snapshot(snapshot):
    logger.info('START wait on outstanding snapshot: %s', snapshot)
    while True:
        snapshot.update()
        # TODO(zheng): throw if snapshot fails
        if snapshot.status == 'completed':
            # snapshot is done
            break
        time.sleep(Config.AWS_PARAMS.EC2_ADDITIONAL_SLEEP_SECONDS)
    logger.info('DONE wait on outstanding snapshot: %s', snapshot)


def finalize_snapshots(snapshots):
    """block on outstanding snapshots till they are done"""
    logger.info('START wait on outstanding snapshots: %s', snapshots)
    for ss in snapshots:
        finalize_snapshot(ss)
    logger.info('DONE wait on outstanding snapshots: %s', snapshots)


def get_max_number_snapshots_by_category(category):
    """return non-negative number. 0 means keep as many as you have"""
    if category == 'adfiles':
        return 1
    elif category == 'backup_log':
        return 7
    elif category == 'data':  # mysql and redis dump, keep 7 versions
        return 7
    elif category == 'files':
        return 1
    elif category == 'image_group':
        return 1
    elif category == 'prof_images':
        return 1
    elif category == 'user_images':
        return 1
    return 0


def cleanup_snapshots(ec2_conn, snapshots=None):
    """delete obsolete snapshots. Number to keep depends on volume category"""
    if snapshots is None:
        snapshots = ec2_conn.get_all_snapshots(owner='self')
    logger.info('START cleanup obsolete snapshots: %s', snapshots)
    mps = set([ss.tags.get('mount_point') for ss in snapshots])
    logger.debug('mount_point: %s', mps)
    m2s = get_mount_point_to_snapshot_map(ec2_conn)
    logger.debug('volume mount_point to snapshot map: %s', m2s)
    for mp in mps:
        if mp in m2s and m2s[mp]:
            category = m2s[mp][0][1].tags.get('category')
            num_to_keep = get_max_number_snapshots_by_category(category)
            logger.info(
                'mount_point: %s, num_keep: %s, snapshots: %s',
                mp, num_to_keep, m2s[mp])
            for ts, ss in m2s[mp][:-num_to_keep]:
                logger.info('deleting snapshot: %s, timestamp: %s', ss, ts)
                ss.delete()
    logger.info('DONE cleanup obsolete snapshots: %s', snapshots)


def is_ec2_instance():
    return os.path.isfile('/etc/salt/is_ec2')


def getec2info():
    import requests
    token = requests.put('http://169.254.169.254/latest/api/token', headers={'X-aws-ec2-metadata-token-ttl-seconds': '60'})
    req = requests.get('http://169.254.169.254/latest/meta-data/local-ipv4', timeout=0.005, headers={'X-aws-ec2-metadata-token': token.text})
    ip = houzz.common.py3_util.unicode2str(req.text)
    req = requests.get('http://169.254.169.254/latest/meta-data/instance-id', timeout=0.005, headers={'X-aws-ec2-metadata-token': token.text})
    i_id = houzz.common.py3_util.unicode2str(req.text)
    return ip, i_id
