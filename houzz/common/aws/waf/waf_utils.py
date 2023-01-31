#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function
import ipaddress
import json
import logging
import math
import optparse
import os
import time

import boto3

import houzz.common.config
from houzz.common.logger import slogger
import six
from six.moves import input

logger = logging.getLogger(__name__)
Config = houzz.common.config.get_config()


PROMPT_CONFIRM_CONTINUE = """
Please confirm the changes are expected to continue: [Y/N]
>>"""

# https://stackoverflow.com/questions/1165352/calculate-difference-in-keys-contained-in-two-python-dictionaries
class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)

    def added(self):
        return self.set_current - self.intersect

    def removed(self):
        return self.set_past - self.intersect

    def changed(self):
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])

    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])


def get_client(conf, name):
    return boto3.client(name, aws_access_key_id=conf['access_key_id'],
                  aws_secret_access_key=conf['access_key'])


def get_valid_input(prompt, valid_values):
    valid_values_upper = [i.upper() for i in valid_values]
    while True:
        ui = input(prompt).upper()
        if ui in valid_values_upper:
            return ui
        else:
            print('invalid input. Please choose from: %s' % valid_values)


def confirm_continue():
    ui = get_valid_input(PROMPT_CONFIRM_CONTINUE, ['Y', 'N'])
    return ui.upper() == 'Y'


def get_change_token(waf_client):
    response = waf_client.get_change_token()
    if 'ChangeToken' in response:
        return response['ChangeToken']
    raise Exception("Unable to get change token")


def get_change_token_status(waf_client, **kwargs):
    change_token = kwargs['change_token']
    status = None
    max_retry = kwargs['max_retry'] if 'max_retry' in kwargs else 1
    retry = 0
    while retry < max_retry:
        response = waf_client.get_change_token_status(ChangeToken=change_token)
        if 'ChangeTokenStatus' in response:
            status = response['ChangeTokenStatus']
            print("ChangeToken: {} Response: {}".format(change_token, status))
            if status == 'INSYNC':
                print("Change Completed.")
                break
        retry += 1
        time.sleep(3)
    return status == 'INSYNC'


def list_web_acls(waf_client, **kwargs):
    response = waf_client.list_web_acls()
    if 'WebACLs' in response:
        for web_acl in response['WebACLs']:
            print("WebACL: Id {} Name: {}".format(web_acl['WebACLId'], web_acl['Name']))
    else:
        print("Invalid response for list_web_acls: {}".format(response))
        return False
    return True


def list_ip_sets(waf_client, **kwargs):
    response = waf_client.list_ip_sets()
    if 'IPSets' in response:
        for ip_set in response['IPSets']:
            print("IPSet: Id: {} Name: {}".format(ip_set['IPSetId'], ip_set['Name']))
    else:
        print("Invalid response for list_ip_sets: {}".format(response))
        return False
    return True


def get_ip_set(waf_client, **kwargs):
    skip_print = kwargs['skip_print'] if 'skip_print' in kwargs else False
    ip_set_id = kwargs['ip_set_id']
    ip_map = {}
    response = waf_client.get_ip_set(IPSetId=ip_set_id)
    if 'IPSet' in response and 'IPSetDescriptors' in response['IPSet']:
        ip_set = response['IPSet']
        print("IPSet: Id: {} Name: {}".format(ip_set['IPSetId'], ip_set['Name']))
        for ip_set_desc in ip_set['IPSetDescriptors']:
            ip_map[ip_set_desc['Value']] = ip_set_desc
            if not skip_print:
                print("Type: {} Value: {}".format(ip_set_desc['Type'], ip_set_desc['Value']))
    else:
        print("Invalid response for get_ip_set: {}".format(response))
    return ip_map


def update_ip_set(waf_client, **kwargs):
    ip_set_id = kwargs['ip_set_id']
    ip_file = kwargs['ip_file']
    do_delete = kwargs['do_delete']
    ip_map = get_ip_set(waf_client, skip_print=True, **kwargs)
    print("IpSet: Id: {} Current IPs:".format(ip_set_id))
    print(json.dumps(sorted(ip_map.keys())))

    # read file and get list of ips
    if not ip_file or not os.path.isfile(ip_file):
        print("Error: Input data file not found: {}".format(ip_file))
        return False

    ignore_strings = ('#', '192.168.', '10.')
    file_ip_map = {}
    with open(ip_file, 'r') as f:
        for line in f:
            ip = line.strip()
            if not ip or any(ip.startswith(s) for s in ignore_strings):
                continue
            # normalize IPs
            try:
                # normalize IP by using ipaddress module
                # e.g. 115.28.0.3 becomes 115.28.0.3/32, 119.255.16.82/24 becomes 119.255.16.0/24
                ip_network = ipaddress.IPv4Interface(six.text_type(ip)).network
                ip = str(ip_network)
                file_ip_map[ip] = {'Type': 'IPV4', 'Value': ip}
            except ValueError as e:
                print("Skipping invalid IP: {} msg: {}".format(ip, e))

    print("File: {} IPs:".format(ip_file))
    print(json.dumps(sorted(file_ip_map.keys())))

    # diff against current set in the system to create change set
    dict_differ = DictDiffer(file_ip_map, ip_map)

    # show change set to confirm: only support IPV4
    ip_change_set = []
    add_set = dict_differ.added()
    if not do_delete:
        print("\nNot doing delete, skip removing entries.")
    else:
        remove_set = dict_differ.removed()
        for remove_ip in remove_set:
            ip_change_set.append({'Action': 'DELETE',
                                  'IPSetDescriptor': {'Type': 'IPV4', 'Value': remove_ip}
                                  })
    for add_ip in add_set:
        ip_change_set.append({'Action': 'INSERT',
                              'IPSetDescriptor': {'Type': 'IPV4', 'Value': add_ip}
                              })

    print("\nChange Set: (total entries: {})".format(len(ip_change_set)))
    if not ip_change_set:
        print("No changes to apply.")
        return False

    print(json.dumps(ip_change_set, indent=2))
    if ip_change_set and confirm_continue():
        # apply change
        change_token = get_change_token(waf_client)
        response = waf_client.update_ip_set(IPSetId=ip_set_id, ChangeToken=change_token, Updates=ip_change_set)
        if 'ChangeToken' in response:
            print('ChangeToken: {}'.format(response['ChangeToken']))
            # wait a bit to see if finish
            get_change_token_status(waf_client, change_token=response['ChangeToken'], max_retry=50)
        else:
            print("Invalid response for update_ip_set: {}".format(response))


def process(options):
    conf = {
       'access_key_id': Config.S3_SYNCER_PARAMS.AWS_ACCESS_KEY_ID,
       'access_key': Config.S3_SYNCER_PARAMS.AWS_SECRET_ACCESS_KEY,
    }
    waf_client = get_client(conf, 'waf')
    cmd = options.cmd
    print("Cmd: {}".format(cmd))
    try:
        if cmd == 'list_web_acls':
            list_web_acls(waf_client)
        elif cmd == 'list_ip_sets':
            list_ip_sets(waf_client)
        elif cmd == 'get_ip_set':
            get_ip_set(waf_client, ip_set_id=options.id)
        elif cmd == 'get_change_token_status':
            get_change_token_status(waf_client, change_token=options.id)
        elif cmd == 'update_ip_set':
            update_ip_set(waf_client, ip_set_id=options.id, ip_file=options.infile, do_delete=options.do_delete)
        else:
            print("Unsupported command: {}".format(cmd))
    except Exception as e:
        logger.exception(e)
        print(e)

if __name__ == '__main__':
    slogger.setUpLogger('/houzz/c2svc/log', 'waf_utils')
    parser = optparse.OptionParser("usage: %prog [options]")
    parser.add_option('-c', '--cmd', dest='cmd', default='list_web_acls', help='Command to run.')
    parser.add_option('-a', '--acl', dest='webacl', default=None, help='WebACL to work on.')
    parser.add_option('-f', '--infile', dest='infile', default=None, help='Input data file.')
    parser.add_option('-i', '--id', dest='id', default=None, help='Id of object to query.')
    parser.add_option('--do_delete', dest='do_delete', action='store_true', default=False, help='If true, remove entries not defined in file as well')
    (options, args) = parser.parse_args()
    if len(args):
        parser.error('incorrect number of arguments')
    process(options)
