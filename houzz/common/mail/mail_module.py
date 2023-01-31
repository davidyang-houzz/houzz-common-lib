from __future__ import absolute_import
__author__ = 'jasonl'

import bisect
import random

from twitter.common import app
from houzz.common.module.config_module import ConfigModule


EPN_MAIL_TYPE_BAD_DOMAIN = 1
EPN_MAIL_TYPE_BULK_ONE_OFF = 2
EPN_MAIL_TYPE_BULK_REGULAR = 3
EPN_MAIL_TYPE_TRIGGERED = 4

ALL_EPN_MAIL_TYPES = {
    EPN_MAIL_TYPE_BAD_DOMAIN: "EPN_MAIL_BAD_DOMAIN_CONFIG",
    EPN_MAIL_TYPE_BULK_ONE_OFF: "EPN_MAIL_BULK_ONE_OFF_CONFIG",
    EPN_MAIL_TYPE_BULK_REGULAR: "EPN_MAIL_BULK_REGULAR_CONFIG",
    EPN_MAIL_TYPE_TRIGGERED: "EPN_MAIL_TRIGGERED_CONFIG"
}


class MailHost(object):

    def __init__(self, host, port, weight, username, password, retries, timeout):
        self._host = host
        self._port = port
        self._weight = weight
        self._username = username
        self._password = password
        self._retries = retries
        self._timeout = timeout

    @property
    def port(self):
        return self._port

    @property
    def host(self):
        return self._host

    @property
    def retries(self):
        return self._retries

    @property
    def password(self):
        return self._password

    @property
    def timeout(self):
        return self._timeout

    @property
    def username(self):
        return self._username

    @property
    def weight(self):
        return self._weight


class MailModule(app.Module):

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        dependencies = [ConfigModule.full_class_path()]
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=dependencies,
            description="Mail Module")

        self._mail_config = {}

    @staticmethod
    def load_vendor_config(config_name):
        hosts_config = ConfigModule().get_config(config_name)
        if not hosts_config:
            raise Exception("unable to load mail hosts config from section %s" % config_name)
        mail_hosts_dict = hosts_config.get("MAIL_HOSTS", None)
        if not mail_hosts_dict:
            raise Exception("unable to load MAIL_HOSTS from section %s" % config_name)
        port = hosts_config.get("MAIL_PORT", 25)
        username = hosts_config.get("MAIL_USER", "")
        password = hosts_config.get("MAIL_PASSWORD", "")
        if not username or not password:
            raise Exception("unable to mail host username and password defined in the section %s" % config_name)
        retries = hosts_config.get("NUMBER_OF_RETRIES", 3)
        timeout = hosts_config.get("MAIL_TIMEOUT", 3)
        mail_hosts_list = []
        prev = 0
        for host, weight in mail_hosts_dict.items():
            weight += prev
            mail_host = MailHost(host=host, port=port, weight=weight, username=username, password=password, retries=retries, timeout=timeout)
            mail_hosts_list.append(mail_host)
            prev = weight
        return mail_hosts_list

    def setup_function(self):
        for mail_type, config_name in ALL_EPN_MAIL_TYPES.items():
            type_config = ConfigModule().get_config(config_name)
            mail_hosts_weight_config = type_config.get("MAIL_HOSTS_CONFIG_WEIGHT", None)
            if not mail_hosts_weight_config:
                raise Exception("unable to load config for mail type %s" % mail_type)
            prev = 0
            mail_hosts = []
            for vendor_config_name, weight in mail_hosts_weight_config.items():
                weight += prev
                vendor_hosts = self.load_vendor_config(vendor_config_name)
                mail_hosts.append((vendor_hosts, weight))
                prev = weight
            self._mail_config[mail_type] = mail_hosts

    def get_mail_host(self, mail_type):
        mail_hosts = self._mail_config[mail_type]
        total_weight = mail_hosts[-1][1]
        r = random.random() * total_weight
        keys = [e[1] for e in mail_hosts]
        vendor_hosts = mail_hosts[bisect.bisect_right(keys, r)][0]
        vendor_total_weight = vendor_hosts[-1].weight
        r = random.random() * vendor_total_weight
        keys = [e.weight for e in vendor_hosts]
        host = vendor_hosts[bisect.bisect_right(keys, r)]
        return host
