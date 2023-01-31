from __future__ import absolute_import
import six
__author__ = 'jasonl'

import logging

from cassandra.cqlengine import connection
from twitter.common import app, options
from houzz.common.module.config_module import ConfigModule


class CassandraModule(app.Module):

    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'cassandra_hosts': options.Option(
            '--cassandra_hosts',
            dest='cassandra_hosts',
            action='append',
            help="the cassandra hosts",
            default=None
        ),
        'cassandra_port': options.Option(
            '--cassandra_port',
            dest='cassandra_port',
            action='append',
            help="the cassandra port",
            default=None
        ),
        'disable_cassandra': options.Option(
            '--disable_cassandra',
            dest='disable_cassandra',
            action='append',
            help="disable cassandra",
            default=False
        )
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[ConfigModule.full_class_path()],
            description="Cassandra Cluster Module")
        self._instance = None
        self._log = logging.getLogger(self.__class__.__name__)

    def setup_function(self, instance_options=None):
        if not instance_options:
            app_options = app.get_options()
            app_config = ConfigModule().app_config
            disable_cassandra = app_options.disable_cassandra or app_config.get("DISABLE_CASSANDRA")
            if disable_cassandra:
                self._log.warning("cassandra module is disabled")
                return
            hosts = app_options.cassandra_hosts or app_config.get("CASSANDRA_HOSTS")
            if hosts and isinstance(hosts, six.string_types):
                hosts = hosts.split(",")
            if not hosts:
                raise Exception("cassandra_hosts is not defined in the config or app options")
            port = app_options.cassandra_port or app_config.get("CASSANDRA_PORT")
            if not port:
                raise Exception("cassandra_port is not defined in the config or app options")

            # (TODO Jasonl) need to add more configs
            cluster_options = {
                "port": port,
                "max_schema_agreement_wait": 0,
            }
            instance_options = {
                'name': app.name(),
                'hosts': hosts,
                'cluster_options': cluster_options,
                'default': True,
            }
        self._options = instance_options
        self._instance = connection.register_connection(**self._options)

    @property
    def connection(self):
        return self._instance

    @property
    def connection_name(self):
        return self._instance.name if self._instance else None

    @property
    def cluster(self):
        return self._instance.cluster if self._instance else None

    @property
    def session(self):
        return self._instance.session if self._instance else None
