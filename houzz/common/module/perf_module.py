#!/usr/bin/env python
from __future__ import absolute_import
import logging

from twitter.common import app, options
from twitter.common.app.module import AppModule

from houzz.common.logger.perf_monitor import PerfMonitor
from houzz.common.logger.perf_monitor import get_perf_count
from houzz.common.module.config_module import ConfigModule
from houzz.common.module.metric_module import MetricModule

logger = logging.getLogger(__name__)


class PerfModule(app.Module):

    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'perf_host': options.Option(
            '--perf-host', dest='perf_host',
            metavar='PERFORMANCE_HOST',
            help='perf host'),
        'perf_port': options.Option(
            '--perf-port', dest='perf_port',
            metavar='PERFORMANCE_PORT',
            help='perf port'),
        'perf_optional': options.Option(
            '--perf_optional', dest='perf_optional',
            action='store_true',
            help='PerfModule optional, ignore if not loaded (default: false)'),
        'only_metric': options.Option(
            '--only_metric', dest='only_metric',
            action='store_true',
            help='only using metric not ganglia')
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(ConfigModule())
        app.register_module(MetricModule())
        dependencies = [
            ConfigModule.full_class_path(),
            MetricModule.full_class_path()
        ]
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=dependencies,
            description="Performance Module")
        self._perf_monitor = None
        self._metric_module = MetricModule()

    @staticmethod
    def get_module_config():
        modules = AppModule.module_registry()
        return modules[ConfigModule.full_class_path()].app_config

    @property
    def perf_instance(self):
        return self._perf_monitor.get_instance()

    @staticmethod
    def get_perf_count(name):
        return get_perf_count(name)

    def setup_function(self):
        options = app.get_options()
        try:
            config = self.get_module_config()
            if config:
                perf_host = options.perf_host or config.get('LOGGER_HOST')
                perf_port = options.perf_port or config.get('LOGGER_PORT')
            else:
                perf_host = options.perf_host
                perf_port = options.perf_port
            if options.only_metric:
                perf_host = None
                perf_port = None
            self._perf_monitor = PerfMonitor.set_instance(
                host=perf_host,
                port=perf_port,
                metric_client=self._metric_module.metric
            )
        except Exception as ex:
            if options.perf_optional:
                logger.warn(
                    'PerfModule failed to load, ignoring: %s' % (ex, ))
            else:
                logger.exception('PerfModule failed to load, freaking out')
                raise
