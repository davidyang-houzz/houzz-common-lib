from __future__ import absolute_import
import logging
import os

from twitter.common import app, options

from houzz.common.module.config_module import ConfigModule
from houzz.common.metric.client import init_metric_client

logger = logging.getLogger(__name__)


class MetricModule(app.Module):

    """
    Binds this application with The metric module
    """

    _DEFAULT_METRIC_UDS = '/home/clipu/c2svc/metric_agent_server.socket'

    OPTIONS = {
        'disable_metrics': options.Option(
            '--disable_metrics',
            dest='disable_metrics',
            action="store_true",
            help='Disable metrics collection'),
        'metric_uds': options.Option(
            '--metric_uds',
            dest='metric_uds',
            default=None,
            help='the socket path to talk with metric agent server'),
        'metrics_optional': options.Option(
            '--metrics_optional', dest='metrics_optional',
            action='store_true',
            help='Metrics optional, ignore if not loaded (default: false)'),
        'metric_tags': options.Option(
            '--metric_tags', dest='metric_tags',
            type='string',
            default=None,
            help='Metrics default tags as "key1:val1;key2:val2", ' +
                 'e.g. env=stghouzz')
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(ConfigModule())
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[ConfigModule.full_class_path()],
            description="Metric Module")
        self._metric_client = None
        # HACK: to support MetricModule being used in multi-processed env.
        # The MetricModule may be created in the parent process. The child
        # process need to re-initialize it make sure all necessary threads
        # are properly created and started.
        self.metric_client_inited = {}  # process id => bool

    @property
    def metric(self):
        return self.create_metric_client()

    def create_metric_client(self):
        pid = os.getpid()
        if pid not in self.metric_client_inited:
            self._metric_client = init_metric_client(
                disable_metrics=self.disable_metrics,
                collector_uds=self.metric_uds,
                service_name=self.service_name,
                tags=self.tags
            )
            self.metric_client_inited[pid] = 1
        return self._metric_client

    def setup_function(self):
        options = app.get_options()
        # HACK: user may disable perf / metric in service_module, which should
        # automatically set disable_metrics flag here.
        try:
            if options.svc_dis_perf:
                logger.info('Metrics module is disabled by the ServiceModule.')
                options.disable_metrics = True
        except AttributeError:
            # svc_dis_perf is unknown because MetricModule is not included from ServiceModule.
            pass

        try:
            config = ConfigModule().env_config
            default_tags = {}
            config_tags = config.get('METRIC_TAGS', None)
            if config_tags:
                default_tags.update(config_tags)
            if options.metric_tags:
                    default_tags.update(
                        dict([tuple(kv.split(':')) for kv in
                             options.metric_tags.split(';')]))
            self.metric_uds = options.metric_uds or config.get("METRIC_UDS") or self._DEFAULT_METRIC_UDS
            if options.disable_metrics:
                logger.warn('MetricModule is disabled. All metrics will be dropped.')
            else:
                logger.info(
                    'MetricModule initializing, metric_uds: %s, default_tags: %s' %
                    (self.metric_uds, default_tags, ))

            # memorize the options in case we need to re-init in multiprocess env
            self.disable_metrics = options.disable_metrics
            self.service_name = app.name()
            self.tags = default_tags or None

            self.create_metric_client()

        except Exception as ex:
            if options.metrics_optional:
                logger.warn(
                    'MetricModule failed to load, ignoring: %s' % (ex, ))
            else:
                logger.exception('MetricModule failed to load, freaking out')
                raise
