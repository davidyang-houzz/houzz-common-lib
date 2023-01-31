#!/usr/bin/env python
from __future__ import absolute_import
from twitter.common import app, options
from twitter.common.app.module import AppModule
from twitter.common.log import LogOptions
from twitter.common.log.formatters import glog
from twitter.common.log.initialize import init, ProxyFormatter

from houzz.common.logger.slogger import setUpLogger
from houzz.common.module.config_module import ConfigModule


class LoggingModule(app.Module):
    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'loglevel': options.Option(
            '--loglevel', dest='loglevel',
            metavar='LOGLEVEL',
            help='log flag'),
        'logdir': options.Option(
            '--logdir', dest='logdir',
            metavar='LOGDIR',
            help='log DIR'),
        'logcount': options.Option(
            '--logcount', dest='logcount',
            metavar='LOGCOUNT',
            help='log count'),
        'logname': options.Option(
            '--logname', dest='logname',
            metavar='LOGNAME',
            help='logger name'),
        'logger_host': options.Option(
            '--loghost', dest='logger_host',
            metavar='LOGHOST',
            help='log host'),
        'logger_port': options.Option(
            '--logport', dest='logger_port',
            metavar='LOGPORT',
            help='log port')
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(ConfigModule())
        dependencies = [ConfigModule.full_class_path()]
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=dependencies,
            description="HZLogging Module")
        LogOptions.disable_disk_logging()

    @staticmethod
    def get_module_config():
        modules = AppModule.module_registry()
        return modules[ConfigModule.full_class_path()].app_config

    def setup_function(self):
        init()
        options = app.get_options()
        config = self.get_module_config()
        if config:
            logdir = options.logdir or config.get('LOG_DIR', '.')
            loglevel = options.loglevel or config.get('LOG_LEVEL', 'DEBUG')
            logcount = options.logcount or config.get('LOG_COUNT')
            logger_host = options.logger_host or config.get('LOGGER_HOST')
            logger_port = options.logger_port or config.get('LOGGER_PORT')
            logname = options.logname or config.get('LOG_NAME', app.name())
        else:
            logdir = options.logdir
            loglevel = options.loglevel
            logcount = options.logcount
            logger_host = options.logger_host
            logger_port = options.logger_port
            logname = options.logname or app.name()

        if not logger_host or (logger_host and logger_host.lower() in ('none', 'null', )):
            logger_host = logger_port = None

        setUpLogger(
            host=logger_host,
            port=logger_port,
            log_dir=logdir,
            log_count=logcount,
            logger_name=logname,
            level=loglevel,
            formatter=ProxyFormatter(lambda: glog.GlogFormatter.SCHEME))
