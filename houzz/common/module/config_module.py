from __future__ import absolute_import
import os

from twitter.common import app, options

from houzz.common.config import load_config, ConfigDict
from houzz.common.module.env_module import EnvModule


class ConfigModule(app.Module):

    _DEFAULT_PATH = [
        '/home/clipu/c2/config/config.yaml',
        '/home/clipu/c2svc/python/houzz/config/bootstrap.yaml',
        '/home/clipu/c2svc/python/houzz/config/config.yaml',
        '/houzz/c2svc/src/python/houzz/config/config.yaml',
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '../../config/config.yaml'
        ),  # use relative path for unittesting purpose
    ]

    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'conf_file': options.Option(
            '--conf_file',
            dest='conf_file',
            action='append',
            default=_DEFAULT_PATH,
            help='configurations for yaml'),
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(EnvModule())
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[EnvModule.full_class_path()],
            description="Configuration Module")
        self._config = None

    @property
    def config(self):
        return self._config

    @property
    def app_config(self):
        options = app.get_options()
        if hasattr(options, 'section') and options.section:
            return self.config.get(options.section, ConfigDict())
        else:
            return self.env_config

    @property
    def env_config(self):
        name = app.name()
        env = EnvModule().env
        config_section = ("%s_%s" % (env, name)).upper()
        return self.config.get(config_section, ConfigDict())

    def get_config(self, section):
        env = EnvModule().env
        config_section = ("%s_%s" % (env, section)).upper()
        config = self.config.get(config_section, ConfigDict())
        if not config:
            config = self.config.get(section.upper(), ConfigDict())
        return config

    def setup_function(self):
        options = app.get_options()
        self._config = load_config(options.conf_file)
