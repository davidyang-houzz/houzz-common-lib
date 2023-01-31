from __future__ import absolute_import
import logging

from twitter.common import app, options


class EnvModule(app.Module):

    ENV_PROD = 'prod'
    ENV_STG = 'stg'
    ENV_DEV = 'dev'
    ENV_AWS = 'aws'

    ENV_STG_STGHOUZZ = 'stghouzz'
    ENV_STG_HOUZZ2 = 'houzz2'

    ALL_ENVS_NO_DUAL_STG = [
        ENV_DEV, ENV_STG, ENV_PROD, ENV_AWS, ]
    ALL_ENVS_DUAL_STG = [
        ENV_DEV, ENV_STG_STGHOUZZ, ENV_STG_HOUZZ2, ENV_PROD, ENV_AWS, ]

    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'env': options.Option(
            '--env', dest='env', type='string', default=None,
            help='running environment: prod, staging or dev (default: dev)'),
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self, support_dual_stg=False):
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            description="Environment Module")
        self._support_dual_stg = support_dual_stg
        self._all_envs = self.ALL_ENVS_DUAL_STG \
            if self._support_dual_stg else self.ALL_ENVS_NO_DUAL_STG
        self._env = self.ENV_DEV
        self._env_specified = False
        self._log = logging.getLogger(self.__class__.__name__)

    @property
    def env(self):
        return self._env

    def is_aws(self):
        return self._env == self.ENV_AWS

    def is_prod(self):
        return self._env == self.ENV_PROD

    def is_staging(self):
        if self._support_dual_stg:
            return self._env in [self.ENV_STG_STGHOUZZ, self.ENV_STG_HOUZZ2, ]
        else:
            return self._env == self.ENV_STG

    def is_dev(self):
        return self._env == self.ENV_DEV

    def setup_function(self):
        options = app.get_options()
        self._env = options.env
        if self._env:
            self._env_specified = True
        else:
            self._env = self.ENV_DEV
        if self._env not in self._all_envs:
            raise Exception("--env option %s is not recognized" % self._env)
        self._log.debug(
            "Starting application [%s] in [%s] env", app.name(), self._env)

    def set_default(self, env_default):
        if not self._env_specified and env_default:
            self._env = env_default
