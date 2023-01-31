from __future__ import absolute_import
__author__ = 'gabriel'

from twitter.common import app, options

from houzz.common.logger import slogger
from houzz.common.module.env_module import EnvModule
from houzz.common.service_proxy_discoveryless import ServiceProxyDiscoveryless
from houzz.common.thrift_gen.discovery_service import ttypes as discovery_ttypes
from houzz.common.thrift_gen.jukwaa.base import ttypes as base_ttypes
from houzz.common.thrift_gen.rewards_service import RewardsService
from houzz.common.thrift_gen.rewards_service import ttypes as rewards_ttypes

DEFAULT_CONTEXT = base_ttypes.Context(request=base_ttypes.Request(requestId="batch-job"))
SERVICE_PATH = 'SERVICE_PATH'
DISCOVERY_SOCKET = '/var/lib/haproxy/sl.sock'
LOCAL_PORT = 9015
LOCAL_HOST = 'macbook'
RETRIES = 3

# env config dictionary that differs based on which env
ENV_CONFIG = {
    EnvModule.ENV_DEV: {
        SERVICE_PATH: ''
    },
    EnvModule.ENV_STG: {
        SERVICE_PATH: '/houzz/rewards/main/providers/staging'
    },
    EnvModule.ENV_PROD: {
        SERVICE_PATH: '/houzz/rewards/main/providers/prod'
    },
}

logger = slogger.get_stdout_logger('/var/tmp/rewards', app.name())


def create_context(rid):
    request_id = "batch-job-%s" % rid
    return base_ttypes.Context(request=base_ttypes.Request(requestId=request_id))


def _log_if_error(method, response):
    if response.status.errorCode != 0:
        logger.info('Error in method %s: error code %s, error %s' % (method, response.status.errorCode, response.status.errorMessage))


class RewardsServiceModule(app.Module):
    '''
    Add default dry run option
    '''
    OPTIONS = {
        'force_write': options.Option(
            '-f',
            '--force_write',
            dest='force_write',
            help='Enables writing actions, such as write to DB.',
            action='store_true',
            default=False
        )
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        self._endpoints = None

        self._fallback_endpoints = [discovery_ttypes.Endpoint(
            host=LOCAL_HOST,
            port=LOCAL_PORT
        )]

        self._env = None

        self._options = None

        self._instance = None

        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[
                EnvModule.full_class_path(),
            ],
            description="Rewards Service Module")

    def setup_function(self):
        self._options = app.get_options()

        logger.info("Options: {}".format(self._options))

        # initialize env here after env module is setup
        self._env = EnvModule().env

        self._instance = ServiceProxyDiscoveryless(
            RewardsService.Client,
            self._fallback_endpoints,
            self._env,
            ENV_CONFIG[EnvModule().env][SERVICE_PATH],
        )

        logger.info("rewards endpoints resolved: [%s]", self._instance._endpoints)

    @property
    def connection(self):
        return self._instance

    def get_users_in_program(self, program):
        request = rewards_ttypes.GetUsersInProgramRequest(context=DEFAULT_CONTEXT, program=program)
        response = self._instance.getUsersInProgram(request)
        _log_if_error(self.get_users_in_program.__name__, response)
        return response.userIds

    def update_tier(self, user_id):
        request = rewards_ttypes.UpdateTierRequest(
            context=DEFAULT_CONTEXT,
            userId=user_id
        )
        logger.info('Update tier for user {}: {}'.format(request.userId, request))
        if self._options.force_write:
            logger.info('Updating tier')
            response = self._instance.updateTier(request)
            _log_if_error(self.update_tier.__name__, response)
        else:
            logger.info('Skipping update for dry run')
