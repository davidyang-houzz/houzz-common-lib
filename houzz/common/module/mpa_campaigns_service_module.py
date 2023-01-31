from __future__ import absolute_import
__author__ = 'jonathan'

import logging
import uuid

from retry import retry
from datetime import timedelta
from twitter.common import app, options

from houzz.common import calendar_utils as cu
from houzz.common.logger import slogger
from houzz.common.calendar_utils import ms_to_ts, get_datetime_from_ts, PST_TZ
from houzz.common.module.env_module import EnvModule
from houzz.common.service_proxy_discoveryless import ServiceProxyDiscoveryless
from houzz.common.thrift_gen.discovery_service import ttypes as discovery_ttypes
from houzz.common.thrift_gen.jukwaa.base import ttypes as base_ttypes
from houzz.common.thrift_gen.mpa_campaigns_service import MPACampaignsService
from houzz.common.thrift_gen.mpa_campaigns_service import ttypes as mpac_ttypes

DEFAULT_CONTEXT = base_ttypes.Context(request=base_ttypes.Request(requestId="batch-job"))
SERVICE_PATH = 'SERVICE_PATH'
DISCOVERY_SOCKET = '/var/lib/haproxy/sl.sock'
LOCAL_PORT = 9011
LOCAL_HOST = 'macbook'
RETRIES = 3

# env config dictionary that differs based on which env
ENV_CONFIG = {
    EnvModule.ENV_DEV: {
        SERVICE_PATH: ''
    },
    EnvModule.ENV_STG: {
        SERVICE_PATH: '/houzz/mpacampaigns/main/providers/staging'
    },
    EnvModule.ENV_PROD: {
        SERVICE_PATH: '/houzz/mpacampaigns/main/providers/prod'
    },
}

# Set this path for prod so that filebeat will pick up the log
if EnvModule().env == 'dev':
    logger_dir = '/var/tmp/product_ads'
else:
    logger_dir = '/home/clipu/c2/log/rq'
logger = slogger.get_stdout_logger(logger_dir, app.name())

def create_context(rid):
    request_id = "batch-job-%s" % rid
    return base_ttypes.Context(request=base_ttypes.Request(requestId=request_id))


class RedisCommandTimeoutException(Exception):
    pass


class MPACampaignsServiceModule(app.Module):
    '''
    Add default dry run option
    '''
    OPTIONS = {
        'force_write': options.Option(
            '-f',
            '--force_write',
            dest='force_write',
            help='Enables writing actions, such as updating campaigns or write to DB.',
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

        self._log = logging.getLogger(self.__class__.__name__)

        self._instance = None

        app.register_module(EnvModule())
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[
                EnvModule.full_class_path(),
            ],
            description="MPA Campaigns Service Module")

    def setup_function(self):
        self._options = app.get_options()

        self._log.info("Options: {}".format(self._options))

        # initialize env here after env module is setup
        self._env = EnvModule().env

        self._instance = ServiceProxyDiscoveryless(
            MPACampaignsService.Client,
            self._fallback_endpoints,
            self._env,
            ENV_CONFIG[EnvModule().env][SERVICE_PATH],
        )

        self._log.info("mpa campaigns endpoints resolved: [%s]", self._instance._endpoints)

    @property
    def connection(self):
        return self._instance

    def _log_if_error(self, method, response):
        if response.status.errorCode != 0:
            logger.info('Error in method %s: error code %s, error %s' % (method, response.status.errorCode, response.status.errorMessage))


    def update_mpa_campaign(self, request):
        request.context = DEFAULT_CONTEXT

        self._log.info('Update mpa campaign request: {}'.format(request))
        if self._options.force_write:
            self._log.info('Updating mpa campaign {}'.format(request.campaignId))
            response = self._instance.updateMPACampaign(request)
            self._log_if_error(self.update_mpa_campaign.__name__, response)
        else:
            self._log.info('Skipping update {} for dry run'.format(request.campaignId))

    def get_campaign_ids_by_user(self, request):
        request.context = DEFAULT_CONTEXT
        self._log.info('Get campaign ids request: {}'.format(request))
        response = self._instance.getCampaignIdsByUser(request)
        self._log_if_error(self.get_campaign_ids_by_user.__name__, response)

        return response

    def write_paced_time(self, request):
        self._log.info('Write paced time request: {}'.format(request))
        if self._options.force_write:
            self._log.info('Writing paced time for campaign {}'.format(request.campaignId))
            response = self._instance.writePacedTime(request)
            self._log_if_error(self.write_paced_time.__name__, response)
        else:
            self._log.info('Skipping paced time write {} for dry run'.format(request.campaignId))

    @retry(RedisCommandTimeoutException, tries=RETRIES, delay=1)
    def get_redis_data_for_day(self, campaign_id, ts, read_type):
        """
        ts must be the beginning of the day in PST Timezone
        """
        redis_request = mpac_ttypes.GetRedisDataRequest(
            campaignId=campaign_id,
            startTs=ts,
            redisReadType=read_type
        )

        res = self._instance.getRedisData(redis_request)
        # return when no error
        self._log_if_error(self.get_redis_data_for_day.__name__, res)
        if res.status.errorCode == 0:
            return res

        if "RedisCommandTimeoutException" in res.status.errorMessage:
            # All retries fail
            error_msg = 'getRedisData failed for campaign [{} {} {}]:{}'.format(
                RETRIES, campaign_id, ts, read_type, res.status.errorMessage
            )
            self._log.error(error_msg)
            raise RedisCommandTimeoutException(error_msg)
        else:
            error_msg = 'getRedisData failed for [{} {} {}]:{}'.format(
                campaign_id, ts, read_type, res.status.errorMessage
            )
            self._log.error(error_msg)
            raise Exception(error_msg)

    def get_clicks_for_day(self, campaign_id, ts):
        return self.get_redis_data_for_day(campaign_id, ts, mpac_ttypes.RedisReadType.CLICKS).count

    def get_impressions_for_day(self, campaign_id, ts):
        return self.get_redis_data_for_day(campaign_id, ts, mpac_ttypes.RedisReadType.IMPS).count

    def get_spend_for_day(self, campaign_id, ts):
        return self.get_redis_data_for_day(campaign_id, ts, mpac_ttypes.RedisReadType.REV).count * 0.01

    def get_spend_for_current_month(self, campaign, current_timestamp):
        '''
        Get spend for previous days from daily metrics endpoint.
        Passing current_timestamp since daily metrics only up to previous day.

        Currency: dollar
        '''

        budget_start = campaign.budgetStartTs
        request_id = str(uuid.uuid4())
        response = self.get_campaign_daily_metrics(
            request_id,
            campaign.campaignId,
            budget_start,
            cu.ts_to_ms(current_timestamp)
        )

        spend_from_metrics = sum([m.spend for m in response.metrics])

        # Get spend for current day from Redis
        day_start_timestamp = cu.datetime_to_ts(cu.get_beginning_of_day(current_timestamp, cu.PST_TZ))
        spend_from_redis = self.get_spend_for_day(campaign.campaignId, day_start_timestamp)

        logger.info(
            "Campaign %s spend: %s from metrics, %s from redis",
            campaign.campaignId,
            spend_from_metrics,
            spend_from_redis
        )

        return spend_from_metrics + spend_from_redis

    def get_mpa_campaign(self, campaign_id):
        request = mpac_ttypes.GetMPACampaignRequest(context=DEFAULT_CONTEXT, campaignId=campaign_id)
        response = self._instance.getMPACampaign(request)
        self._log_if_error(self.get_mpa_campaign.__name__, response)

        return response.campaign

    def get_mpa_campaigns(self):
        request = mpac_ttypes.GetMPACampaignsRequest(context=DEFAULT_CONTEXT)
        response = self._instance.getMPACampaigns(request)
        self._log_if_error(self.get_mpa_campaigns.__name__, response)
        return response.campaigns

    def get_live_mpa_campaigns(self):
        request = mpac_ttypes.GetMPACampaignsRequest(context=DEFAULT_CONTEXT, liveOnly=True)
        response = self._instance.getMPACampaigns(request)
        self._log_if_error(self.get_live_mpa_campaigns.__name__, response)
        return response.campaigns

    def get_mpa_account(self, mpa_advertiser_id):
        request = mpac_ttypes.GetMPAAccountRequest(context=DEFAULT_CONTEXT, mpaAdvertiserId=mpa_advertiser_id)
        response = self._instance.getMPAAccount(request)
        self._log_if_error(self.get_mpa_account.__name__, response)
        return response.account

    def get_mpa_accounts(self, mpa_advertiser_ids):
        request = mpac_ttypes.GetMPAAccountsRequest(context=DEFAULT_CONTEXT, mpaAdvertiserIds=mpa_advertiser_ids)
        response = self._instance.getMPAAccounts(request)
        self._log_if_error(self.get_mpa_accounts.__name__, response)

        return response.accounts

    def get_campaign_product_counts(self, campaign_ids):
        request = mpac_ttypes.GetCampaignProductCountsRequest(context=DEFAULT_CONTEXT, campaignIds=campaign_ids)
        response = self._instance.getCampaignProductCounts(request)
        self._log_if_error(self.get_campaign_product_counts.__name__, response)

        return response.productCounts

    def get_campaigns_with_invalid_products(self, campaigns, current_ts):
        def is_recent_campaign(campaign, current_date):
            DAYS_BEFORE_CHECKING = 2
            campaign_start_date = get_datetime_from_ts(ms_to_ts(campaign.startCampaignTs), PST_TZ)

            if (current_date - campaign_start_date) <= timedelta(days=DAYS_BEFORE_CHECKING):
                logger.info('Skipping campaign id %d, too recent to check' % campaign.campaignId)
                return True
            else:
                return False

        result = []

        current_date = get_datetime_from_ts(current_ts, PST_TZ)

        filters = (
            lambda campaign: campaign.campaignType == mpac_ttypes.CampaignTypes.WHITELIST,
            lambda campaign: not is_recent_campaign(campaign, current_date))

        relevant_campaigns = [x for x in campaigns if all(f(x) for f in filters)]

        id_to_campaign = {campaign.campaignId: campaign for campaign in relevant_campaigns}
        campaign_product_counts = self.get_campaign_product_counts(list(id_to_campaign.keys()))

        for product_count in campaign_product_counts:
            logger.info('Campaign id %d has %d active valid bids, %d invalid bids' %
                        (product_count.campaignId,
                         product_count.activeValidBidProductsCount or 0,
                         product_count.activeInvalidBidProductsCount or 0))

            if product_count.activeValidBidProductsCount == 0:
                campaign = id_to_campaign[product_count.campaignId]
                logger.info('Campaign id %d has 0 active valid bids' % campaign.campaignId)
                result.append((campaign, product_count))

        return result

    def get_product_ads_feeds(self, vendor_id, start_ts, end_ts):
        request = mpac_ttypes.GetProductAdsFeedsRequest(context=DEFAULT_CONTEXT,
                                                        oldestUpdatedTs=start_ts,
                                                        newestUpdatedTs=end_ts,
                                                        vendorId=vendor_id)
        response = self._instance.getProductAdsFeeds(request)
        self._log_if_error(self.get_product_ads_feeds.__name__, response)

        return response.productAdsFeeds

    '''
    system_metrics_frequency: SystemMetricsFrequency in ttypes.py
    start_ts: time_stamp in s 
    end_ts: time_stamp in s 
    context: request context
    '''

    def get_system_metrics(self, system_metrics_frequency, start_ts=None, end_ts=None, context=None):
        response = None
        start_ts = cu.ts_to_ms(start_ts) if start_ts else start_ts
        end_ts = cu.ts_to_ms(end_ts) if end_ts else end_ts
        context = context if context else DEFAULT_CONTEXT
        request = mpac_ttypes.GetSystemMetricsRequest(
            context=context,
            startTs=start_ts,
            endTs=end_ts,
            systemMetricsFrequency=system_metrics_frequency
        )

        self._log.info('Getting System metrics')

        response = self._instance.getSystemMetrics(request)
        self._log_if_error(self.get_system_metrics.__name__, response)

        return response

    def get_campaign_monthly_metrics(self, request_id, campaign_id, months):
        request = mpac_ttypes.GetCampaignMonthlyMetricsRequest(context=create_context(request_id),
                                                               campaignId=campaign_id, months=months)
        response = self._instance.getCampaignMonthlyMetrics(request)
        self._log_if_error(self.get_campaign_monthly_metrics.__name__, response)

        return response

    def get_campaign_daily_metrics(self, request_id, campaign_id, start_ts, end_ts):
        request = mpac_ttypes.GetCampaignDailyMetricsRequest(
            context=create_context(request_id),
            campaignId=campaign_id,
            startDate=start_ts,
            endDate=end_ts
        )
        response = self._instance.getCampaignDailyMetrics(request)
        self._log_if_error(self.get_campaign_daily_metrics.__name__, response)

        return response

    def get_product_bids(self, campaign_id, vendor_id, start_id, batch_size):
        request = mpac_ttypes.GetProductBidsRequest(
            context=create_context('batch-script'),
            campaignId=campaign_id,
            vendorId=vendor_id,
            batchSize=batch_size,
            startVendorListingId=start_id
        )

        response = self._instance.getProductBids(request)
        self._log_if_error(self.get_campaign_daily_metrics.__name__, response)
        return response.productBidsProducts

    def update_product_bids(self, vendor_id, bids):
        request = mpac_ttypes.UpdateProductBidsRequest(
            context=create_context('batch-script'),
            vendorId=vendor_id,
            productBids=bids
        )

        if self._options.force_write:
            self._log.info('updating product bids')
            response = self._instance.updateProductBids(request)
            self._log_if_error(self.update_product_bids.__name__, response)
        else:
            self._log.info('Skipping update for dry run')

    def write_campaign_monthly_credits_metrics(self, request):
        request.requestId = "batch-job"
        self._log.info('Monthly credits metrics for campaign {}: {}'.format(request.campaignId, request))
        if self._options.force_write:
            self._log.info('Writing monthly credits metrics')
            response = self._instance.writeCampaignMonthlyCreditsMetrics(request)
            self._log_if_error(self.write_campaign_monthly_credits_metrics.__name__, response)
        else:
            self._log.info('Skipping update for dry run')

    def write_campaign_monthly_sales_metrics(self, request):
        request.requestId = "batch-job"
        self._log.info('Monthly sales metrics for campaign {}: {}'.format(request.campaignId, request))
        if self._options.force_write:
            self._log.info('Writing monthly sales metrics')
            response = self._instance.writeCampaignMonthlySalesMetrics(request)
            self._log_if_error(self.write_campaign_monthly_sales_metrics.__name__, response)
        else:
            self._log.info('Skipping update for dry run')

    def write_daily_metrics(self, request):

        self._log.info('Daily Metrics for campaign {}: {}'.format(request.campaignId, request))
        if self._options.force_write:
            self._log.info('Writing daily metrics')
            response = self._instance.writeDailyMetrics(request)
            self._log_if_error(self.write_daily_metrics.__name__, response)
        else:
            self._log.info('Skipping update for dry run')

