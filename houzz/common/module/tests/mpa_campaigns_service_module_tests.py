'''
RUN:
./pants test src/python/houzz/common/module/tests: -- -k MPACampaignsServiceModuleTest
'''
from __future__ import absolute_import
from collections import namedtuple
from houzz.common.thrift_gen.mpa_campaigns_service import ttypes

from houzz.common.calendar_utils import ts_to_ms

__author__ = 'yuwei'

import unittest
from mock import MagicMock, patch, Mock
from houzz.common.module.mpa_campaigns_service_module import MPACampaignsServiceModule,RedisCommandTimeoutException
from houzz.common.thrift_gen.mpa_campaigns_service import ttypes as mpac_ttypes

class MPACampaignsServiceModuleTest(unittest.TestCase):
    def setUp(self):
        module = MPACampaignsServiceModule()

        # Stash the ori. get_redis_data_for_day for test cases that need the
        # real function.
        # https://stackoverflow.com/questions/11746431/any-way-to-reset-a-mocked-method-to-its-original-state-python-mock-mock-1-0
        self.original_get_redis_data_for_day = module.get_redis_data_for_day
        # Stop patches
        self.addCleanup(patch.stopall)

    def tearDown(self):
        module = MPACampaignsServiceModule()
        # Pop the ori. get_redis_data_for_day whenever the test case finishes.
        module.get_redis_data_for_day = self.original_get_redis_data_for_day

    def test_get_spend_for_current_month_rev_from_redis(self):
        module = MPACampaignsServiceModule()

        # spend from redis
        module.get_spend_for_day = MagicMock()
        module.get_spend_for_day.return_value = 0.50

        # spend from metrics
        mock_metrics_list = [
            ttypes.CampaignMetrics(spend=2.0),
            ttypes.CampaignMetrics(spend=1.5),
            ttypes.CampaignMetrics(spend=0.89),
            ttypes.CampaignMetrics(spend=15.01),
            ttypes.CampaignMetrics(spend=3.5)
        ]
        module.get_campaign_daily_metrics = MagicMock()
        module.get_campaign_daily_metrics.return_value = MagicMock(
            status=MagicMock(errorCode=0),
            metrics=mock_metrics_list
        )

        mock_campaign = MagicMock(professionalId=1, budgetStartTs=1554102000)

        res = module.get_spend_for_current_month(mock_campaign, 1559631600)

        self.assertEqual(res, 23.4)

    def test_update_campaigns_force_write_flag(self):
        module = MPACampaignsServiceModule()
        module._instance = MagicMock()
        mockRequest = MagicMock(campaignId=999)

        # force_write = False
        module._options = MagicMock(force_write=False)
        module.update_mpa_campaign(mockRequest)
        module._instance.updateMPACampaign.assert_not_called()

        # force_write = True
        module._options.configure_mock(**{'force_write': True})
        module._instance.updateMPACampaign.return_value = Mock(status=Mock(errorCode=0))
        # print module._instance.updateCampaign
        module.update_mpa_campaign(mockRequest)
        module._instance.updateMPACampaign.assert_called_once()

    def test_get_campaigns_with_invalid_products(self):
        TEST_CURRENT_TIME_SEC = float(1578425117)
        TEST_CURRENT_TIME_MS = ts_to_ms(TEST_CURRENT_TIME_SEC)

        product_count = namedtuple('product_count',
                                   'campaignId activeValidBidProductsCount activeInvalidBidProductsCount')
        campaign = namedtuple('campaign', 'campaignId startCampaignTs campaignType')

        def create_product_count(campaign_id, active_valid_bid_products_count, active_invalid_bid_products_count):
            return product_count(
                campaignId=campaign_id,
                activeValidBidProductsCount=active_valid_bid_products_count,
                activeInvalidBidProductsCount=active_invalid_bid_products_count)

        def create_campaign(campaign_id, start_campaign_ts, campaign_type):
            return campaign(campaignId=campaign_id, startCampaignTs=start_campaign_ts, campaignType=campaign_type)

        test_request = [
            create_campaign(1, 0, mpac_ttypes.CampaignTypes.WHITELIST),
            create_campaign(2, 0, mpac_ttypes.CampaignTypes.WHITELIST),
            create_campaign(3, 0, mpac_ttypes.CampaignTypes.WHITELIST),
            create_campaign(4, 0, mpac_ttypes.CampaignTypes.WHITELIST),
            create_campaign(5, 0, mpac_ttypes.CampaignTypes.WHITELIST),
            create_campaign(6, 0, "non-whitelist"),
            create_campaign(7, TEST_CURRENT_TIME_MS, mpac_ttypes.CampaignTypes.WHITELIST)
        ]

        test_product_counts_response = [
            create_product_count(1, 100, 2),
            create_product_count(2, 100, 2),
            create_product_count(3, 0, 2),
            create_product_count(4, 100, 2),
            create_product_count(5, 0, 2)
        ]

        module = MPACampaignsServiceModule()
        module._instance = Mock()

        module._instance.getCampaignProductCounts.return_value = Mock(productCounts=test_product_counts_response)

        response = module.get_campaigns_with_invalid_products(test_request, TEST_CURRENT_TIME_SEC)

        args, kwargs = module._instance.getCampaignProductCounts.call_args

        self.assertEqual(len(args[0].campaignIds), 5)
        self.assertEqual(len(response), 2)
        self.assertEqual(response[0][0], test_request[2])
        self.assertEqual(response[0][1], test_product_counts_response[2])
        self.assertEqual(response[1][0], test_request[4])
        self.assertEqual(response[1][1], test_product_counts_response[4])


def test_get_redis_data_for_day_retries(self):
        mockTimeoutResponse = Mock(
            status=Mock(
                errorCode=-12,
                errorMessage="io.lettuce.core.RedisCommandTimeoutException: Command timed out after 10 second(s)"
            )
        )
        mockOtherExceptionResponse = Mock(
            status=Mock(
                errorCode=-11,
                errorMessage="Other kind of exception"
            )
        )
        mockNormalResponse = Mock(
            status=Mock(
                errorCode=0
            ),
            count=88.88
        )
        module = MPACampaignsServiceModule()
        module._instance = Mock()

        # Case 1: 2nd response contains other exception, raised
        module._instance.getRedisData = Mock(side_effect=[mockTimeoutResponse, mockOtherExceptionResponse])
        self.assertRaises(Exception, module.get_redis_data_for_day, 1, 1, 3)

        # Case 2: all retries fail, raises timeout exception anyways
        module._instance.getRedisData = Mock(side_effect=[mockTimeoutResponse, mockTimeoutResponse, mockTimeoutResponse, mockNormalResponse])
        self.assertRaises(RedisCommandTimeoutException, module.get_redis_data_for_day, 1, 1, 3)

        # Case 3: the 3rd response contains correct data, returned.
        module._instance.getRedisData = Mock(side_effect=[mockTimeoutResponse, mockTimeoutResponse, mockNormalResponse])
        self.assertEqual(module.get_redis_data_for_day(1, 1, 3).count, 88.88)

        # Case 4: 3rd response contains other exception, raised
        module._instance.getRedisData = Mock(side_effect=[mockTimeoutResponse, mockTimeoutResponse, mockOtherExceptionResponse])
        with self.assertRaises(Exception) as cm:
            # uses context manager to store the caught exception to do
            # additional check.
            module.get_redis_data_for_day(1, 1, 3)
        self.assertIsInstance(cm.exception, Exception)
        self.assertNotIsInstance(cm.exception, RedisCommandTimeoutException)
