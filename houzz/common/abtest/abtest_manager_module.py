from __future__ import absolute_import
__author__ = 'menglei'

import logging
import hashlib
from twitter.common import app
from houzz.common.module.config_module import ConfigModule
from houzz.common.module.abtest_service_module import ABTestServiceModule
from houzz.common.module.env_module import EnvModule

from houzz.common.thrift_gen.tm_service.ttypes import *
from houzz.common.logger.common_logger import CommonLogger

from houzz.common.abtest.abtest_config import ABTestConfig
from houzz.common.abtest.abtest_seed import ABTestSeed


class ABTestManager:
    """
    ABTest Manager for registering ABTest
    Currently, only batch ABTest is supported
    """
    def __init__(self,
                 config_manager,
                 user_id=None,
                 visitor_id=None,
                 idfa=None,
                 url=None,
                 object_id=None):
        if not config_manager:
            raise Exception("Please provide A/B Test config manager")
        self._abtest_config = config_manager
        self._user_id = user_id
        self._visitor_id = visitor_id
        self._idfa = idfa
        self._url = url
        self.object_id = object_id
        self._logger = logging.getLogger(__name__)
        self._common_logger = CommonLogger(internal_logger=self._logger)
        self._test_key_value_map = dict()

    def refresh(self,
                user_id=None,
                visitor_id=None,
                idfa=None,
                url=None,
                object_id=None):
        """
        Re-initialize ABTest Manager's session info
        :param user_id:
        :param visitor_id:
        :param idfa:
        :param url:
        :param object_id:
        :return:
        """
        self._common_logger.refresh_test_selection()
        self._test_key_value_map = dict()
        self._user_id = user_id
        self._visitor_id = visitor_id
        self._idfa = idfa
        self._url = url
        self.object_id = object_id

    def register_abtest_key(self,
                          key_name,
                          force_user_id=None,
                          force_visitor_id=None,
                          force_idfa=None,
                          force_url=None,
                          force_object_id=None):
        """
        :param key_name:
        :param force_user_id: optional if specified, this Houzz user_id will be used to calculate the test bucket
        :param force_visitor_id: optional if specified, this Houzz visitor id will be used to calculate the test bucket
        :param force_idfa: optional if specified, this Houzz idfa will be used to calculate the test bucket
        :param force_url: optional if specified, this url will be used to calculate the test bucket
        :param force_object_id: optional if specified, this object id will be used to calculate the test bucket
        :return:
        """

        if not key_name:
            return None
        abtest = self._abtest_config.get_abtest_bucket(key_name)
        if not abtest:
            return None
        if abtest.mod_type == TestModType.MOD_BY_USER_ID:
            user_id = force_user_id if force_user_id else self._user_id
            if not user_id:
                raise Exception("please specify user_id")
            hash_input = ABTestSeed.get_user_id_hash(user_id)
        elif abtest.mod_type == TestModType.MOD_BY_VISITOR_ID:
            visitor_id = force_visitor_id if force_visitor_id else self._visitor_id
            if not visitor_id:
                raise Exception("please specify visitor_id")
            hash_input = ABTestSeed.get_visitor_id_hash(visitor_id)
        elif abtest.mod_type == TestModType.MOD_BY_IDFA:
            idfa = force_idfa if force_idfa else self._idfa
            if not idfa:
                raise Exception("please specify IDFA")
            hash_input = ABTestSeed.get_idfa_hash(idfa)
        elif abtest.mod_type == TestModType.MOD_BY_SEO_OBJECT_ID:
            raise Exception("seo_object_id not supported yet")
        elif abtest.mod_type == TestModType.MOD_BY_SEO_URL:
            raise Exception("seo_url not supported yet")
        else:
            raise Exception("this type is not supported")

        # Append test name, hash, and take top 4 bytes as the int we use for mod input
        mod_input =  abs(int(hashlib.md5(str(hash_input) + "," + key_name)[0:4], 16))

        slot = mod_input % 1000

        variation_bucket = self._assign_bucket(abtest, slot)

        self._test_key_value_map[key_name] = variation_bucket

        self._logger.info("C2ABTestManager: key: {0} by {1} - {2}, modInput: {3}, modBucket: {4}".format(
            key_name, abtest.mod_type, hash_input, mod_input, variation_bucket
        ))
        self._common_logger.log_test_selection(key_name, hash_input, abtest.mod_type)

    @staticmethod
    def _assign_bucket(abtest, slot):
        assignment = None
        # The values array must exist because we've reached this function.  Errors are caught in registration.
        buckets = abtest.buckets
        for name, bucket in buckets.items():
            bucket_slots = bucket.slots
            for bucket_slot in bucket_slots:
                if slot >= bucket_slot.s_start and slot <= bucket_slot.s_end:
                    assignment = name

        return assignment

    def get_bucket_value(self, key_name):
        if key_name in self._test_key_value_map:
            return self._test_key_value_map[key_name]
        else:
            return None


class ABTestManagerModule(app.Module):
    """
    ABTest Manager Module for managing ABTest manager module dependencies and
    create ABTest instance

    Attributes
    ----------
    abtest_config : ABTestConfig
        a ABTest config manager to retrieve abtest from ABTest thrift service
    """
    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def setup_function(self):
        self._abtest_config = ABTestConfig(EnvModule.env, ABTestServiceModule().connection)

    def __init__(self):
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[ConfigModule.full_class_path(),
                          ABTestServiceModule.full_class_path(),
                          EnvModule.full_class_path()],
            description="ABTest Manager Module")
        self._abtest_config = None


    def get_instance(self,
                     user_id=None,
                     visitor_id=None,
                     idfa=None,
                     url=None,
                     object_id=None):
        """
        create an ABTest Manager Instance
        :param user_id: int, optional
        :param visitor_id: string, optional
        :param idfa: string, optional
        :param url: string, optional
        :param object_id: string, optional
        :return: ABTestManager
        """
        return ABTestManager(self._abtest_config, user_id, visitor_id, idfa, url, object_id)
