from __future__ import absolute_import
import unittest
import os

from houzz.common.config import ConfigDict
from houzz.common.yaml import yaml


class TestClass(object):
    def __eq__(self, other):
        return isinstance(other, TestClass)


class YamlTests(unittest.TestCase):
    def setUp(self):
        self.yaml_data_string = \
            """DAEMON_PARAMS_S3_THUMB_CREATOR:
            NUM_OF_PROCESS: 1
            TUBE_PREFIX: 's3_thumb_creator'
            SLEEP_BT_QUERIES: 0
            JOB_MAX_TRIAL: 3
            DELAY_BT_RETRY: 10
            CMDS:
                CREATE_NEW_LOCAL_THUMB:
                    V1: '/usr/bin/php'
                    V2: '/usr/bin/hhvm'
            ARGS:
                CREATE_NEW_LOCAL_THUMB:
                    V1: '-f /home/clipu/c2/batch/createNewLocalThumb.php -- -path %s'
            CWDS:
                CREATE_NEW_LOCAL_THUMB:
                    V1: '/home/clipu/c2/batch'"""
        self.yaml_data_string_extra = \
            """DAEMON_PARAMS_S3_THUMB_CREATOR:
            TUBE_PREFIX: 's3_thumb_creator_extra'
            NEW_KEY: 'new_value'
            """

        self.config = ConfigDict(indict=yaml.safe_load(self.yaml_data_string))

    def test_verify_yaml(self):
        y = None
        with open(os.path.dirname(os.path.abspath(__file__)) + '/../../config/config.yaml', 'r') as f:
            try:
                y = yaml.safe_load(f)
            except:
                y = None
        self.assertIsNotNone(y, 'config.yaml canont be loaded!')

    def test_in(self):
        self.assertEqual(
            True, 'NUM_OF_PROCESS' in self.config.DAEMON_PARAMS_S3_THUMB_CREATOR)
        self.assertEqual(
            False, 'NUM_OF_PROCESS1' in self.config.DAEMON_PARAMS_S3_THUMB_CREATOR)

    def test_first_level(self):
        self.assertEqual(
            1, self.config.DAEMON_PARAMS_S3_THUMB_CREATOR.NUM_OF_PROCESS)
        self.assertEqual(
            's3_thumb_creator',
            self.config.DAEMON_PARAMS_S3_THUMB_CREATOR.TUBE_PREFIX)
        self.assertEqual(
            's3_thumb_creator',
            self.config.DAEMON_PARAMS_S3_THUMB_CREATOR['TUBE_PREFIX'])

    def test_second_level(self):
        self.assertEqual(
            '/usr/bin/php',
            self.config.DAEMON_PARAMS_S3_THUMB_CREATOR.
                CMDS.CREATE_NEW_LOCAL_THUMB.V1)
        self.assertEqual(
            '/usr/bin/hhvm',
            self.config.DAEMON_PARAMS_S3_THUMB_CREATOR.
                CMDS.CREATE_NEW_LOCAL_THUMB.V2)
        self.assertEqual(
            '/home/clipu/c2/batch',
            self.config.DAEMON_PARAMS_S3_THUMB_CREATOR.
                CWDS.CREATE_NEW_LOCAL_THUMB.V1)

    def test_default_value(self):
        self.assertEqual(
            '/usr/bin/hhvm',
            self.config.DAEMON_PARAMS_S3_THUMB_CREATOR.
                CMDS.CREATE_NEW_LOCAL_THUMB.get('V2', 'abc'))
        self.assertEqual(
            'abc',
            self.config.DAEMON_PARAMS_S3_THUMB_CREATOR.
                CMDS.CREATE_NEW_LOCAL_THUMB.get('V3', 'abc'))

    def test_merge_values(self):
        config = ConfigDict(indict=yaml.safe_load(self.yaml_data_string))
        config.merge(indict=yaml.safe_load(self.yaml_data_string_extra))
        self.assertEqual('new_value', config.DAEMON_PARAMS_S3_THUMB_CREATOR.NEW_KEY)
        self.assertEqual('s3_thumb_creator_extra', config.DAEMON_PARAMS_S3_THUMB_CREATOR.TUBE_PREFIX)
