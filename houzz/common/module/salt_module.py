#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from twitter.common import app

import salt.config
import salt.loader

from houzz.common.module.config_module import ConfigModule


class SaltModule(app.Module):
    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(ConfigModule())

        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[
                ConfigModule.full_class_path(),
            ],
            description="Salt Module")

    def setup_function(self):
        self._salt_config = ConfigModule().get_config('SALT_CONFIG')
        self._minion_config_path = self._salt_config.get('MINION_CONFIG_PATH')
        self._minion_config = salt.config.minion_config(self._minion_config_path)
        self._grains = salt.loader.grains(self._minion_config)

    @property
    def minion_config(self):
        return self._minion_config

    @property
    def grains(self):
        return self._grains

    @property
    def roles(self):
        return self._grains.get('role', [])
