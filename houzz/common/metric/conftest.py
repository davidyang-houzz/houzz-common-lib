#!/usr/bin/env python
# -*- coding: utf-8 -*-

#######################################################################
#  pytest hook to setup root logging level based on --verbose config  #
#######################################################################

from __future__ import absolute_import
import logging

def pytest_configure(config):
    logging._original_level = logging.getLogger().getEffectiveLevel()
    logging._level = logging.DEBUG if config.option.verbose > 0 else logging.INFO
    logging.basicConfig(
        level=logging._level,
        format='%(asctime)s %(levelname)-5s %(processName)s %(threadName)s: %(message)s'
    )

def pytest_unconfigure(config):
    logging.getLogger().setLevel(logging._original_level)
    del logging._level
    del logging._original_level
