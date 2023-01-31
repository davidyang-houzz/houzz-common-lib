#!/usr/bin/env python

from __future__ import absolute_import
import os
import logging
import operator

from . import yaml
import six
from functools import reduce

logger = logging.getLogger(__name__)

_Config = None
_loaded_files = []


# Inspired by http://code.activestate.com/recipes/389916/
class ConfigDict(dict):
    """A dictionary where members can be accessed as attributes
    """

    def __init__(self, indict=None, **kwargs):
        """You can refer indict members x either obj.x or obj['x'].
            """
        super(ConfigDict, self).__init__(**kwargs)
        if indict is None:
            indict = {}
        for k, v in six.iteritems(indict):
            self.__setattr__(k, v)

    def __getattr__(self, item):
        """Maps values to attributes. Only called if there *isn't* an attribute
        with this name
        """
        try:
            # print 'DEBUG: normal attribute "%s" not found, try internal dict'
            # % item
            return self.__getitem__(item)
        except KeyError:
            raise AttributeError(item)

    def __setattr__(self, item, value):
        """Maps attributes to values.
        """
        if isinstance(value, dict):
            value = ConfigDict(value)
        if item in six.viewkeys(self):
            # any normal attributes are handled normally
            if isinstance(self.__getitem__(item), dict) \
                    and isinstance(value, dict):
                self.__getitem__(item).update(value)
            else:
                dict.__setattr__(self, item, value)
        else:
            self.__setitem__(item, value)

    def merge(self, indict):
        if indict is None:
            indict = {}
        for k, v in six.iteritems(indict):
            self.__setattr__(k, v)

    def customize(self, default, override):
        assert isinstance(self.__getitem__(default), dict)
        assert isinstance(self.__getitem__(override), dict)
        self.__getitem__(default).update(
            self.__getitem__(override))


def get_config():
    global _Config
    if _Config is None:
        _Config = load_config()
    return _Config


def get_loaded_files():
    global _loaded_files
    return _loaded_files


def load_config(paths=None, auto_reload=False):
    # TODO: Remove the auto_reload arg altogether.
    assert not auto_reload, \
           'auto_reload is no longer supported: ' \
           'import houzz.common.reloader directly!'

    global _Config, _loaded_files

    if _Config is None:
        _Config = ConfigDict()

    if not paths:
        paths = [
            '/home/clipu/c2/config/config.yaml',
            '/home/clipu/c2svc/python/houzz/config/bootstrap.yaml',
            '/home/clipu/c2svc/python/houzz/config/config.yaml',
            '/etc/luigi/houzz_config.yaml',
            '/houzz/c2svc/src/python/houzz/config/config.yaml',
            # Use relative path to find the local config. Mostly for unittesting.
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                '../config/config.yaml'
            ),
        ]

    paths = reduce(operator.add, (path.split(';') for path in paths))
    logger.info('Loading config from paths: %s' % (paths, ))
    for path in paths:
        if os.path.exists(path):
            with open(path, 'r') as f:
                _Config.merge(indict=yaml.safe_load(f))
                _loaded_files.append(path)
    return _Config
