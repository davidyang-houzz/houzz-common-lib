from __future__ import absolute_import
__author__ = 'jasonl'

import logging
import os
import pkgutil
import functools
import sys
import csv

from json import JSONEncoder
from cassandra.cqlengine.management import sync_table, drop_table

log = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

def csv_reader(file_path, batch_size=1):
    batch = []
    with open(file_path, 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if len(batch) < batch_size:
                batch.append(row)
            else:
                yield batch
                batch = []
    if batch:
        yield batch


def get_root_path(import_name):
    # Module already imported and has a file attribute.  Use that first.
    mod = sys.modules.get(import_name)
    if mod is not None and hasattr(mod, '__file__'):
        return os.path.dirname(os.path.abspath(mod.__file__))

    # Next attempt: check the loader.
    loader = pkgutil.get_loader(import_name)

    # Loader does not exist or we're referring to an unloaded main module
    # or a main module without path (interactive sessions), go with the
    # current working directory.
    if loader is None or import_name == '__main__':
        return os.getcwd()

    # For .egg, zipimporter does not have get_filename until Python 2.7.
    # Some other loaders might exhibit the same behavior.
    if hasattr(loader, 'get_filename'):
        filepath = loader.get_filename(import_name)
    else:
        # Fall back to imports.
        __import__(import_name)
        filepath = sys.modules[import_name].__file__

    # filepath is import_name.py for a module, or __init__.py for a package.
    return os.path.dirname(os.path.abspath(filepath))


def get_test_file_path(table_name):
    current_path = get_root_path(__name__)
    file_path = current_path + '/../data/' + table_name + ".csv"
    return file_path


def has_method(cls, method):
    return hasattr(cls, method) and callable(getattr(cls, method))


def create_model(model, force=False, data_dir=None):
    if not model.is_table_exist() or force:
        log.info("creating table %s" % model.__table_name__)
        drop_table(model)
        sync_table(model)
        log.info("loading table %s data" % model.__table_name__)
        test_file = get_test_file_path(model.__table_name__)
        for row in csv_reader(test_file):
            model(**row).save()
        counts = model.objects.count()
        log.info('done with table [%s] with count [%d]' % (model.__table_name__, counts))


class ModelObjectEncoder(JSONEncoder):

    def default(self, obj):
        if not hasattr(obj, "to_json"):
            try:
                iterable = iter(obj)
            except TypeError:
                return obj
            else:
                return list(iterable)
        else:
            return obj.to_json()


def model_map(modal_cls):
    def decorate(func):
        def query(self):
            if not hasattr(self, 'id'):
                pass
            id = self.id
            res = modal_cls(id)
            return res
        return query
    return decorate


def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            wrapper.res = f(*args, **kwargs)
        return wrapper.res
    wrapper.has_run = False
    return wrapper


class ThriftFeature(object):
    all = {}

    @classmethod
    def register(cls, feature):
        def decorator(fn):
            cls.all[feature] = fn.__name__
            return fn
        return decorator

thrift_feature = ThriftFeature.register


def has_attribute(ob, attribute):
    return getattr(ob, attribute, None) is not None


def memoize(obj):
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
            return cache[key]

    return memoizer
