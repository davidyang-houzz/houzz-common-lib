from __future__ import absolute_import
from __future__ import print_function
from cassandra.cqlengine.management import CQLENG_ALLOW_SCHEMA_MANAGEMENT

__author__ = 'jasonl'

import time
import os
import logging

from cassandra.cqlengine import management
from cassandra.util import uuid_from_time
from contexttimer import Timer
from houzz.common.async_executor import AsyncExecutor
from houzz.common.cassandra.cassandra_utils import csv_reader, run_once
from houzz.common.cassandra.base_model import ModelVersion
from houzz.common.module.config_module import ConfigModule
from houzz.common.threading_base import BaseThread

log = logging.getLogger(__name__)


class ClusterManager(BaseThread):

    # {keyspace: {table name: (model_cls, has_version)})
    __keyspace_to_models = {}
    # {keyspace: model_version_cls}
    __keyspace_to_model_version_cls = {}
    __instance = None

    def __init__(self, refresh_interval=None):
        super(ClusterManager, self).__init__()
        self._refresh_interval = refresh_interval
        self.refresh()

    def run(self):
        while not self.stopped_event.isSet():
            self.refresh()
            if self._refresh_interval:
                time.sleep(self._refresh_interval)

    @classmethod
    def refresh(cls):
        for keyspace, models in cls.__keyspace_to_models.items():
            for table_name, (model_cls, has_version) in models.items():
                if has_version:
                    version = cls.get_model_version(keyspace=keyspace, table_name=table_name)
                    if version:
                        # update the table name with version
                        model_cls.__table_name__ = table_name + "_" + str(version)
                        model_cls.__ready__ = model_cls.is_table_exist()
                    else:
                        log.debug("no version info found for [%s:%s] in model_version!",
                                  keyspace, table_name)
                        model_cls.__ready__ = False
                else:
                    model_cls.__ready__ = model_cls.is_table_exist()
                if model_cls.__ready__:
                    log.debug("[%s:%s] is ready for serving!", model_cls.__keyspace__, model_cls.__table_name__)
                else:
                    log.debug("[%s:%s] is not existing, mark the ready flag to false!",
                              model_cls.__keyspace__,
                              model_cls.__table_name__)

    @classmethod
    def init(cls, refresh_interval=None):
        config = ConfigModule().app_config
        is_disabled = config.get("DISABLE_CASSANDRA", False)
        if not is_disabled and not cls.__instance:
            refresh_interval = refresh_interval or config.get("CLUSTER_REFRESH_INTERVAL", None)
            cls.__instance = ClusterManager(refresh_interval=refresh_interval)
            cls.__instance.start()

    @classmethod
    def get_model_version(cls, keyspace, table_name):
        if keyspace in cls.__keyspace_to_model_version_cls:
            model_version_cls = cls.__keyspace_to_model_version_cls[keyspace]
            res = model_version_cls.objects(table_name=table_name).limit(1)
            for r in res:
                return r['version']
        return None

    @staticmethod
    def has_table_name(model_cls):
        return hasattr(model_cls, "__table_name__") and model_cls.__table_name__

    @staticmethod
    def has_keyspace(model_cls):
        return hasattr(model_cls, "__keyspace__") and model_cls.__keyspace__

    @staticmethod
    def new_model_version_cls(keyspace):
        cls_name = keyspace + '_' + ModelVersion.__table_name__
        cls = type(cls_name, (ModelVersion,),
                {
                    "__keyspace__": keyspace
                })
        return cls

    @classmethod
    def register(cls, model_cls, has_version=False):
        if not cls.has_table_name(model_cls) or not cls.has_keyspace(model_cls) or model_cls.__abstract__:
            raise TypeError("Model type [%s] is not as expected, can't register!", model_cls.__name__)
        table_name = model_cls.__table_name__
        keyspace = model_cls.__keyspace__
        if keyspace not in cls.__keyspace_to_models:
            cls.__keyspace_to_models[keyspace] = {}
        models_in_keyspace = cls.__keyspace_to_models[keyspace]
        if has_version:
            if keyspace not in cls.__keyspace_to_model_version_cls:
                model_version_cls = cls.new_model_version_cls(keyspace)
                cls.__keyspace_to_model_version_cls[keyspace] = model_version_cls
        models_in_keyspace[table_name] = (model_cls, has_version)
        log.info("successfully register model[%s] in keyspace [%s] with version flag [%s]" % (table_name, keyspace, has_version))

    @classmethod
    @run_once
    def setup_keyspace(cls, keyspace):
        if keyspace not in cls.__keyspace_to_models:
            raise ValueError("unable to find keyspace %s to setup" % keyspace)
        log.info("setting up keyspace [%s]", keyspace)
        management.create_keyspace_simple(keyspace, 2)
        if keyspace in cls.__keyspace_to_model_version_cls:
            # set up model version table
            model_version_cls = cls.__keyspace_to_model_version_cls[keyspace]
            log.info("setting up keyspace [%s] model version table [%s]", keyspace, model_version_cls.__table_name__)
            #management.drop_table(model_version_cls)
            management.sync_table(model_version_cls)

    @classmethod
    def setup_table(cls, keyspace, table_name, bootstrap=False):
        cls.setup_keyspace(keyspace)
        if table_name not in cls.__keyspace_to_models[keyspace]:
            log.warning("unable to find table name %s in keyspace %s, skipping....", table_name, keyspace)
            return None, None
        (model_cls, has_version) = cls.__keyspace_to_models[keyspace][table_name]
        # version = cls.get_model_version(keyspace,table_name)
        # if has_version:
        #     version = version+1 if version else 1
        # else:
        #     version = None
        version = int(time.time()) if has_version else None
        #update table name with version
        if version:
            table_name = table_name + "_" + str(version)
            model_cls.__table_name__ = table_name
        if model_cls.is_table_exist() and bootstrap:
            management.drop_table(model_cls)
        if bootstrap:
            management.sync_table(model_cls)
        return model_cls, version

    @classmethod
    def load_data(cls, keyspace, table_list, data_dir, batch_size=100, bootstrap=False):
        if bootstrap:
            os.environ[CQLENG_ALLOW_SCHEMA_MANAGEMENT] = 'True'
        cls.setup_keyspace(keyspace)
        if not table_list:
            table_list = list(cls.__keyspace_to_models[keyspace].keys())
        for table_name in table_list:
            (model_cls, version) = cls.setup_table(keyspace, table_name, bootstrap)
            if not model_cls:
                log.warning("unable to setup table name %s in keyspace %s, skipping....", table_name, keyspace)
                continue
            data_path = data_dir + "/" + table_name + ".csv" if data_dir else None
            if not data_path or not os.path.isfile(data_path):
                log.warning("no data file path [%s] exits for table [%s], skipping!", data_path, table_name)
            else:
                log.info("loading table %s data from path %s" % (table_name, data_path))
                executor = AsyncExecutor(pool_size=batch_size)

                def insert_row(row):
                    model_cls(**row).save()

                for batch in csv_reader(data_path, batch_size=batch_size):
                    with Timer() as t:
                        executor.exec_batch(insert_row, batch)
                    log.info('insert %d rows took {%f} seconds', batch_size, t.elapsed)

                log.info('done with table [%s]' % table_name)
                if version:
                    model_version_cls = cls.__keyspace_to_model_version_cls.get(keyspace)
                    meta = model_version_cls(table_name=table_name, version=version, uuid=uuid_from_time(version))
                    meta.save()
                    log.info('save metadata table %s with version %s' % (table_name, str(version)))

    @classmethod
    def get_cluster_view(cls):
        view = dict()
        for keyspace, models in cls.__keyspace_to_models.items():
            view[keyspace] = \
                {model_cls.__name__: cls.normalize_model_view(model_cls, has_version) for (model_cls, has_version) in models.values()}
        return view

    @staticmethod
    def normalize_model_view(model_cls, has_version):
        model_view = model_cls.get_model_info()
        model_view['__has_version__'] = has_version
        return model_view

    @classmethod
    def get_model_version_view(cls):
        view = dict()
        for keyspace, model_version_cls in cls.__keyspace_to_model_version_cls.items():
            res = model_version_cls.objects.all()
            view[keyspace] = [dict(r) for r in res]
        return view


class ModelRegister(object):

    def __init__(self, has_version=False):
        self._has_version = has_version

    def __call__(self, model_cls):
        print("register model_cls " + model_cls.__table_name__)
        ClusterManager.register(model_cls, self._has_version)
        print("finish registering model_cls " + model_cls.__table_name__)
        return model_cls
