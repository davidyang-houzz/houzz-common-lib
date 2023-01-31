from __future__ import absolute_import
__author__ = 'jasonl'

import time
import logging
import os

from cassandra.cqlengine import columns, CQLEngineException
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import DoesNotExist

from houzz.common.cassandra.cassandra_module import CassandraModule
from houzz.common.cassandra.cassandra_utils import has_attribute, memoize
from houzz.common.json_serializable import JSONSerializable

log = logging.getLogger(__name__)

class BaseModel(Model, JSONSerializable):
    '''
    https://datastax.github.io/python-driver/api/cassandra/cqlengine/models.html
    https://datastax.github.io/python-driver/api/cassandra/cqlengine/columns.html
    Follow the above link to build a model class
    All the houzz model class must extend from the BaseModel
    '''

    __abstract__ = True

    '''
    Required fields must be defined in the subclass
    '''
    __keyspace__ = None
    __table_name__ = None

    '''
    Optional configure field
    Full table configuration options:
    http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html
    i.e.:
        __options__ = {'default_time_to_live': 60,
                      'comment': 'User category feature stored here'}
    '''

    '''
    Query optional fields:
    https://datastax.github.io/python-driver/api/cassandra/cqlengine/query.html
    __filter__: define all the table-wise filters, type as dict
    __limit__: define the table-wise query limiter, type as integer
    __defer__: define the table-wise query defer fields, type as list
    __only__: define the table-wise query only fields, type as list
    '''
    __filter__ = None
    __limit__ = None
    __defer__ = None
    __only__ = None

    '''
    Override these method if you want to have your own serialize logic
    '''
    def to_json(self):
        return dict(self)

    @classmethod
    def is_table_exist(cls):
        raw_cf_name = cls._raw_column_family_name()
        cluster = CassandraModule().cluster
        try:
            keyspace = cluster.metadata.keyspaces[cls.__keyspace__]
        except KeyError:
            raise CQLEngineException("Keyspace '%s' does not exist." % cls.__keyspace__)
        tables = keyspace.tables
        return raw_cf_name in tables

    @classmethod
    def has_filter(cls):
        # check the model has filter defined
        has_attribute(cls, "__filter__")

    @classmethod
    def has_limit(cls):
        # check the model has query limit defined
        has_attribute(cls, "__limit__")

    @classmethod
    def has_defer(cls):
        # check the model has the fields don't want to load defined
        has_attribute(cls, "__defer__")

    @classmethod
    def has_only(cls):
        # check the model has the only fields want to load defined
        has_attribute(cls, "__only__")

    @classmethod
    @memoize
    def has_composed_primary_key(cls):
        primary_key_count = 0
        for c in cls._columns.values():
            if c.is_primary_key:
                primary_key_count += 1
        return primary_key_count > 1

    @classmethod
    def get_model_info(cls):
        view = dict()
        view['__keyspace__'] = cls.__keyspace__
        view['__table_name__'] = cls.__table_name__
        view['Columns'] = \
            sorted([cls.normalize_column_view(c) for c in cls._columns.values()], key=lambda column: column['position'])
        return view

    @staticmethod
    def normalize_column_view(column):
        view = dict()
        view['name'] = column.db_field_name
        view['column_def'] = column.get_column_def()
        view['is_partition_key'] = column.partition_key
        view['is_primary_key'] = column.primary_key
        view['is_required'] = column.required
        view['position'] = column.position
        return view


class ModelVersion(BaseModel):
    '''
    If there is any houzz model register to class with with_version field to True
    Cluster manager will consider this model should be version tracked. And will create an entry in
    the model_version table to track the table version
    '''

    __table_name__ = "model_version"

    table_name = columns.Text(primary_key=True)
    version = columns.BigInt(primary_key=True, clustering_order="DESC")
    uuid = columns.TimeUUID(primary_key=True, clustering_order="DESC")


class ModelProperty(object):
    '''
    Decorator class to map a class property to the model
    It will do lazy query for the model object when access the the class Property.
    try take a look at the example in  "UserProfile class /houzz/c2svc/src/python/houzz/profile/model/user_profile.py"

    usage:
    @ModelProperty(ModelClass)
    all the following parameters will override the model class corresponding fields
    '''

    def __init__(self, model_class, filter=None, limit=None, only=None, defer=None, ttl=-1):
        self._ttl = ttl
        self._model_class = model_class
        self._filter = filter if filter else getattr(model_class, "__filter__", None)
        self._limit = limit if limit else getattr(model_class, "__limit__", None)
        self._only = only if only else getattr(model_class, "__only__", None)
        self._defer = defer if defer else getattr(model_class, "__defer__", None)
        self._is_batch_query = model_class.has_composed_primary_key()
        self._log = logging.getLogger(self.__class__.__name__)

    def __call__(self, fget, doc=None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__
        self.__module__ = fget.__module__
        return self

    def __get__(self, inst, owner):
        now = time.time()
        try:
            value, last_update = inst._cache[self.__name__]
            if now - last_update > self._ttl >= 0:
                raise AttributeError
        except (KeyError, AttributeError):
            if not hasattr(inst, 'id'):
                raise AttributeError
            if hasattr(self._model_class, "__ready__") and not self._model_class.__ready__:
                # the model is not ready for serving yet, return empty result
                value = [] if self._is_batch_query else None
            else:
                value = self._model_class.objects(id=inst.id)
                if self._filter:
                    value = value.filter(**self._filter).allow_filtering()
                if self._limit:
                    value = value.limit(self._limit)
                if self._only:
                    value = value.only(self._only)
                if self._defer:
                    value = value.defer(self._defer)
                try:
                    value = value.all() if self._is_batch_query else value.get()
                except DoesNotExist:
                    log.error("unable to get value from table", self._model_class.__table_name__, exc_info=1)
                    value = None
            try:
                cache = inst._cache
            except AttributeError:
                cache = inst._cache = {}
            cache[self.__name__] = (value, now)
        return value

model_property = ModelProperty
