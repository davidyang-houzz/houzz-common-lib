from __future__ import absolute_import
from houzz.common.cassandra.base_model import BaseModel


class MPCatalogKeySpace(BaseModel):
    __abstract__ = True
    __keyspace__ = 'mp_catalog'
