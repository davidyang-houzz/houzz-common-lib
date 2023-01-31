from __future__ import absolute_import
from cassandra.cqlengine import columns
from houzz.common.cassandra.cluster_manager import ModelRegister
from .mp_catalog_keyspace import MPCatalogKeySpace


@ModelRegister(has_version=False)
class FtpFileByAccount(MPCatalogKeySpace):
    __table_name__ = "ftp_files_by_account"

    account = columns.Text(partition_key=True)
    created = columns.DateTime(primary_key=True, clustering_order="DESC")
    type = columns.Integer(primary_key=True, clustering_order="DESC")
    source = columns.Integer(primary_key=True, clustering_order="DESC")
    ftp_file_id = columns.UUID()
