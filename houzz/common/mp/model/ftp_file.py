from __future__ import absolute_import
from cassandra.cqlengine import columns
from houzz.common.cassandra.cluster_manager import ModelRegister
from .mp_catalog_keyspace import MPCatalogKeySpace


@ModelRegister(has_version=False)
class FtpFile(MPCatalogKeySpace):
    __table_name__ = "ftp_files"

    TYPE_GENERAL = 1

    SOURCE_IMPORT = 1
    SOURCE_EXPORT = 2

    # import flow
    STATUS_INCOMING = 0
    STATUS_PENDING = 1
    STATUS_PROCESSING = 2
    STATUS_PROCESSED = 3
    STATUS_ARCHIVED = 4

    # export flow
    STATUS_EXPORTING = 10
    STATUS_EXPORTED = 11

    ftp_file_id = columns.UUID(primary_key=True)
    type = columns.Integer()
    source = columns.Integer()
    status = columns.Integer()
    account = columns.Text()
    original_name = columns.Text()
    s3_key = columns.Text()
    job_id = columns.Integer()
    created = columns.DateTime()
    activities = columns.Text()
