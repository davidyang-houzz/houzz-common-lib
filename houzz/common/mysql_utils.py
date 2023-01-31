# Refactored out of operation_utils.py on 2019-03-12.

from __future__ import absolute_import
import logging

from pymysql import cursors
import pymysql

from houzz.common import config

DEFAULT_BULK_UPDATE_SIZE = 500

logger = logging.getLogger(__name__)
Config = config.get_config()

def run_query(query, chunk_size=None, extra_query_clause=None, **kwargs):

    db_params = Config.MYSQL_DB_PARAMS
    db_connection = create_connection(db_params=db_params, **kwargs)

    if extra_query_clause:
        query = '%s %s' % (query, extra_query_clause)
    logger.debug(
            'execute query: %s. chunk_size: %s, extra_query_clause: %s',
            query, chunk_size, extra_query_clause)

    cur = db_connection.cursor()
    cur.execute(query)
    pos = 0
    if not chunk_size:
        chunk = cur.fetchall()
        logger.info('yield %s : %s results', pos, pos+len(chunk))
        yield chunk
    else:
        while True:
            chunk = cur.fetchmany(chunk_size)
            if not chunk:
                break
            logger.info('yield %s : %s results', pos, pos+len(chunk))
            yield chunk
            pos += len(chunk)
    cur.close()
    db_connection.close()


def run_update(query, params=None, **kwargs):

    db_params = Config.MYSQL_DB_PARAMS
    db_connection = create_connection(db_params=db_params, **kwargs)

    logger.debug('execute query: %s. params: %s', query, params)
    cur = db_connection.cursor()
    cur.execute(query, params)
    num_rows = cur.rowcount
    cur.close()
    db_connection.commit()
    db_connection.close()
    return num_rows


def run_bulk_update(query, params=None, **kwargs):
    if not len(params):
        return 0

    db_params = Config.MYSQL_DB_PARAMS
    db_connection = create_connection(db_params=db_params, **kwargs)
    chunk_size = DEFAULT_BULK_UPDATE_SIZE
    if 'chunk_size' in kwargs:
        chunk_size = kwargs['chunk_size']
    assert isinstance(params, list), 'you have to use list as params for bulk update'

    logger.debug('execute bulk query: %s. chunk_size: %s, len(params): %s, params: %s',
            query, chunk_size, len(params), params)
    cur = db_connection.cursor()
    num_rows = 0  # for update, if no change, no increment
    if len(params) <= chunk_size:
        logger.debug('single execute bulk query: index range: [%s, %s)', 0, len(params))
        cur.executemany(query, params)
        num_rows = cur.rowcount
    else:
        cnt = 0
        while params:
            # TODO: This is O(n^2) on len(params) !!
            this_batch, params = params[:chunk_size], params[chunk_size:]
            logger.debug('execute bulk query: index range: [%s, %s). params: %s',
                    cnt, cnt+len(this_batch), this_batch)
            cnt += len(this_batch)
            cur.executemany(query, this_batch)
            num_rows += cur.rowcount
    cur.close()
    db_connection.commit()
    db_connection.close()
    return num_rows

def create_connection(db_params=None, **kwargs):
    if 'db_connection' in kwargs:
        return kwargs['db_connection']
    db_params.update(kwargs)
    db_params.update({'cursorclass': cursors.Cursor})
    return pymysql.connect(**db_params)
