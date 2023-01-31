from __future__ import absolute_import
import logging
import copy
import sys

from twitter.common import app

from houzz.common.metric.common.ratelimiting import RateLimiting
from houzz.common.module.metric_module import MetricModule
from houzz.common.data_access.exceptions import DataConnectivityError

try:
    import MySQLdb as pymysql
    from MySQLdb import cursors, escape_string, MySQLError
except Exception:
    import pymysql
    from pymysql import cursors, MySQLError
    if sys.version_info[0] == 2:
        from pymysql import escape_string
    else:
        from pymysql.converters import escape_string

logger = logging.getLogger(__name__)

SERVICE_NAME = 'sys'


class Connector(object):
    _CHUNK_SIZE = 10000
    _DEFAULT_LIMIT_COUNT = 100

    _metric_client = None

    def __init__(self, config=None):
        self.cmd = sys.argv[0]
        if not config or 'MYSQL_DB_PARAMS' not in config:
            from houzz.common.config import get_config
            config = get_config()
        if 'MYSQL_DB_PARAMS' in config:
            self._do_init(config)
        else:
            logger.warning('Failed to initialize connector, config missing DB')
        modules = app.module.AppModule.module_registry()
        metric_module = modules.get(MetricModule.full_class_path())
        if metric_module:
            self._metric_client = metric_module.metric

    def _do_init(self, config):
        self._base_params = copy.deepcopy(config.MYSQL_DB_PARAMS)
        self._base_params['cursorclass'] = cursors.Cursor
        self._master_params = copy.deepcopy(config.MYSQL_DB_PARAMS)
        self._master_params['cursorclass'] = cursors.Cursor
        if 'MYSQL_DB_SLAVE_PARAMS' in config:
            self._slave_params = copy.deepcopy(config.MYSQL_DB_SLAVE_PARAMS)
        else:
            self._slave_params = copy.deepcopy(config.MYSQL_DB_PARAMS)
        self._slave_params['cursorclass'] = cursors.Cursor
        logger.debug(
            'MySQL Connector initialized, master: %s:%s, slave: %s:%s' %
            (self._master_params.host, self._master_params.port,
                self._slave_params.host, self._slave_params.port, ))

        self._rate_limiter = RateLimiting(-1)

        if 'DB_MASTER_QUERY_RATE_LIMIT_PRAMS' in config:
            rate_limit_config = config.DB_MASTER_QUERY_RATE_LIMIT_PRAMS
            if 'ENABLED' in rate_limit_config and rate_limit_config.ENABLED:
                rate_limit_count = self._DEFAULT_LIMIT_COUNT
                if 'RATE_LIMIT' not in rate_limit_config:
                    logger.warning('Missing DB_MASTER_QUERY_RATE_LIMIT_PRAMS.RATE_LIMIT, default to %d' %
                                   self._DEFAULT_LIMIT_COUNT)
                else:
                    rate_limit_count = rate_limit_config.RATE_LIMIT

                rate_limit_window = 1
                if 'WINDOW_SIZE_SEC' not in rate_limit_config:
                    logger.warning('Missing DB_MASTER_QUERY_RATE_LIMIT_PRAMS.WINDOW_SIZE_SEC, default to 1')
                else:
                    rate_limit_window = rate_limit_config.WINDOW_SIZE_SEC

                self._rate_limiter = RateLimiting(rate_limit_count, rate_limit_window)

    def query_master(self, query, as_dict=False, chunk_size=None):
        query = '/*ms=master*//*src=batch*//*cmd=%s*/' % self.cmd + query
        with self._rate_limiter:
            return self._run_query(
                query, chunk_size=chunk_size or self._CHUNK_SIZE,
                as_dict=as_dict, **self._master_params)

    def query_slave(self, query, as_dict=False, chunk_size=None):
        query = '/*ms=slave*//*src=batch*//*cmd=%s*/' % self.cmd + query
        return self._run_query(
            query, chunk_size=chunk_size or self._CHUNK_SIZE,
            as_dict=as_dict, **self._slave_params)

    def update(self, query, params=None):
        query = '/*ms=master*//*src=batch*//*cmd=%s*/' % self.cmd + query
        with self._rate_limiter:
            return self._run_update(query, params, **self._master_params)

    def _create_connection(self, **kwargs):
        if 'db_connection' in kwargs:
            result = kwargs['db_connection']
        else:
            connection_params = kwargs
            if kwargs != self._master_params and kwargs != self._slave_params:
                connection_params = copy.deepcopy(self._base_params)
                connection_params.update(kwargs)
            else:
                connection_params = kwargs
            result = pymysql.connect(**connection_params)
        return result

    def _db_escape_string(self, src_str):
        """ return escaped version of the string to be included in SQL """
        return escape_string(src_str)

    def _run_query(self, query, chunk_size=None, extra_query_clause=None,
                   as_dict=False, **kwargs):
        db_connection = self._create_connection(**kwargs)
        if extra_query_clause:
            query = '%s %s' % (query, extra_query_clause)
        logger.debug(
            'execute query: %s. chunk_size: %s, extra_query_clause: %s',
            query, chunk_size, extra_query_clause)

        cur_class = None
        if as_dict:
            cur_class = pymysql.cursors.DictCursor

        cur = db_connection.cursor(cur_class)

        try:
            self._log_query(query)
            cur.execute(query)
            pos = 0
            if not chunk_size:
                chunk = cur.fetchall()
                logger.debug('yield %s : %s results', pos, pos + len(chunk))
                yield chunk
            else:
                while True:
                    chunk = cur.fetchmany(chunk_size)
                    if not chunk:
                        break
                    logger.debug(
                        'yield %s : %s results', pos, pos + len(chunk))
                    yield chunk
                    pos += len(chunk)
        except MySQLError as err:
            logger.error('Failed to run query, error: %s' % err)
            self._log_query(query, err=err)
            raise DataConnectivityError(origin_exception=err)
        else:
            self._log_query(query, finish=True)
        finally:
            cur.close()
            db_connection.close()

    def _run_update(self, query, params=None, **kwargs):

        db_connection = self._create_connection(**kwargs)

        logger.debug('execute query: %s. params: %s', query, params)
        cur = db_connection.cursor()
        try:
            self._log_query(query, read=False)
            cur.execute(query, params)
            num_rows = cur.rowcount
        except MySQLError as err:
            logger.error('Failed to run query, error: %s' % err)
            self._log_query(query, read=False, err=err)
            raise DataConnectivityError(origin_exception=err)
        else:
            self._log_query(query, read=False, finish=True)
        finally:
            cur.close()
            db_connection.commit()
            db_connection.close()
        return num_rows

    def _log_query(self, query, read=True, err=None, finish=False):
        if not self._metric_client:
            return
        master_or_slave = '_master' if query.startswith('/*ms=master*/') else '_slave'
        read_or_write = '_read' if read else '_write'
        if finish:
            suffix = '_finish_count'
        elif err:
            suffix = '_error'
        else:
            suffix = '_count'
        tags = {
            'cmd': self.cmd,
        }
        if err:
            tags['error_code'] = str(getattr(err, 'errno', None) or err.args[0])
        params = {
            'key': 'py_sql' + master_or_slave + read_or_write + suffix,
            'val': 1,
            'tags': tags,
            'service_name': SERVICE_NAME
        }
        self._metric_client.log_counter(**params)
        return params
