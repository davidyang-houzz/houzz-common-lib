from __future__ import absolute_import
__author__ = 'jasonl'

import logging
import sys
from pymysql import cursors, OperationalError, connect
if sys.version_info[0] == 2:
    from pymysql import escape_string
else:
    from pymysql.converters import escape_string
from twitter.common import app

from houzz.common.module.config_module import ConfigModule

logger = logging.getLogger(__name__)


class MysqlModule(app.Module):

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self, shard='main'):
        self._shard = shard
        app.register_module(ConfigModule())
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[ConfigModule.full_class_path()],
            description="Mysql Module")
        self._master = None
        self._slave = None

    def setup_function(self):
        self._master = self.connect(True)
        self._slave = self.connect(False)

    def connect(self, is_master=False):
        section = "MYSQL_{}DB_{}PARAMS".format('' if self._shard == 'main' else self._shard.upper() + '_', '' if is_master else "SLAVE_")
        app_config = ConfigModule().app_config
        config = app_config.get(section, None)
        if config is None:
            config = ConfigModule().get_config(section)
        config['cursorclass'] = cursors.Cursor
        config['charset'] = 'utf8'
        if is_master:
            self._master = connect(**config)
            return self._master
        else:
            self._slave = connect(**config)
            return self._slave

    def close(self, is_master=False):
        if is_master:
            if self._master.open:
                self._master.close()
        else:
            if self._slave.open:
                self._slave.close()

    def reconnect(self, is_master=False):
        self.close(is_master)
        return self.connect(is_master)
    
    def ping(self, is_master=False):
        if is_master:
            self._master.ping()
        else:
            self._slave.ping()

    @property
    def shard(self):
        return self._shard

    @property
    def master(self):
        return self._master

    @property
    def slave(self):
        return self._slave

    def retry_if_operational_error(self, exception):
        """Return True if we should retry (in this case when it's an IOError), False otherwise"""
        if isinstance(exception, OperationalError):
            self.connect(True)
            self.connect(False)
            return True
        else:
            return False

    def query(self, query, chunk_size=None, extra_query_clause=None,
               as_dict=False, is_master=False, **kwargs):
        connection = self._master if is_master else self._slave
        if extra_query_clause:
            query = '%s %s' % (query, extra_query_clause)
        logger.debug(
            'execute query: %s. chunk_size: %s, extra_query_clause: %s',
            query, chunk_size, extra_query_clause)

        cur_class = None
        if as_dict:
            cur_class = cursors.DictCursor
        cur = connection.cursor(cur_class)
        cur.execute(query)
        connection.commit()
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
        cur.close()

    @staticmethod
    def db_escape_string(src_str):
        """ return escaped version of the string to be included in SQL """
        return escape_string(src_str)

    def update(self, query, params=None, **kwargs):
        logger.debug('execute query: %s. params: %s', query, params)
        cur = self._master.cursor()
        cur.execute(query, params)
        num_rows = cur.rowcount
        cur.close()
        self._master.commit()
        return num_rows

    def insert_one(self, query, params=None, **kwargs):
        logger.debug('execute insert query: %s. params: %s', query, params)
        cur = self._master.cursor()
        cur.execute(query, params)
        lastrowid = cur.lastrowid
        cur.close()
        self._master.commit()
        return lastrowid