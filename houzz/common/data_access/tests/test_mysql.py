import unittest

from houzz.common.data_access import mysql


class LogQueryTests(unittest.TestCase):
    def setUp(self):
        self.set_up_metric()
        self.set_up_connector()

    def set_up_metric(self):
        m = mysql.MetricModule()
        m.disable_metrics = True
        m.metric_uds = ''
        m.service_name = 'default'
        m.tags = {}
        mysql.app.register_module(m)

    def set_up_connector(self):
        self.connector = mysql.Connector()

    def test_master_read(self):
        sql = '''/*ms=master*//*src=batch*/
            select * from some_table;
        '''
        res = self.connector._log_query(sql)
        self.assertEqual(res, {
            'key': 'py_sql_master_read_count',
            'val': 1,
            'tags': {'cmd': self.connector.cmd},
            'service_name': mysql.SERVICE_NAME
        })

    def test_master_read_error(self):
        sql = '''/*ms=master*//*src=batch*/
            select * from some_table;
        '''
        err_code = 2003
        res = self.connector._log_query(sql, err=mysql.MySQLError(err_code))
        self.assertEqual(res, {
            'key': 'py_sql_master_read_error',
            'val': 1,
            'tags': {'cmd': self.connector.cmd, 'error_code': str(err_code)},
            'service_name': mysql.SERVICE_NAME
        })

    def test_master_read_finish(self):
        sql = '''/*ms=master*//*src=batch*/
            select * from some_table;
        '''
        res = self.connector._log_query(sql, finish=True)
        self.assertEqual(res, {
            'key': 'py_sql_master_read_finish_count',
            'val': 1,
            'tags': {'cmd': self.connector.cmd},
            'service_name': mysql.SERVICE_NAME
        })

    def test_master_write_finish(self):
        sql = '''/*ms=master*//*src=batch*/
            select * from some_table;
        '''
        res = self.connector._log_query(sql, read=False, finish=True)
        self.assertEqual(res, {
            'key': 'py_sql_master_write_finish_count',
            'val': 1,
            'tags': {'cmd': self.connector.cmd},
            'service_name': mysql.SERVICE_NAME
        })


    def test_slave_read(self):
        sql = '''/*ms=slave*//*src=batch*/
            select * from some_table;
        '''
        res = self.connector._log_query(sql)
        self.assertEqual(res, {
            'key': 'py_sql_slave_read_count',
            'val': 1,
            'tags': {'cmd': self.connector.cmd},
            'service_name': mysql.SERVICE_NAME
        })

    def test_master_write(self):
        sql = '''/*ms=master*//*src=batch*/
            select * from some_table;
        '''
        res = self.connector._log_query(sql, read=False)
        self.assertEqual(res, {
            'key': 'py_sql_master_write_count',
            'val': 1,
            'tags': {'cmd': self.connector.cmd},
            'service_name': mysql.SERVICE_NAME
        })

    def test_master_write_error(self):
        sql = '''/*ms=master*//*src=batch*/
            select * from some_table;
        '''
        err_code = 2003
        res = self.connector._log_query(sql, read=False, err=mysql.MySQLError(err_code))
        self.assertEqual(res, {
            'key': 'py_sql_master_write_error',
            'val': 1,
            'tags': {'cmd': self.connector.cmd, 'error_code': str(err_code)},
            'service_name': mysql.SERVICE_NAME
        })
