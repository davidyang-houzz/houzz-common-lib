import unittest

from houzz.common.data_access import mysql


class BadQueryTests(unittest.TestCase):
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
        from houzz.common.config import get_config
        config = get_config()
        config['MYSQL_DB_SLAVE_PARAMS'] = config['DEV_MYSQL_DB_SLAVE_PARAMS']
        config['MYSQL_DB_PARAMS'] = config['DEV_MYSQL_DB_PARAMS']
        self.connector = mysql.Connector(config)

    def test_select_bad_col_slave(self):
        query = 'select bad_col from users'
        self.assertRaises(mysql.DataConnectivityError, self.do_query, query)

    def test_select_bad_col_master(self):
        query = 'select bad_col from users'
        self.assertRaises(mysql.DataConnectivityError, self.do_query, query, slave=False)

    def do_query(self, query, slave=True):
        if slave:
            chunks = self.connector.query_slave(query)
        else:
            chunks = self.connector.query_master(query)
        for chunk in chunks:
            for row in chunk:
                pass
