from datetime import datetime, timedelta

from twitter.common import app

from houzz.common.module.metric_module import MetricModule
from houzz.push.native.models import PushUser

app.add_option('--conf_file', dest='conf_file',
               metavar='CONF_FILE', type='string',
               default='/opt/configs/config/config.yaml', help='config file path')

m = MetricModule()
m.disable_metrics = False
m.metric_uds = '/opt/sock/hzsock/metric_agent_server.socket'
m.service_name = 'default'
m.tags = {}
app.register_module(m)


def set_up_connector():
    from houzz.common.config import load_config
    conf_file = app.get_options().conf_file
    config = load_config([conf_file])
    config['MYSQL_DB_SLAVE_PARAMS'] = config['STG_MYSQL_DB_SLAVE_PARAMS']
    config['MYSQL_DB_PARAMS'] = config['STG_MYSQL_DB_PARAMS']
    PushUser.init_connector(config)


if __name__ == '__main__':
    last_time = datetime.now()
    set_up_connector()
    while True:
        if last_time + timedelta(seconds=1) > datetime.now():
            continue
        res = PushUser.fetch_by_id(7)
        try:
            res = PushUser.fetch('no_col = 1')
        except:
            pass
        last_time = datetime.now()
