# These two modules are packed together as "base" because they're used pretty
# much everywhere.
python_library(
    name='base_module_lib',
    sources=[
        'config_module.py',
        'env_module.py',
    ],
    dependencies=[
        '3rdparty/python:twitter_common_app_req',
        'src/python/houzz/common:config_lib',
    ]
)

python_library(
    name='abtest_service_module_lib',
    sources=['abtest_service_module.py'],
    dependencies=[
        ':base_module_lib',
        ':discovery_service_lib',
        'src/python/houzz/common:service_proxy_lib',
        '3rdparty/python:twitter_common_app_req',
        'c2thrift/definitions/backends:discovery_service',
        'c2thrift/definitions/backends:tm_service',
    ]
)

python_library(
    name='user_profile_module_lib',
    sources=['user_profile_module.py'],
    dependencies=[
        ':base_module_lib',
        ':discovery_service_lib',
        'src/python/houzz/common:service_proxy_lib',
        '3rdparty/python:twitter_common_app_req',
        'c2thrift/definitions/backends:discovery_service',
        'c2thrift/definitions/backends:user_profile_service',
    ]
)

python_library(
    name='access_control_module_lib',
    sources=['access_control_module.py'],
    dependencies=[
        ':base_module_lib',
        ':discovery_service_lib',
        'src/python/houzz/common:service_proxy_lib',
        '3rdparty/python:twitter_common_app_req',
        'c2thrift/definitions/backends:access_control',
        'c2thrift/definitions/backends:discovery_service',
    ]
)

python_library(
    name='discovery_service_lib',
    sources=['discovery_service_module.py'],
    dependencies=[
        ':base_module_lib',
        'src/python/houzz/common:thrift_req',
        '3rdparty/python:twitter_common_app_req',
        'c2thrift/definitions/backends:discovery_service',
    ]
)

# TODO: Currently not used anywhere!
python_library(
    name='epn_service_module_lib',
    sources=['epn_service_module.py'],
    dependencies=[
        ':base_module_lib',
        ':discovery_service_lib',
        'src/python/houzz/common:service_proxy_lib',
        '3rdparty/python:twitter_common_app_req',
        'c2thrift/definitions/backends:epn_service',
    ]
)

# TODO: Refactor out dependency on thrift (via slogger_lib), which is only
# needed when LOGGER_HOST is not None.
python_library(
    name='logging_module_lib',
    sources=['logging_module.py'],
    dependencies=[
        ':base_module_lib',
        '3rdparty/python:twitter_common_app_req',
        'src/python/houzz/common/logger:slogger_lib',
    ]
)

python_library(
    name='metric_module_lib',
    sources=['metric_module.py'],
    dependencies=[
        ':base_module_lib',
        '3rdparty/python:twitter_common_app_req',
        'src/python/houzz/common/metric/client:metric_client',
    ]
)

python_library(
    name='mpa_campaigns_service_module_lib',
    sources=['mpa_campaigns_service_module.py'],
    dependencies=[
        ':base_module_lib',
        'src/python/houzz/common:calendar_utils_lib',
        'src/python/houzz/common:retry_req',
        'src/python/houzz/common:service_proxy_discoveryless_lib',
        'src/python/houzz/common/logger:slogger_lib',
        'c2thrift/definitions/backends:discovery_service',
        'c2thrift/definitions/jukwaa:base',
        'c2thrift/definitions/backends:mpa_campaigns_service',
        '3rdparty/python:twitter_common_app_req',
    ]
)

python_library(
    name='rewards_service_module_lib',
    sources=['rewards_service_module.py'],
    dependencies=[
        ':base_module_lib',
        'src/python/houzz/common:calendar_utils_lib',
        'src/python/houzz/common:service_proxy_discoveryless_lib',
        'src/python/houzz/common/logger:slogger_lib',
        'c2thrift/definitions/backends:discovery_service',
        'c2thrift/definitions/jukwaa:base',
        'c2thrift/definitions/backends:rewards_service',
        '3rdparty/python:twitter_common_app_req',
    ]
)

python_library(
    name='email_module_lib',
    sources=['email_module.py'],
    dependencies=[
        ':base_module_lib',
        'src/python/houzz/common/logger:slogger_lib',
        '3rdparty/python:twitter_common_app_req',
        "src/python/houzz/common:email_utils_lib",
    ]
)

python_library(
    name='mysql_module_lib_py3',
    sources=['mysql_module.py'],
    dependencies=[
        ':base_module_lib',
        'src/python/houzz/common:pymysql_req_py3',
        '3rdparty/python:twitter_common_app_req',
    ]
)

python_library(
    name='mysql_module_lib',
    sources=['mysql_module.py'],
    dependencies=[
        ':base_module_lib',
        'src/python/houzz/common:pymysql_req',
        '3rdparty/python:twitter_common_app_req',
    ]
)

# Currently only used by houzz/paddy:incoming_email_postfix_proxy.
python_library(
    name='paddy_service_module_lib',
    sources=['paddy_service_module.py'],
    dependencies=[
        ':base_module_lib',
        ':discovery_service_lib',
        ':service_module_lib',
        'src/python/houzz/common:service_proxy_lib',
        '3rdparty/python:twitter_common_app_req',
        'c2thrift/definitions/backends:paddy',
        'src/python/houzz/paddy:paddy_thrift_gen_tornado',
    ]
)

python_library(
    name='perf_module_lib',
    sources=['perf_module.py'],
    dependencies=[
        ':base_module_lib',
        ':metric_module_lib',
        'src/python/houzz/common/logger:logger_lib',  # perf_monitor.py
        '3rdparty/python:twitter_common_app_req',
    ]
)

python_library(
    name='redis_module_lib',
    sources=['redis_module.py'],
    dependencies=[
        ':base_module_lib',
        'src/python/houzz/common:redis_utils_lib',
        'src/python/houzz/common:redis_req',
        '3rdparty/python:twitter_common_app_req',
    ]
)

python_library(
    name='redis_module_lib_new',
    sources=['redis_module.py'],
    dependencies=[
        ':base_module_lib',
        'src/python/houzz/common:redis_utils_lib_new',
        'src/python/houzz/common:redis_req_new',
        '3rdparty/python:twitter_common_app_req',
    ]
)

# Currently only used by src/python/houzz/metric/agent:metric_agent_server.
python_library(
    name='salt_module_lib',
    sources=['salt_module.py'],
    dependencies=[
        ':base_module_lib',
        'src/python/houzz/common:salt_req',
        '3rdparty/python:twitter_common_app_req',
    ]
)

python_library(
    name='service_module_lib',
    sources=['service_module.py'],
    dependencies=[
        ':base_module_lib',
        ':perf_module_lib',
        'src/python/houzz/common:py3_util_lib',
        'src/python/houzz/common:svcregister_lib',
        'src/python/houzz/common:requests_req',
        'src/python/houzz/common:six_req',
        'src/python/houzz/common:thrift_req',
        'src/python/houzz/common:tornado_req',
        '3rdparty/python:twitter_common_app_req',
    ]
)

# TODO: Currently not used anywhere.
python_library(
    name='service_proxy_module_lib',
    sources=['service_proxy_module.py'],
    dependencies=[
        ':discovery_service_lib',
        'src/python/houzz/common:service_proxy_lib',
        '3rdparty/python:twitter_common_app_req',
    ]
)
