python_library(
    name="logger_lib",
    sources=[
        'gmetric.py',
        'perf_monitor.py',
        'process.py',
        'separate.py',
        'smart_file.py',
        'common_logger.py',
        'logger_process.py',
        'queue_handler.py'
    ],
    dependencies=[
        'src/python/houzz/common:requests_req',
        'src/python/houzz/common:six_req',
        'src/python/houzz/common:thrift_req',
        ':slogger_lib',
        'src/python/houzz/common:config_lib',
        'c2thrift/definitions/backends:log_record',
        'src/python/houzz/common/metric/client:metric_client',
    ],
)

# Common library refactoring.

# Let's keep slogger.py and hz_handlers.py together: as of 12/3/2018,
# hz_handlers.py is only imported from slogger.py and
# rq/perf/SocketLoggerTest.py.
#
# TODO: Most users of logging do not actually use logger_host, so don't need the
# dependency on Thrift, but currently it's hard to extricate the dependency off.
python_library(
    name='slogger_lib',
    sources=[
        'hz_handlers.py',
        'slogger.py',
    ],
    dependencies=[
        'src/python/houzz/common:six_req',
        'src/python/houzz/common:thrift_req',
        'c2thrift/definitions/backends:log_record',
    ]
)
