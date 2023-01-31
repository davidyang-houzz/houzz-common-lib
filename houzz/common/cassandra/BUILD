python_library(
    name="cassandra_lib",
    sources=[
        'base_model.py',
        'cassandra_module.py',
        'cassandra_utils.py',
        'cluster_manager.py',
    ],
    dependencies=[
        'src/python/houzz/common:async_executor_lib',
        'src/python/houzz/common:json_serializable_lib',
        'src/python/houzz/common:threading_base_lib',
        'src/python/houzz/common/module:base_module_lib',
        'src/python/houzz/common/logger:logger_lib',
        ':cassandra_extra_req',
        'src/python/houzz/common:cassandra_req',
        "src/python/houzz/common:six_req",
        '3rdparty/python:twitter_common_app_req',
    ],
)

python_requirement_library(
    name="cassandra_extra_req", requirements=[
        python_requirement(name="time_uuid", requirement="time_uuid==0.2.0"),
        python_requirement(name='contexttimer', requirement='contexttimer>=0.3.1'),
    ]
)
