python_library(
    name="syncer_lib",
    sources=[
        'base.py',
        'dump_syncer.py',
        'exceptions.py',
        'job.py',
        'log_syncer.py',
        'syncer_module.py',
        'uploader.py',
        'utils.py',
    ],
    dependencies=[
        "src/python/houzz/common:process_utils_lib",
        "src/python/houzz/common:shutdown_handler_lib",
        "src/python/houzz/common/module:base_module_lib",
        "src/python/houzz/common/module:metric_module_lib",
        "3rdparty/python:twitter_common_app_req",
        "src/python/houzz/common:boto3_req",
        "src/python/houzz/common:six_req",
    ],
)
