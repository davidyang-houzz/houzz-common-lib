python_library(
    name="s3_image_util_lib",
    sources="s3_image_util.py",
)

python_library(
    name='boto3_thread_pool_lib',
    sources=['boto3_thread_pool.py'],
    dependencies=[
        'src/python/houzz/common:boto3_req',
        'src/python/houzz/common:futures_req',
        'src/python/houzz/common:six_req',
    ]
)

python_library(
    name='ec2_utils_lib',
    sources=['ec2_utils.py'],
    dependencies=[
        'src/python/houzz/common:config_lib',
        'src/python/houzz/common:email_utils_lib',
        'src/python/houzz/common:py3_util_lib',
        'src/python/houzz/common/logger:slogger_lib',
        'src/python/houzz/common:boto3_req',
        'src/python/houzz/common:paramiko_req',
        'src/python/houzz/common:requests_req',
        'src/python/houzz/common:six_req',
    ],
)

python_library(
    name='s3_syncer_lib',
    sources=['s3_syncer.py'],
    dependencies=[
        ":ec2_utils_lib",
        ":s3_image_util_lib",
        "src/python/houzz/common:config_lib",
        "src/python/houzz/common:operation_utils_lib",
        "src/python/houzz/common/aws/s3/syncer:syncer_lib",
        'src/python/houzz/common:boto3_req',
        "src/python/houzz/common:six_req",
    ],
)

# Currently ony used by houzz/tools/trep.
python_library(
    name='s3_utils_lib',
    sources=['s3_utils.py'],
    dependencies=[
        'src/python/houzz/common:config_lib',
        ":filechunkio_req",
        'src/python/houzz/common:boto3_req',
        "src/python/houzz/common:six_req",
    ],
)

python_requirement_library(
    name="filechunkio_req", requirements=[
        python_requirement(name='filechunkio', requirement='filechunkio==1.6'),
    ],
)
