# This is strictly for generating houzz.common package for c2dw: do not depend
# on it inside c2svc.
python_library(
    name="c2dw_houzz_common_package",
    sources=[ ],
    dependencies=[
        ':async_client_lib',
        ':config_lib',
        ':email_utils_lib',
        ':redis_utils_lib',
    ],
    provides=setup_py(
        name='houzz.common',
        version='2020.4.27',
        zip_safe=True,
        author="houzz",
        author_email="dev@houzz.com",
        description="common lib for houzz",
    )
)

python_library(
    name='async_client_lib',
    sources=['async_client.py'],
    dependencies=[
        ':config_lib',
        ':requests_req',
    ],
)

python_library(
    name='async_executor_lib',
    sources=['async_executor.py'],
    dependencies=[
        ':gevent_req',
    ],
)

python_library(
    name='date_utils_lib',
    sources=['date_utils.py'],
    dependencies=[
        ':six_req',
    ],
)

python_library(
    name='dmarc_parser_lib',
    sources=['dmarc_parser.py'],
)

python_library(
    name='domain_list_sync_files_lib',
    sources=['domain_list_sync_files.py'],
    dependencies=[
        ':operation_utils_lib',
        ':process_utils_lib',
        ':salt_sync_files_lib',
        ':salt_utils_lib',
        'src/python/houzz/common/logger:slogger_lib',
        ':six_req',
    ],
)

python_library(
    name='email_utils_lib',
    sources=['email_utils.py'],

    # NOTE: It optionally imports from houzz.common.metric.client, but the code
    # is structured so that it works even if import fails, so I will not declare
    # it as dependency here.
    dependencies=[
        ':config_lib',
        ':six_req',
    ],
)

python_library(
    name='facebook_utils_lib',
    sources=['facebook_utils.py'],
    dependencies=[
        ':config_lib',
        ':mysql_utils_lib',
        ':redis_utils_lib',
        ':url_utils_lib',
        ':six_req',
    ],
)

python_library(
    name='general_utils_lib',
    sources=['general_utils.py'],
)

python_library(
    name='internal_ip_lib',
    sources=['internal_ip.py'],
)

python_library(
    name='interrupt_handler_lib',
    sources=['interrupt_handler.py'],
    dependencies=[
        ':shutdown_handler_lib',
    ],
)

python_library(
    name='json_serializable_lib',
    sources=['json_serializable.py'],
)

python_library(
    name='local_cache_sync_files_lib',
    sources=['local_cache_sync_files.py'],
    dependencies=[
        ':config_lib',
        ':email_utils_lib',
        ':operation_utils_lib',
        ':process_utils_lib',
        ':salt_sync_files_lib',
        ':salt_utils_lib',
        'src/python/houzz/common/logger:slogger_lib',
        ':six_req',
    ],
)

python_library(
    name='multipart_post_util_lib',
    sources=['multipart_post_util.py'],
    dependencies=[
        ':six_req',
    ],
)

python_library(
    name='mysql_utils_lib',
    sources=['mysql_utils.py'],
    dependencies=[
        ':config_lib',
        ':pymysql_req',
    ],
)

python_library(
    name='operation_utils_lib',
    sources=['operation_utils.py'],
    dependencies=[
        ':config_lib',
        ':six_req',
    ],
)

python_library(
    name='paid_pro_utils_lib',
    sources=['paid_pro_utils.py'],
    dependencies=[
        ':config_lib',
        ':mysql_utils_lib',
        ':redis_utils_lib',
        ':six_req',
    ],
)

python_library(
    name='process_utils_lib',
    sources=['process_utils.py'],
    dependencies=[
        ':config_lib',
        ':psutil_req',
    ],
)

python_library(
    name='dev_tool_utils_lib',
    sources=['dev_tool_utils.py'],
    dependencies=[
        ':readchar_req',
        ':six_req',
    ],
)

python_library(
    name='dev_tool_utils_lib_for_py2',
    sources=['dev_tool_utils.py'],
    dependencies=[
        ':readchar_req_for_py2',
        ':six_req',
    ],
)

python_library(
    name='py3_util_lib',
    sources=['py3_util.py'],
)

python_library(
    name='redis_repo_lib',
    sources=['redis_repo.py'],
    dependencies=[
        'src/python/houzz/common/module:base_module_lib',
        ':boto3_req',
        ':six_req',
        ':thrift_req',
    ],
)

# TODO: redis_utils.py is also included in redis_lib!
#       ..Can we merge these two libraries into one?
python_library(
    name='redis_utils_lib',
    sources=['redis_utils.py'],
    dependencies=[
        ':config_lib',
        ':six_req',
        ':redis_req',
        ':redis_py_cluster_req',
    ],
)

python_library(
    name='redis_utils_lib_new',
    sources=['redis_utils.py'],
    dependencies=[
        ':config_lib',
        ':six_req',
        ':redis_req_new',
        ':redis_py_cluster_req_new',
    ],
)

python_requirement_library(
    name="requests_req", requirements=[
        python_requirement(name='requests', requirement='requests==2.27.1'),
    ]
)

python_requirement_library(
    name="requests_req_py2", requirements=[
        # Pin this because 2.28.1 (latest at the time) no longer supports python 2.7
        # https://github.com/psf/requests/pull/6091
        python_requirement(name='requests', requirement='requests==2.27.1'),
        python_requirement(name='certifi', requirement='certifi==2020.4.5.1'),
    ]
)

python_requirement_library(
    name="eloqua_req", requirements=[
        #python_requirement(name='pyeloqua', requirement='pyeloqua>=0.3.1'),
    ]
)

python_library(
    name='salt_sync_files_lib',
    sources=['salt_sync_files.py'],
    dependencies=[
        ':config_lib',
        ':email_utils_lib',
        ':mysql_utils_lib',
        ':operation_utils_lib',
        ':salt_utils_lib',
    ],
)

python_library(
    name='salt_utils_lib',
    sources=['salt_utils.py'],
    dependencies=[
        ':internal_ip_lib',
        ':operation_utils_lib',
        ':ssh_utils_lib',
        ':salt_req',
    ],
)

python_library(
    name='scp_lib',
    sources=['scp.py'],
)

# TODO: Currently not being used anywhere.
python_library(
    name='service_pool_lib',
    sources=['service_pool.py'],
    dependencies=[
        ':thrift_req',
    ],
)

python_library(
    name='service_proxy_discoveryless_lib',
    sources=['service_proxy_discoveryless.py'],
    dependencies=[
        ':service_proxy_lib',
        'src/python/houzz/common/module:base_module_lib',  # env_module.py
        'c2thrift/definitions/backends:discovery_service',
        ':thrift_req',
    ],
)

python_library(
    name='service_proxy_lib',
    sources=[
        'service_client_wrapper.py',
        'service_proxy.py',
    ],
    dependencies=[
        'src/python/houzz/common/module:discovery_service_lib',
        ':retry_req',
        ':thrift_req',
        '3rdparty/python:twitter_common_app_req',
    ],
)

python_library(
    name='ssh_utils_lib',
    sources=['ssh_utils.py'],
    dependencies=[
        ':scp_lib',
        ':paramiko_req',
        ':six_req',
    ],
)

python_library(
    name='stack_dumper_lib',
    sources=['stack_dumper.py'],
    dependencies=[
        ':six_req',
    ],
)

python_library(
    name="svcregister_lib",
    sources=["service_register.py"],
    dependencies=[
        ':kazoo_req',
        ':py3_util_lib',
    ],
)

python_library(
    name="shutdown_handler_lib",
    sources=["shutdown_handler.py"],
)

python_library(
    name="url_utils_lib",
    sources=["url_utils.py"],
    dependencies=[
        ':requests_req'
    ],
)

python_library(
    name="eloqua_utils_lib",
    sources=["eloqua_utils.py"],
    dependencies=[
        ':requests_req',
        #':eloqua_req'
        ':pyeloqua_lib',
    ],
)

python_library(
    name='pyeloqua_lib',
    sources=[
        'pyeloqua/bulk.py',
        'pyeloqua/error_handling.py',
        'pyeloqua/forms.py',
        'pyeloqua/pyeloqua.py',
        'pyeloqua/system_fields.py',
    ],
    dependencies=[
    ]
)



python_library(
    name="zookeeper_util_lib",
    sources=["zookeeper_util.py"],
    dependencies=[
        ':py3_util_lib',
        ':kazoo_req',
    ],
)

python_library(
    name="config_lib",
    sources=[
        "config.py"
    ],
    dependencies=[
        'src/python/houzz/common/yaml:yaml_lib',
        ':six_req',
    ],
)

python_library(
    name="redis_lib",
    sources=[
        "redis_utils.py",
        "redis_cluster_utils.py",
        "data_access/redis_con.py",
    ],
    dependencies=[
        ':redis_req',
        ':redis_py_cluster_req',
        ':config_lib',
    ],
)

python_library(
    name='threading_base_lib',
    sources=['threading_base.py'],
)

python_library(
    name="calendar_utils_lib",
    sources=[
        "calendar_utils.py"
    ],
    dependencies=[
        ':pytz_req'
    ],
)

# Currently used by houzz/common/aws.
# TODO: Upgrade to boto3.
python_requirement_library(
    name="boto_req", requirements=[
        python_requirement(name='boto', requirement='boto>=2.36.0,<3'),
    ],
)

python_requirement_library(
    name="boto3_req_for_py2", requirements=[
        python_requirement(name='boto3', requirement='boto3==1.17.112'),
        python_requirement(name='botocore', requirement='botocore==1.20.112'),
        python_requirement(name='certifi', requirement='certifi==2020.4.5.1'),
    ],
)

python_requirement_library(
    name="boto3_req", requirements=[
        python_requirement(name='boto3', requirement='boto3>=1.9.200'),
    ],
)

# Refactored from common/cassandra:cassandra_req.
python_requirement_library(
    name="cassandra_req", requirements=[
        python_requirement(name="cassandra-driver", requirement="cassandra-driver==3.14.0"),
    ]
)

# Python 3's concurrent.futures backported to Python 2.7.
# (This package is only available for Python 2, we disable the dependency when
# building for Python 3.)
python_requirement_library(
    name="futures_req", requirements=[
        python_requirement(name='futures', requirement='futures>=3.3.0;python_version<"3"'),
    ]
)

python_requirement_library(
    name="gevent_req", requirements=[
        python_requirement(name='gevent', requirement='gevent>=0.4.10'),
    ],
    dependencies=[':setuptools_req'],
)

python_requirement_library(
    name="jinja2_req", requirements=[
        # Jinja2 3.0 is Python3 only.
        python_requirement(name='Jinja2', requirement='Jinja2>=2.11.3,<3'),

        # Jinja2 requires MarkupSafe, but the latest version is Python3 only.
        python_requirement(name='MarkupSafe', requirement='MarkupSafe>=1.1.1,<2'),
    ],
)

python_requirement_library(
    name="kazoo_req", requirements=[
        python_requirement(name='kazoo', requirement='kazoo>=2.7.0,<3'),
    ],
)

python_requirement_library(
    name="paramiko_req", requirements=[
        python_requirement(name='paramiko', requirement='paramiko==1.17.1'),
    ],
)

python_requirement_library(
    name="psutil_req", requirements=[
        python_requirement(name='psutil', requirement='psutil==5.9.3'),
    ],
)

python_requirement_library(
    name="pymysql_req", requirements=[
        python_requirement(name='pymysql', requirement='pymysql==0.7.9'),
    ],
)

python_requirement_library(
    name="pymysql_req_py3", requirements=[
        python_requirement(name='pymysql', requirement='pymysql==1.0.2'),
    ],
)

python_requirement_library(
    name="pytz_req", requirements=[
        python_requirement(name='pytz', requirement='pytz==2018.5'),
    ],
)

# PyYAML 3.x is considered "insecure" (although no harm is done as long as you
# always call yaml.safe_load() instead of yaml.load()).
python_requirement_library(
    name="pyyaml_req", requirements=[
        python_requirement(name='pyyaml', requirement='pyyaml>=5.1,<6'),
    ],
)

python_requirement_library(
    name="queuelib_req", requirements=[
        python_requirement(name='queuelib', requirement='queuelib==1.5.0')
    ]
)

python_requirement_library(
    name="pyzmq_req", requirements=[
        python_requirement(name='pyzmq', requirement='pyzmq==15.2.0')
    ]
)

python_requirement_library(
    name="pyzmq_req_py36", requirements=[
        python_requirement(name='pyzmq', requirement='pyzmq==20.0.0')
    ]
)

python_requirement_library(
    name="readchar_req", requirements=[
        python_requirement(name='readchar', requirement='readchar>=2.0.1')
    ]
)

python_requirement_library(
    name="readchar_req_for_py2", requirements=[
        python_requirement(name='readchar', requirement='readchar<3.0.0')
    ]
)

python_requirement_library(
    name="redis_req", requirements=[
        python_requirement(name='redis', requirement='redis==2.10.6'),
    ],
)

python_requirement_library(
    name="redis_req_new", requirements=[
        python_requirement(name='redis', requirement='redis==3.5.3'),
    ],
)

# redis-py-cluster 2.0.0 requires redis 3.0, which conflicts with other redis
# requirements.
python_requirement_library(
    name="redis_py_cluster_req", requirements=[
        python_requirement(name='redis-py-cluster', requirement='redis-py-cluster>=1.3.1,<2'),
    ],
)

python_requirement_library(
    name="redis_py_cluster_req_new", requirements=[
        python_requirement(name='redis-py-cluster', requirement='redis-py-cluster==2.1.0'),
    ],
)

python_requirement_library(
    name="retry_req", requirements=[
        python_requirement(name='retry', requirement='retry'),

        # retry requires decorator, but the newer versions are Python3 only.
        python_requirement(name='decorator', requirement='decorator>=4.4.2,<5'),
    ]
)

# Fix the salt version for now, see https://github.com/saltstack/salt/issues/54871
python_requirement_library(
    name="salt_req", requirements=[
        python_requirement(name='salt', requirement='salt==3000.3'),
        python_requirement(name='Jinja2', requirement='Jinja2>=2.11.3,<3'),
        python_requirement(name='MarkupSafe', requirement='MarkupSafe>=1.1.1,<2'),
    ]
)

# 45 and above are Python3-only.
#
# NOTE: This is also listed as dependencies for some other packages because they
# depend on setuptools, but (i) setuptools>=45 requires Python 3, and (ii) pants
# has a broken dependency resolution logic that tries to download the latest
# version (unless another version is cached) - so a Python 2 build will randomly
# fail depending on which version is cached.
python_requirement_library(
    name="setuptools_req", requirements=[
        python_requirement(name='setuptools', requirement='setuptools<45'),
    ]
)

python_requirement_library(
    name="six_req", requirements=[
        python_requirement(name='six', requirement='six>=1.12.0,<2'),
    ]
)

# NOTE: Tornado 6.0+ doesn't even support Python 2 any more.
#
# 2020-01-06: I used to guard with 'python_full_version>="2.7.9"' (older Python
# cannot run Tornado 5.x) but for some reason it's no longer working - I'm not
# sure why.  Reverting to using 5.x everywhere.  (If you have to build for
# Ubuntu 14.04, please change the version to "tornado==4.2.1".)
python_requirement_library(
    name="tornado_req", requirements=[
        python_requirement(name='tornado', requirement='tornado>=5.1.1,<6'),
    ],
)

python_requirement_library(
    name="thrift_req", requirements=[
        python_requirement(name='thrift', requirement='thrift==0.10.0'),
    ],
)
