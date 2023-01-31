python_tests(name="metric_common_tests",
    dependencies=[
        "src/python/houzz/common/metric/common:metric_common",
        "src/python/houzz/common/metric/client:metric_client",
        "src/python/houzz/common/metric/components:metric_components",
        "c2thrift/definitions/backends:metrics",
        ":metric_common_test_utils",
        "src/python/houzz/common:six_req",
    ],
    sources=globs("*_tests.py"),
)

python_library(
    name="metric_common_test_utils",
    sources=[
        "common_test_utils.py",
    ],
    dependencies=[
        "c2thrift/definitions/backends:metrics",
        "src/python/houzz/common/mem_disk_queue:mem_disk_queue_lib",
        "src/python/houzz/common/metric/common:metric_common",
        ":metric_common_test_utils_req",
        "src/python/houzz/common:six_req",
    ]
)

python_requirement_library(
    name="metric_common_test_utils_req",
    requirements=[
        python_requirement(name="mock", requirement="mock>=1.3"),
    ]
)
