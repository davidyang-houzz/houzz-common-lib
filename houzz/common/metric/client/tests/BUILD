python_tests(name="metric_client_tests",
    dependencies=[
        "src/python/houzz/common/metric/client:metric_client",
        "src/python/houzz/common/metric/components/tests:metric_components_test_utils",
        "src/python/houzz/common/metric/common/tests:metric_common_test_utils",
        "c2thrift/definitions/backends:metrics",
        ":metric_client_tests_req",
        "src/python/houzz/common:six_req",
    ],
    sources=globs("*_tests.py"),
)

python_requirement_library(
    name="metric_client_tests_req",
    requirements=[
        python_requirement(name="mock", requirement="mock>=1.3"),
    ]
)
