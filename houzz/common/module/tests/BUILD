python_tests(
    name="module_tests",
    dependencies=[
        "src/python/houzz/common/module:mpa_campaigns_service_module_lib",
        'c2thrift/definitions/backends:mpa_campaigns_service',
        ":module_test_req",
    ],
    sources=[
        'mpa_campaigns_service_module_tests.py',
    ]
)

python_requirement_library(
    name="module_test_req",
    requirements=[
        python_requirement(name='responses', requirement='responses==0.10.4'),
        python_requirement(name="mock", requirement="mock>=1.3")
    ]
)
