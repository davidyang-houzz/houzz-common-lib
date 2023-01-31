python_tests(name="syncer_tests",
    dependencies=[
        ":syncer_tests_req",
        "src/python/houzz/common/aws/s3/syncer:syncer_lib",
    ],
    sources=globs("*_tests.py"),
)

python_requirement_library(
    name="syncer_tests_req",
    requirements=[
        python_requirement(name="mock", requirement="mock>=1.3"),
        python_requirement(name="moto", requirement="moto>=0.4.30"),
    ]
)
