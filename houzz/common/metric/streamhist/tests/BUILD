python_tests(name='streamhist_tests',
  dependencies=[
    "src/python/houzz/common/metric/streamhist:streamhist",
    ":streamhist_tests_req",
    "src/python/houzz/common:six_req",
  ],
  sources=globs("test_*.py"),
)

python_requirement_library(
    name="streamhist_tests_req",
    requirements=[
        python_requirement(name="numpy", requirement="numpy>=1.11,<1.17"),
    ]
)
