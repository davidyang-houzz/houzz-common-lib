python_tests(name='mem_disk_queue_tests',
  dependencies=[
    "src/python/houzz/common/mem_disk_queue:mem_disk_queue_lib",
    ":mem_disk_queue_tests_req",
    "src/python/houzz/common:six_req",
  ],
  sources=globs("*_tests.py"),
)

python_requirement_library(
    name="mem_disk_queue_tests_req",
    requirements=[
        python_requirement(name="mock", requirement="mock>=1.3"),
    ]
)
