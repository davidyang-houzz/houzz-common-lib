python_library(
    name="yaml_lib",
    sources=['__init__.py'],
    dependencies=[
        'src/python/houzz/common:pyyaml_req',
        'src/python/houzz/common:six_req',
    ],
)

python_tests(
    name="yaml_tests",
    dependencies=[
        ':yaml_lib',
        'src/python/houzz/common:pyyaml_req',
    ],
    sources=['yaml_tests.py'],
)
