###########################################################################
#                              NOTE                                       #
# This library is forked from https://github.com/carsonfarmer/streamhist. #
# The original project has multiple bugs that have been fixed here.       #
###########################################################################

python_library(
    name="streamhist",
    sources=[
        '__init__.py',
        'histogram.py',
        'utils.py',
        'weighted_histogram.py',
    ],
    dependencies=[
        ":streamhist_req",
        "src/python/houzz/common:six_req",
    ]
)

python_requirement_library(
    name="streamhist_req", requirements=[
        python_requirement(name="sortedcontainers", requirement="sortedcontainers>=2.0.5"),
    ]
)
