# We build components & client into a single library because they depend on each
# other: see client/__init__.py.
python_library(
    name='metric_components_client',
    sources=[
        'client/__init__.py',
        'client/client.py',
        'client/pusher.py',
        'client/utils.py',
        'components/aggregator.py',
        'components/component.py',
        'components/controller.py',
        'components/publisher.py',
        'components/receiver.py',
        'components/snapshoter.py',
        'components/prometheus_collector.py',
    ],
    dependencies=[
        "c2thrift/definitions/backends:metrics",
        "src/python/houzz/common/mem_disk_queue:mem_disk_queue_lib",
        "src/python/houzz/common/metric/common:metric_common",
        "src/python/houzz/common/metric/streamhist:streamhist",
        "src/python/houzz/common:shutdown_handler_lib",
        "src/python/houzz/common:queuelib_req",
        "src/python/houzz/common:six_req",
        "src/python/houzz/common:thrift_req",
        ":metric_client_req",
    ]
)

python_requirement_library(
    name="metric_client_req", requirements=[
        python_requirement(name='prometheus_client', requirement='prometheus_client==0.10.1')
    ],
)