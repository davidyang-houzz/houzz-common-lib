if __name__.endswith('thrift_gen2'):
    import sys
    sys.stderr.write('''*** Error: do not import thrift_gen2 directly.
*** To use auto-generated files during local development, run:
***     (...)/c2svc/gitutils/generate_py_thrift.py use_thrift_gen2=true
*** and import houzz.common.thrift_gen.(thrift module) as before.
''')
    raise ImportError
