from __future__ import absolute_import
import datetime
import functools
import re
import yaml
from yaml.representer import SafeRepresenter
import six

try:
    from yaml import (CDumper as Dumper, CSafeDumper as SafeDumper,
                      CLoader as Loader, CSafeLoader as SafeLoader)
except ImportError:
    from yaml import Dumper, SafeDumper, Loader, SafeLoader

DATE_HOUR_FMT = '%Y-%m-%dT%H'
DATE_HOUR_PATTERN = re.compile('\d{4}-\d{2}-\d{2}T\d{2}')
DATE_HOUR_IDENTIFIER = '!datehour'


def date_hour_representer(yaml_dumper, datehour):
    return yaml_dumper.represent_scalar(
        DATE_HOUR_IDENTIFIER,
        datehour.strftime(DATE_HOUR_FMT))


def date_hour_constructor(yaml_loader, node):
    value = yaml_loader.construct_scalar(node)
    return datetime.datetime.strptime(value, DATE_HOUR_FMT)


for loader, dumper in ((Loader, Dumper), (SafeLoader, SafeDumper)):
    yaml.add_representer(
        datetime.datetime,
        date_hour_representer,
        Dumper=dumper)
    yaml.add_constructor(
        DATE_HOUR_IDENTIFIER,
        date_hour_constructor,
        Loader=loader)
    yaml.add_implicit_resolver(DATE_HOUR_IDENTIFIER, DATE_HOUR_PATTERN,
                               Dumper=dumper, Loader=loader)

class PrettyDumper(Dumper): pass
PrettyDumper.add_representer(str, SafeRepresenter.represent_str)
if six.PY2:
    PrettyDumper.add_representer(six.text_type, SafeRepresenter.represent_unicode)
PrettyDumper.add_representer(tuple, SafeRepresenter.represent_list)
PrettyDumper.add_representer(set, SafeRepresenter.represent_list)

load = functools.partial(yaml.load, Loader=Loader)
safe_load = functools.partial(yaml.load, Loader=SafeLoader)
dump = functools.partial(yaml.dump, Dumper=Dumper)
safe_dump = functools.partial(yaml.dump, Dumper=SafeDumper)

# See: http://stackoverflow.com/questions/1950306/pyyaml-dumping-without-tags
def pretty_dump(data, stream=None, Dumper=PrettyDumper, default_flow_style=False, encoding='utf8', allow_unicode=True, **kwargs):
    """Dump prettified yaml for human.
    @param  data  any
    @param* stream  file
    @raise

    For example, {"k1":"v1","k2":"v2"} will be printed as "k1:v1\nk2:v2\n" instead of JSON-like string.
    """
    return yaml.dump(data, stream=stream,
        encoding=encoding,
        default_flow_style=default_flow_style,
        allow_unicode=allow_unicode,
        Dumper=PrettyDumper,
        *kwargs)
