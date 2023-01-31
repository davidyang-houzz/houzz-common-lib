# Utility functions for Python2/3 compatibility.
## py3-no-migrate-begin

import sys

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

# In Python 2, 'bytes' and 'str' are the same, so this is a no-op.
# In Python 3, convert 'bytes' to 'str'.
#
# Basic idea is stolen from six.ensure_str, etc.
def bytes2str(b, encoding='utf-8', errors='strict'):
    assert type(b) == bytes
    return b if PY2 else b.decode(encoding, errors)

# The other direction.
def str2bytes(s, encoding='utf-8', errors='strict'):
    assert type(s) == str
    return s if PY2 else s.encode(encoding, errors)

# In Python 2, convert 'str' to 'unicode'.
# In Python 3, they are the same, so this is a no-op.
def str2unicode(s, encoding='utf-8', errors='strict'):
    assert type(s) == str
    return s if PY3 else s.decode(encoding, errors)

# The other direction.
def unicode2str(u, encoding='utf-8', errors='strict'):
    if PY2:
        assert type(u) == unicode
        return u.encode(encoding, errors)
    else:
        assert type(u) == str
        return u

## py3-no-migrate-end
