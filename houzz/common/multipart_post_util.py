# A helper module to create multipart/form-data body for POST.
#
# Normally, the requests library can handle multipart/form-data just fine.
# However, there are some situations when we want to use other libraries (e.g.,
# tornado.httpclient.HTTPRequest).  So it's handy to be able to generate a
# multipart POST body.
#
# Main logic stolen from: https://github.com/ActiveState/code/blob/master/recipes/Python/146306_Http_client_POST_using/recipe-146306.py
# See also: https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/POST

from __future__ import absolute_import
import uuid
import six
from six.moves import range

# In Python 3, change all strings ("str") to "bytes".
# (There's no "unicode" type.)
# In Python 2, change all "unicode" strings to "str" (= "bytes").
def to_bytes(s):
    try:
        return s.encode('utf-8')
    except:
        return s

def encode_multipart_form(items):
    """
    Returns (content_type, body).

    'items' can be either a dict or list of tuples.

    If it's a dict, key is the "name" and value is either a string or a tuple
    (filename, value).  E.g.,
        {
            'field1': 'upload',
            'data': ('x.py', '#!/usr/bin/python\n blah blah...')
        }

    If it's a list, then each item is either a tuple (name, value) or (name,
    filename, value).

    TODO: This may not work if 'name' or 'filename' contains non-alphanumeric
          characters.
    """

    # Change all data to "bytes".
    itemlist = []
    if type(items) == dict:
        for key, value in six.iteritems(items):
            if type(value) in [list, tuple]:
                filename, value = value
            else:
                filename = ''
            itemlist.append((key, filename, value))
    else:
        for item in items:
            try:
                key, filename, value = item
            except:
                key, value = item
                filename = ''
            itemlist.append((key, filename, value))

    itemlist = [[to_bytes(s) for s in item] for item in itemlist]

    # Generate a random boundary string: the chance that it exists in the data
    # is astronomically low.
    for i in range(5):
        boundary = to_bytes(str(uuid.uuid4()))
        if not any(boundary in s for item in itemlist for s in item):
            break
    else:
        # This should never happen!
        assert None, 'The impossible happened: cannot find a boundary string!'

    lines = []
    for name, filename, value in itemlist:
        lines.append(b'--' + boundary)
        if filename:
            lines.append(
                b'Content-Disposition: form-data; name="%s"; filename="%s"' %
                (name, filename))
        else:
            lines.append(
                b'Content-Disposition: form-data; name="%s"' % name)
        lines.append(b'')
        lines.append(value)
    lines.append(b'--' + boundary + b'--')
    lines.append(b'')

    content_type = b'multipart/form-data; boundary="%s"' % boundary
    return content_type, b'\r\n'.join(lines)
