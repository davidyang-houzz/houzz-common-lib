from functools import wraps
from flask import Response


def read_only(is_read_only):
    def decorator(func):
        @wraps(func)
        def _wrap(*args, **kwargs):
            if is_read_only:
                return Response("Server is in read_only mode, performing this request is forbiddened", status=403)
            return func(*args, **kwargs)
        return _wrap
    return decorator
