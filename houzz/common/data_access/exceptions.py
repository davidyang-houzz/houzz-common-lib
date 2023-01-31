

class DataAccessError(Exception):

    def __init__(self, error_message=None, origin_exception=None):
        if not error_message and origin_exception:
            error_message = str(origin_exception)
        super(DataAccessError, self).__init__(error_message)
        self.origin_exception = origin_exception


class DataConnectivityError(DataAccessError):
    pass
