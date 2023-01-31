from __future__ import absolute_import
import os
import json
import datetime
import logging
import logging.handlers
import six


class SeparateLogger(object):

    _logger_name = 'default'
    _base_file_name = None
    _logger_level = logging.INFO

    def __init__(self, log_dir=None,
                 logger_name=None, base_file_name=None, logger_level=None,
                 internal_logger=None):
        self._internal_logger = internal_logger or logging.getLogger(__name__)
        self._internal_logger.debug(
            'SeparateLogger initializing (%s)' % (self.__class__.__name__, ))
        if not log_dir:
            try:
                from houzz.common.config import get_config
                import os
                K8S_POD_NAME = 'K8S_POD_NAME'
                if K8S_POD_NAME in os.environ:
                    log_dir = '/home/clipu/c2/log/' + os.environ[K8S_POD_NAME]
                else:
                    config = get_config()
                    log_dir = config.get('PYTHON_HOME', {}) \
                        .get('LOGGING', {}).get('LOG_ROOT', {})
                self._internal_logger.info('SeparateLogger dir: [%s]' % log_dir)
            except:
                self._internal_logger.exception(
                    'SeparateLogger failed to get log_dir')
        if not log_dir:
            import tempfile
            log_dir = tempfile.gettempdir()
            self._internal_logger.error(
                'SeparateLogger has no log_dir, will use temp dir: %s' %
                (log_dir, ))
        self.log_dir = log_dir
        self._pid = None
        if logger_name:
            self._logger_name = logger_name
        if base_file_name:
            self._base_file_name = base_file_name
        if logger_level:
            self._logger_level = logger_level
        self._init_file_handler()
        self._internal_logger.debug(
            ('SeparateLogger initialized, logger_name: %s, log_dir: %s, ' +
                'base_file_name: %s, logger_level: %s') %
            (self._logger_name, self.log_dir, self._base_file_name,
                self._logger_level, ))

    def _init_file_handler(self):
        self._pid = os.getpid()
        self._log_handler = SeparateLogger.InternalLogFileHandler(
            self.log_dir, self._base_file_name, self._pid)
        self._log_handler.setFormatter(SeparateLogger.InternalLogFormatter(
            self, self._pid))
        self._logger = logging.Logger(
            self._logger_name, level=self._logger_level)
        self._logger.addHandler(self._log_handler)

    def _check_pid(self):
        if self._pid != os.getpid():
            self._init_file_handler()

    @property
    def logger(self):
        self._check_pid()
        return self._logger

    def _prepare_record(self, record):
        return record

    class MessageJsonEncoder(json.JSONEncoder):

        _EPOCH = datetime.datetime(1970, 1, 1)

        def default(self, obj):
            if isinstance(obj, datetime.datetime):
                return int(round((obj - self._EPOCH).total_seconds()))
            if isinstance(obj, datetime.timedelta):
                return int(round(
                    (datetime.datetime.now() + obj - self._EPOCH)
                    .total_seconds()))
            return json.JSONEncoder.default(self, obj)

    def _format_record(self, record):
        try:
            return json.dumps(record.msg, cls=self.MessageJsonEncoder)
        except:
            self._internal_logger.exception('Failed to dump recod')
        try:
            return json.dumps(self._clean_data(record.msg))
        except:
            self._internal_logger.exception('Failed to clean and dump recod')
        return str(record.msg)

    _BASE_TYPES = (six.string_types, int, float, int, bool, )
    _LIST_TYPES = (list, tuple, )

    def _clean_data(self, data, key_path="root"):
        result = None
        if data is None or isinstance(data, self._BASE_TYPES):
            result = data
        elif isinstance(data, dict):
            result = dict((
                (key, self._clean_data(value, '%s>%s' % (key_path, key, ),))
                for key, value in six.iteritems(data)))
        elif isinstance(data, self._LIST_TYPES):
            result = [self._clean_data(item, key_path) for item in data]
        else:
            self._internal_logger.warn(
                ('_clean_data coercing to string, key_path: %s, ' +
                    'data: %s, data type: %s') %
                (key_path, data, type(data), ))
            result = str(data)
        return result

    class InternalLogFormatter(logging.Formatter):

        def __init__(self, handler, pid):
            self._handler = handler
            self._pid = pid
            super(SeparateLogger.InternalLogFormatter, self).__init__()

        def format(self, record):
            record = self._handler._prepare_record(record) or record
            return self._handler._format_record(record)

    class InternalLogFileHandler(logging.handlers.BaseRotatingHandler):

        def __init__(self, log_dir, base_file_name, pid):
            self._base_filename = '%s/%s' % (log_dir, base_file_name,)
            self._suffix = "%y_%m_%d__%H" + ('__py%s' % (pid,))
            filename = self._get_current_time_filename()
            logging.handlers.BaseRotatingHandler.__init__(
                self, filename, 'a', encoding='utf8', delay=False)

        def _get_current_time_filename(self):
            now = datetime.datetime.now()
            self._file_hour = now.hour
            self._file_date = now.date()
            return '%s_%s' % (self._base_filename, now.strftime(self._suffix),)

        def shouldRollover(self, record):
            now = datetime.datetime.now()
            return now.hour != self._file_hour or now.date() != self._file_date

        def doRollover(self):
            if self.stream:
                self.stream.close()
                self.stream = None
            self.baseFilename = self._get_current_time_filename()
            self.stream = self._open()
