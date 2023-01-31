from __future__ import absolute_import
import os, errno

import logging.handlers

import logging
import six

LOGGER_TYPE_COMMON_LOG = "common_log"
LOGGER_TYPE_BATCH_COMMON_LOG = "batch_common_log"


def mlevel(level):
    if 'info' == level.lower():
        return logging.INFO
    elif 'debug' == level.lower():
        return logging.DEBUG
    elif 'fatal' == level.lower():
        return logging.FATAL
    return logging.INFO


def _set_up_builtin_logger(logger_name, log_dir, level, host, port, formatter, capacity, encoding,
                           override_log_file_name=None, log_count=None):
    logger = logging.getLogger(logger_name) if _is_built_in_log(logger_name) else logging.getLogger()
    if override_log_file_name:
        log_file_name = override_log_file_name
    else:
        log_file_name = logger_name
    _configure_logger(logger, log_dir, log_file_name, logger_name, level=level, host=host, port=port,
                      formatter=formatter, capacity=capacity, encoding=encoding, log_count=log_count)
    return logger


def setUpLogger(log_dir, logger_name, level=None, capacity=102400, host=None, port=None, formatter=None, encoding=None,
                log_count=None):
    # set up the general logger
    defaultLogger = _set_up_builtin_logger(logger_name, log_dir, level, host, port, formatter, capacity, encoding,
                                           log_count=log_count, override_log_file_name='%s.log' % logger_name)

    # set up built-in loggers
    _set_up_builtin_logger(LOGGER_TYPE_COMMON_LOG, log_dir, level, host, port, formatter, capacity, encoding)
    _set_up_builtin_logger(LOGGER_TYPE_BATCH_COMMON_LOG, log_dir, level, host, port, formatter, capacity, encoding)

    return defaultLogger


def _is_built_in_log(logger_name):
    return logger_name in [LOGGER_TYPE_COMMON_LOG, LOGGER_TYPE_BATCH_COMMON_LOG]


def _configure_logger(logger, log_dir, log_file_name, logger_name, level=None, host=None, port=None, formatter=None,
                      capacity=None, encoding=None, log_count=None):
    if not log_dir:
        raise Exception(
            'Failed to configure log, no log_dir %s, logger_name: %s' %
            (log_dir, logger_name))
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                # race condition: log_dir didn't exist when running os.path.exists(), but does now
                pass
    if host:
        # Let's move it here, so that we import thrift only when necessary.
        from houzz.common.logger.hz_handlers import HZThriftHandler
        hdlr = HZThriftHandler(host, port, logger_name)
    else:
        full_file_name = os.path.join(log_dir, log_file_name)
        if _is_built_in_log(logger_name):
            hdlr = logging.handlers.TimedRotatingFileHandler(full_file_name, when='H',
                                                             backupCount=log_count or 24 * 3, encoding=encoding)
            # be consistent with php hourly file format
            hdlr.suffix = '%Y-%m-%d__%H'
        else:
            hdlr = logging.handlers.TimedRotatingFileHandler(full_file_name, when='midnight',
                                                             backupCount=log_count or 7, encoding=encoding)

    if not formatter:
        if _is_built_in_log(logger_name):
            formatter = logging.Formatter('%(message)s')
        else:
            formatter = logging.Formatter(
                '%(asctime)s %(process)d %(thread)d %(name)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    log_level = logging.INFO
    if isinstance(level, (str, six.text_type)):
        log_level = mlevel(level)
    mhdlr = logging.handlers.MemoryHandler(capacity, log_level, hdlr)
    logger.addHandler(mhdlr)
    logger.setLevel(log_level)


_all_local_logger = set()


def get_logger(log_dir, logger_name, level=None, capacity=102400, reset=False, encoding=None):
    global _all_local_logger

    if (not reset) and logger_name in _all_local_logger:
        return logging.getLogger(logger_name)

    if not log_dir:
        return None

    logger = logging.getLogger(logger_name)

    if _is_built_in_log(logger_name):
        log_file_name = logger_name
    else:
        log_file_name = '%s.log' % logger_name

    _configure_logger(logger, log_dir, log_file_name, logger_name, level=level, capacity=capacity, encoding=encoding)

    _all_local_logger.add(logger_name)
    return logger


def get_stdout_logger(log_dir, logger_name, level=None):
    '''
    Gets a logger that logs to standard output. Useful for kibana since that picks up logs that are output to stdout.

    :param logger_name:  Name of app
    :return:
    '''

    # early return if already set
    if logger_name in _all_local_logger:
        return logging.getLogger(logger_name)

    logger = get_logger(log_dir, logger_name, level)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s %(process)d %(thread)d %(name)s %(levelname)s %(message)s'))
    logger.addHandler(ch)

    return logger

