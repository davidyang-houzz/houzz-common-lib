from __future__ import absolute_import
import six
__author__ = "zviki,menglei"

import logging
import socket
import datetime

from houzz.common.logger.separate import SeparateLogger

logger = logging.getLogger(__name__)


class CommonLogger(SeparateLogger):

    _logger_name = 'common_log'
    _base_file_name = 'batch_common_log'

    EVENT_TYPE_PUSH_CLIENT_SUBSCRIBE = 'push_client_subscribe'
    EVENT_TYPE_PUSH_CLIENT_PROCESSED = 'push_client_processed'
    EVENT_TYPE_PUSH_BROADCAST_SEGMENT_SENT = 'push_broadcast_segment_sent'
    EVENT_TYPE_PUSH_BROADCAST_MESSAGE_SENT = 'push_broadcast_message_sent'
    EVENT_TYPE_PUSH_PERSONAL_MESSAGE_SENT = 'push_personal_message_sent'
    EVENT_TYPE_PUSH_USER_PROCESSED = 'push_user_processed'
    EVENT_TYPE_PUSH_DELIVERED_BROADCAST = 'push_delivered_broadcast'
    EVENT_TYPE_PUSH_DELIVERED_PERSONAL = 'push_delivered_personal'
    EVENT_TYPE_PUSH_DELIVERED_SILENT = 'push_delivered_silent'
    EVENT_TYPE_PUSH_CLIENT_CHANGED_STATUS = 'push_client_change_status'
    EVENT_TYPE_REGISTER_ABTEST = "register_abtest"
    EVENT_TYPE_PUSH_SOCKET_SEND = 'push_socket_send'
    EVENT_TYPE_PADDY_SEND_EMAIL = 'paddy_send_email'

    def __init__(self, **kwargs):
        super(CommonLogger, self).__init__(**kwargs)

        self._test_selections = dict()

        try:
            self._server_ip = socket.gethostbyname(socket.gethostname())
        except Exception as ex:
            logger.warn(
                'CommonLogger failed to get host ip, error: %s' % (ex, ))
            self._server_ip = '127.0.0.1'

    def log_test_selection(self, test_name, test_variant, mod_type, object_id=None):
        self._test_selections[test_name] = test_variant
        event_metadata = {
            "test_name": test_name,
            "test_bucket": test_variant,
            "test_mod_by": mod_type,
            "object_id": object_id
        }
        self.log_one_event(CommonLogger.EVENT_TYPE_REGISTER_ABTEST, event_metadata, {})

    def refresh_test_selection(self):
        self._test_selections = dict()

    def log_one_event(self, event_type, event_metadata, request_common,
                      test_selections=None):
        if event_metadata:
            updated_values = {}
            for key, value in six.iteritems(event_metadata):
                if isinstance(value, datetime.datetime):
                    updated_values[key] = value.strftime('%Y-%m-%d_%H:%M:%S')
            if updated_values:
                event_metadata.update(updated_values)
        if not test_selections:
            test_selections = self._test_selections
        event_data = {
            'requestCommon': request_common,
            'testSelections': test_selections or {},
            'userEvents': [
                {
                    'count': 0,
                    'eventType': event_type,
                    'metadata': event_metadata
                }
            ]
        }
        self.logger.info(event_data)
