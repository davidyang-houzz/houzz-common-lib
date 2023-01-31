from __future__ import absolute_import
__author__ = 'zhen'

import logging
from twitter.common import app, options

from houzz.common import email_utils
from houzz.common.module.env_module import EnvModule

DEFAULT_DELIMITER = ','

class EmailModule(app.Module):
    """
    This is the app options wrapper for email_utils
    """

    OPTIONS = {
        'email_from_overrides': options.Option(
            '-m',
            '--email_from_overrides',
            dest='email_from_overrides',
            type='str',
            help='Change the send-from email address to input email address (only one)'
        ),
        'email_to_overrides': options.Option(
            '-t',
            '--email_to_overrides',
            dest='email_to_overrides',
            type='str',
            help='Change the send-to email addresses to input emails addresses (separated by comma only)'
        ),
        'email_bcc_overrides': options.Option(
            '-b',
            '--email_bcc_overrides',
            dest='email_bcc_overrides',
            type='str',
            help='Change the bcc email addresses to input emails addresses (separated by comma only)'
        ),
        'send_email': options.Option(
            '-s',
            '--send_email',
            dest='send_email',
            help='By Setting this, actual email will be sent',
            action='store_true',
            default=False
        )
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        self._env = None

        self._options = None

        self._log = logging.getLogger(self.__class__.__name__)

        app.register_module(EnvModule())
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=[
                EnvModule.full_class_path(),
            ],
            description="Email Module")

    def setup_function(self):
        self._options = app.get_options()

        self._log.info("Options: {}".format(self._options))

        self._env = EnvModule().env

    def send_email(self, mail_from, mail_tos, subject, body, text_content=None, custom_header=None, mail_host=None,
                   mail_port=None, mail_user=None, mail_password=None, attachments=None, tls=False, mail_host_type=None,
                   is_html=True, is_bulk=False, is_shared=False, bcc=None):

        if self._options.email_from_overrides:
            mail_from = self._options.email_from_overrides

        if self._options.email_to_overrides:
            mail_tos = self._options.email_to_overrides.split(DEFAULT_DELIMITER)

        if self._options.email_bcc_overrides:
            bcc = self._options.email_bcc_overrides.split(DEFAULT_DELIMITER)

        is_dryrun = not self._options.send_email

        result_msg = "send email to %s from %s and cc %s" % (mail_tos, mail_from, bcc)

        try:
            response = email_utils.send_email(mail_from, mail_tos, subject, body, text_content, custom_header, mail_host,
                                              mail_port, mail_user, mail_password, attachments, tls, mail_host_type,
                                              is_html, is_bulk, is_shared, bcc, is_dryrun)

            return response, result_msg
        except Exception as e:
            self._log.exception(e)
            result_msg = "exception happens, fail to send email to %s from %s and cc %s" % (mail_tos, mail_from, bcc)
            return False, result_msg
