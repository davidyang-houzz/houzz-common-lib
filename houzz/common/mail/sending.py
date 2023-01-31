from __future__ import absolute_import
import logging
import mimetypes
import os
import smtplib
import hashlib

from email import encoders
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.Utils import formatdate
import six
from six.moves import range


class EmailSender(object):

    def __init__(self, logger=None, config=None):
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        if config:
            self._config = config
        else:
            from houzz.common.config import get_config
            self._config = get_config()

    # add attachments to the message.
    def add_attachments(self, msg, attachments):
        """ add attachments to the message.
            attachments contain a list of filenames to be added.
        """
        for f in attachments:
            # guess content type based on extension
            ctype, encoding = mimetypes.guess_type(f)
            if ctype is None or encoding is not None:
                # No guess could be made, or the file is encoded (compressed),
                # so use a generic bag-of-bits type.
                ctype = 'application/octet-stream'
            maintype, subtype = ctype.split('/', 1)
            with open(f, 'rb') as fp:
                if maintype == 'text':
                    # Note: we should handle calculating the charset
                    attach = MIMEText(fp.read(), subtype, 'utf-8')
                elif maintype == 'image':
                    attach = MIMEImage(fp.read(), subtype)
                else:
                    attach = MIMEBase(maintype, subtype)
                    attach.set_payload(fp.read())
                    # Encode the payload using Base64
                    encoders.encode_base64(attach)
            # Set the filename parameter
            attach.add_header(
                'Content-Disposition', 'attachment',
                filename=os.path.basename(f))
            msg.attach(attach)

    # send text only email
    def send_text_email(
            self, mailfrom, mailtos, subject, body, mail_password=None,
            mail_host=None, mail_port=None, mail_user=None, tls=False):
        """ send text email.
            mailtos is an array of one or more email addresses
        """
        # compose the mail..
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Date'] = formatdate()
        msg['Subject'] = subject
        msg['From'] = mailfrom
        if isinstance(mailtos, list):
            msg['To'] = ', '.join(mailtos)
        elif isinstance(mailtos, str) or isinstance(mailtos, six.text_type):
            msg['To'] = mailtos
        else:
            raise ValueError('unknown type of mailtos. value:%s' % mailtos)

        # and post it..
        self._post_smtp_mail(
            mailfrom, mailtos, msg, mail_host, mail_port, mail_user,
            mail_password, tls)

    # send a html email
    def send_html_email(
            self, mailfrom, mailtos, subject, body, text_content=None,
            custom_header=None, mail_host=None, mail_port=None,
            mail_user=None, mail_password=None, attachments=None, tls=False):
        """ send html email.
            mailtos is an array of one or more email addresses
        """
        # compose the mail..
        msg = MIMEMultipart('mixed')
        msg['Date'] = formatdate()
        msg['Subject'] = subject
        msg['From'] = mailfrom
        if isinstance(mailtos, list):
            msg['To'] = ', '.join(mailtos)
        elif isinstance(mailtos, str) or isinstance(mailtos, six.text_type):
            msg['To'] = mailtos
        else:
            raise ValueError('unknown type of mailtos. value:%s' % mailtos)
        htmlMsg = MIMEText(body, 'html', 'utf-8')
        if text_content:
            textMsg = MIMEText(text_content, 'plain', 'utf-8')
            bodyMsg = MIMEMultipart('alternative')
            bodyMsg.attach(textMsg)
            bodyMsg.attach(htmlMsg)
            msg.attach(bodyMsg)
        else:
            msg.attach(htmlMsg)
        if attachments:
            self.add_attachments(msg, attachments)
        if custom_header:
            (header_key, header_value) = custom_header.split(':', 1)
            msg.add_header(header_key, header_value)
        # and post it..
        self._post_smtp_mail(
            mailfrom, mailtos, msg, mail_host, mail_port, mail_user,
            mail_password, tls=tls)

    def get_email_domain(self, email_address):
        pos = email_address.find('@')
        if pos == -1:
            return ""
        else:
            return email_address[pos + 1:]

    def _should_use_thirdparty_mailserver(self, mailtos):
        result = False
        mandril_config = self._config.get('MANDRILL_CONFIG', {})
        if mandril_config and mandril_config.get('ENABLED_PERCENTAGE', 0) > 0:
            address = mailtos
            if isinstance(mailtos, list):
                address = mailtos[0]
            domain = self.get_email_domain(address)
            domains_percentage_applied = mandril_config.get(
                'DOMAINS_PERCENTAGE_APPLIED', None)
            if domain in domains_percentage_applied:
                percentage = domains_percentage_applied[domain]
                mod_value = int(hashlib.md5(address).hexdigest(), 16) % 100
                result = mod_value < percentage
        return result

    def _post_smtp_mail(
            self, mailfrom, mailtos, msg, mail_host=None, mail_port=None,
            mail_user=None, mail_password=None, tls=False):
        """ post a composed mail in msg to an smtp server in mail_host
            Note:internal function, do not call this directly, use
            send_html_email/send_text_email instead
        """

        mandril_config = self._config.get('MANDRILL_CONFIG', {})
        mail_config = self._config.get('MAIL_CONFIG', {})
        if self._should_use_thirdparty_mailserver(mailtos):
            mail_host = mandril_config.get('MAIL_HOST', None)
            mail_port = mandril_config.get('MAIL_PORT', None)
            mail_user = mandril_config.get('MAIL_USER', None)
            mail_password = mandril_config.get('MAIL_PASSWORD', None)
            mail_timeout = mandril_config.get('MAIL_TIMEOUT', None)
            number_of_retries = mandril_config.get('NUMBER_OF_RETRIES', None)
        else:
            if not mail_host:
                mail_host = mail_config.get('MAIL_HOST', None)
            if not mail_port:
                mail_port = mail_config.get('MAIL_PORT', None)
            if not mail_user:
                mail_user = mail_config.get('MAIL_USER', None)
            if not mail_password:
                mail_password = self._get_mail_password()

            mail_timeout = mail_config.get('MAIL_TIMEOUT', None)
            number_of_retries = mail_config.get('NUMBER_OF_RETRIES', None)

        # to deal with the case of multiple servers
        hosts = mail_host.split(';')
        for attempt_cnt in range(0, number_of_retries):
            for mail_host in hosts:
                try:
                    self._logger.debug(
                        ('_post_smtp_mail connecting, host: %s, ' +
                            'port: %s, user: %s') %
                        (mail_host, mail_port, mail_user, ))
                    smtp = smtplib.SMTP(
                        mail_host, mail_port, None, mail_timeout)
                    if tls:
                        smtp.starttls()
                    smtp.login(mail_user, mail_password)
                    self._logger.debug(
                        '_post_smtp_mail sending, from: %s to: %s' %
                        (mailfrom, mailtos, ))
                    smtp.sendmail(mailfrom, mailtos, msg.as_string())
                    smtp.close()
                except Exception as e:
                    self._logger.exception(
                        ('_post_smtp_mail failed to connect, retry ' +
                            'other hosts, error: %s') %
                        (e, ))
                else:
                    self._logger.debug(
                        "posted mail from '%s' to '%s' to the smtp host %s",
                        mailfrom, mailtos, mail_host)
                    return
        self._logger.info(
            'failed to post the mail to the smtp host %s', mail_host)
        return False

    MAIL_SECRET_PATH = '/root/.mail_secret'

    def _get_mail_password(self, path=None):
        """ get mail server login password for outgoing mail
        """
        return open(path or self.MAIL_SECRET_PATH, 'r').read().strip()
