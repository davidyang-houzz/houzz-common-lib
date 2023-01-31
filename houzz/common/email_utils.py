#!/usr/bin/python

""" Collection of email utilities.
"""

from __future__ import absolute_import
import time
import email
import hashlib
import imaplib
import logging
import mimetypes
import os
import smtplib
import tempfile
import bisect
import random

from . import config
from datetime import datetime
from email import encoders
from email.header import Header
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, formataddr
import six
from six.moves import range

logger = logging.getLogger(__name__)
Config = config.get_config()


MAIL_HOST_REGULAR_OPT_IN = 1
MAIL_HOST_ONE_OFF_OPT_IN = 2
MAIL_HOST_TYPE_BAD_DOMAIN = 3
MAIL_HOST_TYPE_OPT_OUT = 4
MAIl_HOST_TYPE_TRIGGERED = 5
MAIL_HOST_TYPE_DEFAULT = 6

ALL_EPN_MAIL_TYPES = {
    MAIL_HOST_TYPE_BAD_DOMAIN: "EPN_MAIL_BAD_DOMAIN_CONFIG",
    MAIL_HOST_ONE_OFF_OPT_IN: "EPN_MAIL_BULK_ONE_OFF_CONFIG",
    MAIL_HOST_TYPE_OPT_OUT: "EPN_MAIL_BULK_ONE_OFF_CONFIG",
    MAIL_HOST_REGULAR_OPT_IN: "EPN_MAIL_BULK_REGULAR_CONFIG",
    MAIl_HOST_TYPE_TRIGGERED: "EPN_MAIL_TRIGGERED_CONFIG",
    MAIL_HOST_TYPE_DEFAULT: "EPN_MAIL_DEFAULT_CONFIG",
}


class MailHost(object):

    def __init__(self, host, port, weight, username, password, retries, timeout, tls, username_as_key=False):
        self._host = host
        self._port = port
        self._weight = weight
        self._username = username
        self._password = password
        self._retries = retries
        self._timeout = timeout
        self._tls = tls
        self._username_as_key = username_as_key


    @property
    def port(self):
        return self._port

    @property
    def host(self):
        return self._host

    @property
    def retries(self):
        return self._retries

    @property
    def password(self):
        return self._password

    @property
    def username(self):
        return self._username

    @property
    def timeout(self):
        return self._timeout

    @property
    def tls(self):
        return self._tls

    @property
    def weight(self):
        return self._weight

    @property
    def key(self):
        if self._username_as_key:
            return "%s@%s:%s" % (self.username, self.host, self.port)
        else:
            return "%s:%s" % (self.host, self.port)


class MailConfig(object):

    __mail_config = {}
    __mail_host_map = {}
    __init = False

    @classmethod
    def init(cls):
        for mail_type, config_name in ALL_EPN_MAIL_TYPES.items():
            type_config = Config.get(config_name, None)
            if not type_config:
                logger.warning("unable to find the mail host for config %s", config_name)
                continue
            mail_hosts_weight_config = type_config.get("MAIL_HOSTS_CONFIG_WEIGHT", None)
            if not mail_hosts_weight_config:
                logger.warning("unable to load mail_hosts_weight_config for  %s", config_name)
                raise Exception("unable to load config for mail type %s" % mail_type)
            prev = 0
            mail_hosts = []
            for vendor_config_name, weight in mail_hosts_weight_config.items():
                weight += prev
                vendor_hosts = cls.load_vendor_config(vendor_config_name)
                mail_hosts.append((vendor_hosts, weight))
                prev = weight
            cls.__mail_config[mail_type] = mail_hosts
        cls.__init = True

    @classmethod
    def load_vendor_config(cls, config_name):
        hosts_config = Config.get(config_name)
        if not hosts_config:
            logger.warning("unable to load mail host config from section %s", config_name)
            raise Exception("unable to load mail hosts config from section %s" % config_name)
        mail_hosts_dict = hosts_config.get("MAIL_HOSTS", None)
        if not mail_hosts_dict:
            logger.warning("unable to load MAIL_HOSTS from section %s", config_name)
            raise Exception("unable to load MAIL_HOSTS from section %s" % config_name)
        port = hosts_config.get("MAIL_PORT", 25)
        username = hosts_config.get("MAIL_USER", "")
        password = hosts_config.get("MAIL_PASSWORD", "")
        if not username or not password:
            raise Exception("unable to mail host username and password defined in the section %s" % config_name)
        retries = hosts_config.get("NUMBER_OF_RETRIES", 3)
        timeout = hosts_config.get("MAIL_TIMEOUT", 3)
        tls = hosts_config.get("TLS", False)
        mail_hosts_list = []
        prev = 0
        for host, weight in mail_hosts_dict.items():
            weight += prev
            user_and_host = host.split('@')
            username_as_key = False
            if len(user_and_host) == 2:
                username = user_and_host[0]
                host = user_and_host[1]
                username_as_key = True
            host_and_optional_port = host.split(':')
            if len(host_and_optional_port) == 2:
                host = host_and_optional_port[0]
                port = int(host_and_optional_port[1])
            mail_host = MailHost(host=host, port=port, weight=weight,
                                 username=username, password=password,
                                 retries=retries, timeout=timeout, tls=tls,
                                 username_as_key=username_as_key)
            mail_hosts_list.append(mail_host)
            cls.__mail_host_map[mail_host.key] = mail_host
            prev = weight
        return mail_hosts_list

    @classmethod
    def get_mail_host_by_type(cls, mail_type):
        if not cls.__init:
            cls.init()
        mail_hosts = cls.__mail_config.get(mail_type) or cls.__mail_config.get(MAIL_HOST_TYPE_DEFAULT)
        if not mail_hosts:
            return None
        total_weight = mail_hosts[-1][1]
        r = random.random() * total_weight
        keys = [e[1] for e in mail_hosts]
        vendor_hosts = mail_hosts[bisect.bisect_right(keys, r)][0]
        vendor_total_weight = vendor_hosts[-1].weight
        r = random.random() * vendor_total_weight
        keys = [e.weight for e in vendor_hosts]
        host = vendor_hosts[bisect.bisect_right(keys, r)]
        return host

    @classmethod
    def get_mail_host_by_name_and_port(cls, mail_host, mail_port):
        key = "%s:%s" % (mail_host, mail_port)
        return cls.__mail_host_map.get(key, None)

MailConfig.init()

class EmailException(Exception):
    """ email exception """
    pass


# add attachments to the message.
def add_attachments(msg, attachments):
    """ add attachments to the message.
        attachments contain a list of filenames to be added.
    """
    for f in attachments:
        # guess content type based on extension
        ctype, encoding = mimetypes.guess_type(f)
        if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed), so
            # use a generic bag-of-bits type.
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
        attach.add_header('Content-Disposition', 'attachment',
                          filename=os.path.basename(f))
        msg.attach(attach)


# send text only email
def send_text_email(mailfrom, mailtos, subject, body, mail_password=None,
                    mail_host=None, mail_port=None, mail_user=None, tls=False, mail_host_type=None,
                    is_shared=False):
    """ send text email.
        mailtos is an array of one or more email addresses
    """
    send_email(mailfrom, mailtos, subject, body,
               mail_host=mail_host,
               mail_port=mail_port,
               mail_user=mail_user,
               mail_password=mail_password,
               tls=tls,
               mail_host_type=mail_host_type,
               isHtml=False,
               is_shared=is_shared,
               )


# send a html email
def send_html_email(mailfrom, mailtos, subject, body, text_content=None,
                    custom_header=None,
                    mail_host=None, mail_port=None, mail_user=None,
                    mail_password=None,
                    attachments=None, tls=False, mail_host_type=None, is_bulk=False,
                    is_shared=False):
    """ send html email.
        mailtos is an array of one or more email addresses
    """
    send_email(mailfrom, mailtos, subject, body, text_content, custom_header, mail_host, mail_port, mail_user,
               mail_password, attachments, tls, mail_host_type, is_bulk=is_bulk, is_shared=is_shared)


def send_email(mailfrom, mailtos, subject, body, text_content=None,
               custom_header=None,
               mail_host=None, mail_port=None, mail_user=None,
               mail_password=None,
               attachments=None, tls=False, mail_host_type=None, isHtml = True, is_bulk=False,
               is_shared=False, bcc=None, is_dryrun=False):
    if isinstance(mailfrom, tuple) and len(mailfrom) == 2:
        from_name = mailfrom[0] or ''
        from_addr = mailfrom[1]
    elif isinstance(mailfrom, str):
        from_name = ''
        from_addr = mailfrom
    if not from_addr:
        logger.error("missing sender address")
        return
    if six.PY2 and isinstance(from_addr, unicode):  ## py3-no-migrate
        from_addr = from_addr.encode('utf-8')
    if six.PY2 and isinstance(from_name, unicode):  ## py3-no-migrate
        from_name = str(Header(from_name, 'utf-8'))
    mailfrom = formataddr((from_name, from_addr))
    if isinstance(custom_header, str) or isinstance(custom_header, six.text_type):
        custom_header = [custom_header]

    if not subject:
        logger.error("missing subject")
        raise ValueError("missing subject")
    if isinstance(subject, six.text_type):
        subject = Header(subject, 'utf-8')

    if not body:
        logger.error("missing body")
        raise ValueError("missing body")
    if six.PY2 and isinstance(body, unicode):  ## py3-no-migrate
        body = body.encode('utf-8')

    if six.PY2 and isinstance(text_content, unicode):  ## py3-no-migrate
        text_content = text_content.encode('utf-8')

    if isinstance(mailtos, list):
        if six.PY2:  ## py3-no-migrate-begin
            mailtos = [mail.encode('utf-8') if isinstance(mail, unicode) else mail for mail in mailtos]
        ## py3-no-migrate-end
        logger.warning("Got a list of mailtos {}".format(mailtos))
        mailtos_in_msg = ','.join(mailtos)
    else:
        if six.PY2 and isinstance(mailtos, unicode):  ## py3-no-migrate
            mailtos = mailtos.encode('utf-8')
        mailtos = mailtos.replace(' ', '')
        mailtos_in_msg = mailtos
        mailtos = mailtos.split(',')
        logger.warning("Got a list of mailtos {}".format(mailtos))
    if not mailtos_in_msg:
        logger.error("missing to address")
        raise ValueError("missing to address")
    if six.PY2 and isinstance(mailtos_in_msg, unicode):  ## py3-no-migrate
        mailtos_in_msg = mailtos_in_msg.encode('utf-8')

    if bcc:
        if isinstance(bcc, list):
            if six.PY2:  ## py3-no-migrate-begin
                bcc = [mail.encode('utf-8') if isinstance(mail, unicode) else mail for mail in bcc]
            ## py3-no-migrate-end
            mailtos += bcc
        else:
            if six.PY2 and isinstance(bcc, unicode):  ## py3-no-migrate-begin
                bcc = bcc.encode('utf-8')
            ## py3-no-migrate-end
            bcc = bcc.replace(' ', '')
            mailtos.append(bcc)

    mail_host_type = mail_host_type or MAIl_HOST_TYPE_TRIGGERED
    logger.debug('sending mail with subject {} from {} to {}'.format(subject, mailfrom, mailtos))

    # compose the mail..
    if isHtml:
        msg = MIMEMultipart('mixed')
        msg['Date'] = formatdate()
        msg['Subject'] = subject
        msg['From'] = mailfrom
        msg['To'] = mailtos_in_msg
        html_msg = MIMEText(body, 'html', 'utf-8')
        if text_content:
            text_msg = MIMEText(text_content, 'plain', 'utf-8')
            body_msg = MIMEMultipart('alternative')
            body_msg.attach(text_msg)
            body_msg.attach(html_msg)
            msg.attach(body_msg)
        else:
            msg.attach(html_msg)
        if attachments:
            add_attachments(msg, attachments)
        # TODO: It's kind of hacky, since we add this header to all the emails, and we haven't
        # distinguish marketing emails vs transactional emails yet.
        # Jason can you help implement this in a right way? Basically we want all batch use the 'marketing' ip_pool,
        # while the transactional ones go to 'transactional' pool.
        #X-MSYS-API: { 'options' : { 'ip_pool' : 'your_ip_pool' } }

        if is_bulk:
            msg.add_header('X-MSYS-API', '{ "options" : { "ip_pool": "marketing"} }')
        elif not is_shared:
            msg.add_header('X-MSYS-API', '{ "options" : { "ip_pool": "transactional"} }')
    else:
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Date'] = formatdate()
        msg['Subject'] = subject
        msg['From'] = mailfrom
        msg['To'] = mailtos_in_msg

    if custom_header:
        if not isinstance(custom_header, list):
            logger.error('custom_header is not in right format')
        else:
            for header in custom_header:
                if six.PY2 and isinstance(header, unicode):  ## py3-no-migrate
                    header = header.encode('utf-8')
                if header:
                    (header_key, header_value) = header.split(':', 1)
                    msg.add_header(header_key, header_value)


    # and post it..
    return _post_smtp_mail(mailfrom, mailtos, msg, mail_host,
                           mail_port, mail_user, mail_password, tls=tls, mail_host_type=mail_host_type, is_bulk=is_bulk, is_dryrun=is_dryrun)


def get_email_domain(email_address):
    pos = email_address.find('@')
    if pos == -1:
        return ""
    else:
        return email_address[pos+1:].lower()


def _should_use_thirdparty_mailserver(mailtos):
    if Config.MANDRILL_CONFIG.ENABLED_PERCENTAGE > 0:
        address = mailtos
        if isinstance(mailtos, list):
            address = mailtos[0]
        domain = get_email_domain(address)
        if domain in Config.MANDRILL_CONFIG.DOMAINS_PERCENTAGE_APPLIED:
            percentage = Config.MANDRILL_CONFIG.DOMAINS_PERCENTAGE_APPLIED[domain]
            mod_value = int(hashlib.md5(address).hexdigest(), 16) % 100
            return mod_value < percentage

    return False


def _post_smtp_mail(mailfrom, mailtos, msg, mail_host=None,
                    mail_port=None, mail_user=None, mail_password=None,
                    tls=False, mail_host_type=MAIl_HOST_TYPE_TRIGGERED, is_bulk=False, is_dryrun = False):
    """ post a composed mail in msg to an smtp server in mail_host
        Note:internal function, do not call this directly, use
        send_html_email/send_text_email instead
    """

    #override mail_host_type
    if _should_use_thirdparty_mailserver(mailtos):
        mail_host_type = MAIL_HOST_TYPE_BAD_DOMAIN


    if mail_host:
        hosts = mail_host.split(';')
    else:
        hosts = []

    # apply default if it is not set
    if not mail_host or mail_host_type == MAIL_HOST_TYPE_BAD_DOMAIN or mail_host_type == MAIL_HOST_ONE_OFF_OPT_IN:
        mail_host_config = MailConfig.get_mail_host_by_type(mail_host_type)
        if mail_host_config:
            hosts[:0] = [mail_host_config.key]
        else:
            logger.error('unable to load any mail host config for mail host type: [%s]', ALL_EPN_MAIL_TYPES.get(mail_host_type, 'none'))

    # to deal with the case of multiple servers

    metrics = get_metric_client()
    for mail_host in hosts:
        host_and_optional_port = mail_host.split(':')
        if len(host_and_optional_port) == 2:
            mail_host = host_and_optional_port[0]
            mail_port = int(host_and_optional_port[1])
        mail_host_config = MailConfig.get_mail_host_by_name_and_port(mail_host, mail_port)
        if not mail_host_config:
            logger.error('failed to find mail host config with mail_host %s, mail_port %s', mail_host, mail_port)
            continue
        mail_timeout = mail_host_config.timeout
        mail_user = mail_host_config.username
        mail_password = mail_host_config.password
        number_of_retries = mail_host_config.retries
        host = mail_host_config.host
        port = mail_host_config.port
        tls = mail_host_config.tls
        for attempt_cnt in range(0, number_of_retries):
            try:
                if not is_dryrun:
                    smtp = smtplib.SMTP(host, port, None, mail_timeout)
                    if tls:
                        smtp.starttls()
                    smtp.login(mail_user, mail_password)
                    result = smtp.sendmail(mailfrom, mailtos, msg.as_string())
                else:
                    logger.info('[dryrun] pretended to post the mail from \'%s\' to \'%s\' to '
                                'the smtp host [%s:%s] with mail_host_type %d',
                                mailfrom, mailtos, host, port, mail_host_type)
                    result = None

                if not result:
                    if metrics:
                        tags = {"is_bulk": "true" if is_bulk else "false", "from_address": mailfrom}
                        tags["mail_host"] = "%s_%s_%s" % (mail_user, host, port)
                        metrics.log_counter(key='smtp_success', val=1, service_name='epn', tags=tags)
                else:
                    for mail_to in result:
                        (error_code, error_msg) = result[mail_to]
                        logger.error("failed to send msg to [%s] from [%s] using mail host [%s@%s:%s] due to error code [%d], error msg [%s]",
                                    mail_to, mailfrom, mail_user, host, port, error_code, error_msg)
                        if metrics:
                            tags = dict()
                            tags["error_code"] = str(error_code)
                            tags["mail_host"] = "%s_%s_%s" % (mail_user, host, port)
                            metrics.log_counter(key='smtp_failure', val=1, service_name='epn', tags=tags)
                if not is_dryrun:
                    smtp.close()
            except Exception as e:
                logger.exception('failed connecting to smtp host [%s@%s:%s] with mail_host_type %d or '
                            'lost connection to it, retrying other hosts... Excetion info: [%s]',
                            mail_user, host, port, mail_host_type, e)
                if metrics:
                    tags = dict()
                    tags["error_code"] = str(403)
                    tags["mail_host"] = "%s_%s_%s" % (mail_user, host, port)
                    metrics.log_counter(key='smtp_failure', val=1, service_name='epn', tags=tags)
            else:
                logger.info('posted the mail from \'%s\' to \'%s\' to '
                             'the smtp host [%s@%s:%s] with mail_host_type %d',
                             mailfrom, mailtos, mail_user, host, port, mail_host_type)
                if metrics:
                    tags = dict()
                    tags["mail_host"] = "%s_%s_%s" % (mail_user, host, port)
                    metrics.log_counter(key='smtp_success', val=1, service_name='epn', tags=tags)
                return True
    logger.error('failed to post the mail to the smtp host')
    tags = dict()
    tags["error_code"] = str(403)
    if metrics:
        metrics.log_counter(key='smtp_final_failure', val=1, service_name='epn', tags=tags)
    return False


def get_metric_client():
    try:
        from houzz.common.metric.client import init_metric_client
        #metric client socket runs in different path if the job runs on kubernetes
        is_kubernete = os.path.exists("/var/run/secrets/kubernetes.io")
        if is_kubernete:
            metric_uds = Config.LPV_COMMON.get('METRIC_UDS')
        else:
            metric_uds = Config.LOGGER_CONFIG.get('METRIC_UDS')

        metrics = init_metric_client(disable_metrics=False,
                                     collector_uds=metric_uds)
        return metrics
    except:
        return None


def connect(server, username, password):
    """ connect to imap mail server to retrieve messages.
        make sure to call disconnect().
    """
    mail = imaplib.IMAP4_SSL(server)
    mail.login(username, password)
    return mail


def disconnect(mail):
    """ disconnect from imap mail server.
    """
    mail.logout()


def select(mail, mailbox='INBOX'):
    """ select mailbox after connect().
        make sure to call close().
    """
    result, data = mail.select(mailbox)
    if result == "OK":
        return data
    else:
        raise EmailException('select failed for mailbox: %s' % mailbox)


def close(mail):
    """ close the selected mailbox.
    """
    mail.close()


def search(mail, search_filter='ALL'):
    """ search for messages with the given filter.
        returns the list of message uids.
    """
    result, data = mail.uid('search', None, search_filter)
    if result == "OK":
        return data
    else:
        raise EmailException('search failed with search_filter: %s'
                             % search_filter)


def fetch(mail, uid, flags='(RFC822)'):
    """ fetch message with the given uid.  Returns the message as "email"
    object as defined in email library.  """
    result, data = mail.uid('fetch', uid, flags)
    if result == "OK":
        raw_email = data[0][1]
        return email.message_from_string(raw_email)
    else:
        raise EmailException('fetch failed for uid: %s' % uid)


def store(mail, uid, cmd='+FLAGS', args='(\\Seen)'):
    """ update the message.
    """
    result, data = mail.uid('store', uid, cmd, args)
    if result == "OK":
        return data
    else:
        raise EmailException('store failed for uid: %s cmd: %s args: %s'
                             % (uid, cmd, args))


def get_all_headers(email_message, header):
    """ return all the header values that match the given header string.
    """
    maintype = email_message.get_content_maintype()
    result = []
    if maintype == 'text' or maintype == 'message':
        header_result = email_message.get_all(header)
        if header_result:
            result = result + header_result
    elif maintype == 'multipart':
        for part in email_message.walk():
            part_headers = part.get_all(header)
            if part_headers:
                result = result + part_headers
    return result


def get_text_blocks(email_message):
    """ return all the text blocks of the email_message (an email object) as a
    list.  if the message type is text, return the payload directly as a list
    of one entry.  if the message type is multipart, return all text blocks as
    a list of entries.  """
    maintype = email_message.get_content_maintype()
    result = []
    if maintype == 'text' or maintype == 'message':
        payload = email_message.get_payload(decode=True)
        if payload:
            result.append(
                payload.replace("=\r\n", "").
                replace("=\n", "").
                replace("=3D", "="))
    elif maintype == 'multipart':
        for part in email_message.walk():
            if part.get_content_maintype() == 'text' or \
                    part.get_content_maintype() == 'message':
                payload = part.get_payload(decode=True)
                if payload:
                    result.append(
                        payload.replace("=\r\n", "").
                        replace("=\n", "").
                        replace("=3D", "="))
    return result


def get_email_attachments(email_message, content_types,
                          out_dir=None, max_num=-1):
    """ iterate through the message and return the attachments that match the
    given content types.
    """
    filenames = []
    if not type(content_types) == list:
        content_types = [content_types]
    maintype = email_message.get_content_maintype()
    logger.debug('maintype: %s content_types: %s', maintype, content_types)
    if email_message.get_content_type() in content_types:
        data = email_message.get_payload(decode=True)
        filename = email_message.get_filename()
        if filename:
            filename = '%s/%s' % (out_dir, filename)
        else:
            filename = tempfile.mkstemp(dir=out_dir)
        logger.debug("saving file: %s", filename)
        with open(filename, "wb") as outfile:
            outfile.write(data)
        filenames.append(filename)
    elif maintype == 'multipart':
        for part in email_message.walk():
            logger.debug('content_type: %s', part.get_content_type())
            if part.get_content_type() in content_types:
                data = part.get_payload(decode=True)
                filename = part.get_filename()
                if filename:
                    filename = '%s/%s' % (out_dir, filename)
                else:
                    filename = tempfile.mkstemp(dir=out_dir)
                logger.debug("saving file: %s", filename)
                with open(filename, "wb") as outfile:
                    outfile.write(data)
                filenames.append(filename)
                if 0 < max_num <= len(filenames):
                    break
    return filenames


def purge_old_emails(mail, mailbox_labels, filter_str,
                     purge_date, max_purge, no_dry_run):
    """ purge old emails older than given date
    """
    def purge_each_email(mail, uid, msg, no_dry_run, **kwargs):
        if no_dry_run:
            store(mail, uid, '+X-GM-LABELS', '\\Trash')
            store(mail, uid, '+FLAGS', '(\\Deleted)')
        else:
            logger.info("dryrun mode, not marking mail %s as deleted.", uid)
        return True

    def purge_success(msg, no_dry_run, **kwargs):
        pass

    if not purge_date:
        return None

    summaries = {}
    # filter string
    if not filter_str:
        filter_str = ' SEEN '
    filter_str = '(' + filter_str + ' SENTBEFORE %s)' % \
        datetime.strptime(purge_date, "%Y-%m-%d").strftime("%d-%b-%Y")
    logger.info("Filter: %s", filter_str)

    for mailbox in mailbox_labels:
        summary = process_emails(mail, [mailbox], filter_str,
                                 purge_each_email, func_success_per_email=None,
                                 max_process=max_purge, no_dry_run=no_dry_run)
        summaries[mailbox] = summary

    return summaries


def process_emails(mail, mailbox_labels, filter_str, func_process_per_email,
                   func_success_per_email=None, max_process=-1,
                   no_dry_run=False, **kwargs):
    """ reads IMAP account and invokes the func_process_per_email function for
    each email caller is responsible for connecting and disconnecting the mail
    connection by using connect and disconnect.  if no_dry_run is False and
    func_process_per_email returns True, it'll run func_success_per_email
    function if defined, otherwise it'll mark the email as read by default.
        returns the summary string.
    """
    logger.info('process_emails start')
    summary = []
    if not func_process_per_email:
        logger.error('No function specified.')
        return summary

    max_process = int(max_process)
    total_processed = 0
    total_no_op = 0
    total_failed = 0
    for mailbox in mailbox_labels:
        num_messages = select(mail, mailbox)
        logger.info("Mailbox: [%s] Number of messages: [%s]",
                    mailbox, num_messages)
        if num_messages == 0:
            logger.info("No messages to process for mailbox: [%s].", mailbox)
            continue

        logger.info("Filter: %s", filter_str)
        data = search(mail, filter_str)
        logger.debug("Data: %s", data)
        uids = data[0].split()
        total = len(uids)

        for i in range(total):
            if 0 <= max_process <= (total_processed + total_failed):
                logger.info("max_process set to %s, stopping.", max_process)
                break
            logger.debug("Processing msg %d uid: %s", i, uids[i])
            # fetch each message but do not mark as read until successfully
            # processed
            msg = fetch(mail, uids[i], '(BODY.PEEK[])')
            logger.debug("uid: %s date: %s from: %s message-id: %s",
                         uids[i], msg['Date'], msg['From'], msg['Message-Id'])
            try:
                success = func_process_per_email(mail, uids[i], msg,
                                                 no_dry_run, **kwargs)
            except:
                logger.exception('Failed processing msg uid: %s', uids[i])
                total_failed += 1
                continue
            logger.debug("uid: %s success: %s", uids[i], success)
            if success:
                if not no_dry_run:
                    logger.debug("Dryrun mode, not updating.")
                else:
                    if func_success_per_email:
                        func_success_per_email(msg, no_dry_run, **kwargs)
                    else:
                        store(mail, uids[i], '+FLAGS.SILENT', '(\\Seen)')
                total_processed += 1
            elif success is None:
                total_no_op += 1
            else:
                total_failed += 1
        if no_dry_run:
            logger.debug("expunge mailbox: %s", mailbox)
            mail.expunge()
        close(mail)
        if max_process >= 0 and (total_processed+total_failed+total_no_op) >= max_process:
            break
    logger.info("Total processed: %s", total_processed)
    logger.info("Total no op: %s", total_no_op)
    logger.info("Total failed: %s", total_failed)
    summary.append("Total emails processed: %s, total no-op: %s, total total failed: %s"
                   % (total_processed, total_no_op, total_failed))
    return summary


def process_emails_with_retry(mail, mailbox, filter_str, func_process_per_email, mailserver, username, password,
                              func_success_per_email=None, max_process=-1,
                              no_dry_run=False, retries=10, **kwargs):
    """ reads IMAP account and invokes the func_process_per_email function for
    each email caller is responsible for connecting and disconnecting the mail
    connection by using connect and disconnect.  if no_dry_run is False and
    func_process_per_email returns True, it'll run func_success_per_email
    function if defined, otherwise it'll mark the email as read by default.
        returns the summary string.
    """
    logger.info('process_emails start')
    summary = []
    if not func_process_per_email:
        logger.error('No function specified.')
        return summary

    max_process = int(max_process)
    total_processed = 0
    total_no_op = 0
    total_failed = 0


    for retry in range(retries):
        try:
            num_messages = select(mail, mailbox)
            logger.info("Mailbox: [%s] Number of messages: [%s]",
                        mailbox, num_messages)
            if num_messages == 0:
                logger.info("No messages to process for mailbox: [%s].", mailbox)
                return summary

            logger.info("Filter: %s", filter_str)
            data = search(mail, filter_str)
            logger.debug("Data: %s", data)
            uids = data[0].split()
            total = len(uids)

            for i in range(total):
                if 0 <= max_process <= (total_processed + total_failed):
                    logger.info("max_process set to %s, stopping.", max_process)
                    break
                logger.debug("Processing msg %d uid: %s", i, uids[i])
                # fetch each message but do not mark as read until successfully
                # processed
                msg = fetch(mail, uids[i], '(BODY.PEEK[])')
                logger.debug("uid: %s date: %s from: %s message-id: %s",
                             uids[i], msg['Date'], msg['From'], msg['Message-Id'])
                try:
                    success = func_process_per_email(mail, uids[i], msg,
                                                     no_dry_run, **kwargs)
                except:
                    logger.exception('Failed processing msg uid: %s', uids[i])
                    total_failed += 1
                    continue
                logger.debug("uid: %s success: %s", uids[i], success)
                if success:
                    if not no_dry_run:
                        logger.debug("Dryrun mode, not updating.")
                    else:
                        if func_success_per_email:
                            func_success_per_email(msg, no_dry_run, **kwargs)
                        else:
                            store(mail, uids[i], '+FLAGS.SILENT', '(\\Seen)')
                    total_processed += 1
                elif success is None:
                    total_no_op += 1
                else:
                    total_failed += 1

                if max_process >= 0 and (total_processed+total_failed+total_no_op) >= max_process:
                    break

            if no_dry_run:
                logger.debug("expunge mailbox: %s", mailbox)
                mail.expunge()
            logger.info("successfully processed mailbox [%s]" % mailbox)
            break
        except Exception as e:
            logger.exception("process mailbox [%s] failed in the [%s]th retry with [%s]" % (mailbox, retry + 1, e.message))
            try:
                close(mail)
            except Exception as e:
                logger.exception(e.message)
            time.sleep(5)
            mail = connect(mailserver, username, password)
            continue

    logger.info("Total processed: %s", total_processed)
    logger.info("Total no op: %s", total_no_op)
    logger.info("Total failed: %s", total_failed)
    summary.append("Total emails processed: %s, total no-op: %s, total total failed: %s"
                   % (total_processed, total_no_op, total_failed))
    return summary
