#!/usr/bin/python

from __future__ import absolute_import
__author__ = 'kenneth'

import logging
import optparse

from houzz.common.logger import slogger

from houzz.common import email_utils

if __name__ == '__main__':
    parser = optparse.OptionParser('Usage: %prog [options]')
    parser.add_option('-s', '--server', dest='server', default='', help='hostname of mail server to use')
    parser.add_option('-e', '--email', dest='email', default='kenneth@houzz.com', help='email address to send to')
    (options, args) = parser.parse_args()

    slogger.setUpLogger("/home/clipu/c2svc/log", "send_test_email", level=logging.DEBUG)

    if len(args):
        parser.error('incorrect number of arguments')

    from_email = 'noreply@bounces.houzz.com'
    subject = 'Test Email for server {0}'.format(options.server)
    text_body = 'Text body for server {0}'.format(options.server)
    html_body = '<html><head></head><body><p><b>HTML</b> body for server {0}.</p></body></html>'.format(options.server)
    email_utils.send_html_email(from_email, options.email, subject, html_body, text_body, mail_host=options.server, mail_port=25,
                                mail_user="SMTP_Injection")
