from __future__ import absolute_import
__author__ = 'jesse'
import sys
import subprocess
from datetime import datetime


class SmartFile(object):
    def __init__(self, filename=None, mode=None):
        if filename and filename != '-':
            strftime = datetime.now().\
                strftime('%Y_%m_%d_%H_%M_%S')
            date = datetime.now().\
                strftime('%Y_%m_%d')
            filename = filename.format(
                now=strftime, date=date)
            self.fh = open(filename, mode)
        else:
            self.fh = subprocess.PIPE

    def __enter__(self):
        return self.fh

    def __exit__(self, ty, value, tb):
        if self.fh is not subprocess.PIPE:
            self.fh.close()
