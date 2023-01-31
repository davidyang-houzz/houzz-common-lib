# Miscellaneous utility functions for writing command-line tools.

from __future__ import absolute_import
from __future__ import print_function
import pwd
import os

import readchar
from six.moves import input

# Helpful colors.
class COLORS(object):
    RED     = '\033[31m'
    GREEN   = '\033[32m'
    BLUE    = '\033[34m'
    MAGENTA = '\033[35m'

    PROD    = '\033[38;5;202m'  # orange
    STAGING = '\033[38;5;120m'  # bright green

    RESET = '\033[0m'

# Read a single key from terminal.
# For example, we can use it to ask "Press any key to continue:".
def get_char(prompt):
    if prompt != '' and prompt[-1] not in ' \n': prompt += ' '
    print(prompt, end=' ')  # No linebreak.
    char = readchar.readchar()
    print(char)

    return char

# E.g., yes_or_no('Continue? (y/N)', default=False)
def yes_or_no(prompt, default=None):
    if prompt != '' and prompt[-1] not in ' \n': prompt += ' '
    while True:
        s = input(prompt)
        if s.upper() in ['Y', 'YES']: return True
        if s.upper() in ['N', 'NO']: return False
        if s == '' and default is not None: return default
        print("Invalid input - please type 'y' or 'n'.")

# Print messages in color.
#
# If it starts with a blank line, we remove it, so that we can write:
#       print_color(COLORS.RED, '''
# This is the first line of the message.
# This is the second line.''')
def print_color(color, s):
    # Remove a blank line (in case we used triple quotes).
    if s[0] == '\n': s = s[1:]
    print('%s%s%s' % (color, s, COLORS.RESET))

# A helper function to figure out who I am.
# (If running under sudo, it will say 'root'.)
def find_current_user():
    return pwd.getpwuid(os.getuid()).pw_name
