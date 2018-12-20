#!/usr/bin/env python

import sys
import subprocess

COMMAND = '{command}'
ALLOW_FAILURE = bool(int('{allow_failure}'))

exit_code = subprocess.call(COMMAND.split(' '))
sys.exit(0 if ALLOW_FAILURE else exit_code)
