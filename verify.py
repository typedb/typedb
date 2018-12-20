#!/usr/bin/env python

from __future__ import print_function
from pprint import pprint
import subprocess
import sys


java_targets = set(subprocess.check_output([
    'bazel', 'query',
    '(kind(java_library, //...) union kind(java_test, //...)) except //dependencies/...'
]).split())

checkstyle_targets = set(map(lambda x: x.replace('-checkstyle', ''), subprocess.check_output([
    'bazel', 'query', 'kind(checkstyle_test, //...)'
]).split()))

java_target_with_no_checkstyle = java_targets - checkstyle_targets

if java_target_with_no_checkstyle:
    print('Java targets with no attached checkstyle_test rule')
    pprint(java_target_with_no_checkstyle)
    sys.exit(1)
