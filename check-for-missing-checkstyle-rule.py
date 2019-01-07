#!/usr/bin/env python

#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

from __future__ import print_function
from itertools import chain
import os
from pprint import pprint
from xml.etree import ElementTree
import subprocess
import sys


def check_output_discarding_stderr(*args, **kwargs):
    with open(os.devnull, 'w') as devnull:
        return subprocess.check_output(*args, stderr=devnull, **kwargs)


print('Checking if there are any source files not covered by checkstyle...')

java_targets = set(check_output_discarding_stderr([
    'bazel', 'query',
    '(kind(java_library, //...) union kind(java_test, //...)) '
    'except //dependencies/... except attr("tags", "checkstyle_ignore", //...)'
]).split())

checkstyle_targets_xml = check_output_discarding_stderr([
    'bazel', 'query', 'kind(checkstyle_test, //...)', '--output', 'xml'
])
checkstyle_targets_tree = ElementTree.fromstring(checkstyle_targets_xml)
checkstyle_targets = set(map(lambda x: x.get('value'),
                             chain(
                                 checkstyle_targets_tree.findall(".//label[@name='target'][@value]"),
                                 checkstyle_targets_tree.findall(".//list[@name='targets']//label[@value]"),
                             )))

java_targets_with_no_checkstyle = java_targets - checkstyle_targets
target_count = len(java_targets_with_no_checkstyle)

if java_targets_with_no_checkstyle:
    print('ERROR: Found %d bazel targets which are not covered by a `checkstyle_test`:' % target_count)
    for i, target_label in enumerate(java_targets_with_no_checkstyle, start=1):
        print('%d: %s' % (i, target_label))
    sys.exit(1)
else:
    print('SUCCESS: Every source code is covered by a `checkstyle_test`!')
