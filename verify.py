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
from pprint import pprint
from xml.etree import ElementTree
import subprocess
import sys


java_targets = set(subprocess.check_output([
    'bazel', 'query',
    '(kind(java_library, //...) union kind(java_test, //...)) except //dependencies/...'
]).split())

checkstyle_targets_xml = subprocess.check_output([
    'bazel', 'query', 'kind(checkstyle_test, //...)', '--output', 'xml'
])
checkstyle_targets_tree = ElementTree.fromstring(checkstyle_targets_xml)
checkstyle_targets = set(map(lambda x: x.get('value'),
                             checkstyle_targets_tree.findall(".//label[@name='target'][@value]")))

java_targets_with_no_checkstyle = java_targets - checkstyle_targets

if java_targets_with_no_checkstyle:
    print('Java targets with no attached checkstyle_test rule')
    pprint(java_targets_with_no_checkstyle)
    sys.exit(1)
