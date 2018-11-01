#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from unittest import TestCase
from datetime import datetime
from forbiddenfruit import curse

import six

class DummyContextManager(object):
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self, *args, **kwargs):
        pass

    def __exit__(self, *args, **kwargs):
        pass


class test_Base(TestCase):
    """ Sets up DB for use in tests """

    @classmethod
    def setUpClass(cls):
        super(test_Base, cls).setUpClass()

        def _datetime_to_timestamp(self):
            epoch = datetime(1970, 1, 1)
            diff = self - epoch
            return int(diff.total_seconds())

        if six.PY2:
            print 'Patching datetime.timestamp for PY2'
            print 'Patching unittest.TestCase.subTest for PY2'
            curse(datetime, 'timestamp', _datetime_to_timestamp)
            curse(TestCase, 'subTest', DummyContextManager)
