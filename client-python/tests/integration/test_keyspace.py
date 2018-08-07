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

import unittest
import grakn

client = grakn.Grakn("localhost:48555")

class test_Keyspace(unittest.TestCase):

   def test_retrieve_delete(self):
       """ Test retrieving and deleting a specific keyspace """

       session = client.session(keyspace="keyspacetest")
       tx = session.transaction(grakn.TxType.WRITE)
       tx.close()

       keyspaces = client.keyspace.retrieve()
       self.assertGreater(len(keyspaces), 0)
       self.assertTrue('keyspacetest' in keyspaces)

       client.keyspace.delete('keyspacetest')
       post_delete_keyspaces = client.keyspace.retrieve()
       self.assertFalse('keyspacetest' in post_delete_keyspaces)

       session.close()
       #client.keyspace.delete("keyspacetest")

