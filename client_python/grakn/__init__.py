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

import grpc
from grakn.service.Session.util.enums import TxType, DataType
from grakn.service.Keyspace.KeyspaceService import KeyspaceService
from grakn.service.Session.TransactionService import TransactionService
from protocol.session.Session_pb2_grpc import SessionServiceStub
from grakn.exception.GraknError import GraknError

class Grakn(object):
    """ A client/representation of a Grakn instance"""

    def __init__(self, uri, credentials=None):
        self.uri = uri
        self._keyspace_service = KeyspaceService(self.uri, credentials)
        self.credentials = credentials

    def session(self, keyspace):
        """ Open a session for a specific  keyspace. Can be used as `with Grakn('localhost:48555').session(keyspace='test') as session: ... ` or as normal assignment"""
        return Session(self.uri, keyspace, self.credentials)
    session.__annotations__ = {'keyspace': str}

    def keyspaces(self):
        return self._keyspace_service


class Session(object):
    """ A session for a Grakn instance and a specific keyspace """

    def __init__(self, uri, keyspace, credentials):

        self.keyspace = keyspace
        self.uri = uri
        self.credentials = credentials

        self._channel = grpc.insecure_channel(uri)
        self._stub = SessionServiceStub(self._channel)
        self._closed = False
    __init__.__annotations__ = {'uri': str, 'keyspace': str}

    def transaction(self, tx_type):
        """ Open a transaction to Grakn on this keyspace

        Can be used as `with session.transaction(grakn.TxType.READ) as tx: ...`
        Don't forget to commit within the `with`!
        Alternatively you can still do `tx = session.transaction(...); ...; tx.close()`

        :param grakn.TxType tx_type: The type of transaction to open as indicated by the tx_type enum
        """
        if self._closed:
            raise GraknError("Session is closed")

        # create a transaction service which hides GRPC usage
        transaction_service = TransactionService(self.keyspace, tx_type, self.credentials, self._stub.transaction)
        return Transaction(transaction_service)

    def close(self):
        """ Close this keyspace session """
        self._closed = True
        self._channel.close()
    
    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()
        if tb is None:
            # No exception
            pass
        else:
            #print("Closing Session due to exception: {0} \n traceback: \n {1}".format(type, tb))
            return False


class Transaction(object):
    """ Presents the Grakn interface to the user, actual work with GRPC happens in TransactionService """

    def __init__(self, transaction_service):
        self._tx_service = transaction_service
    __init__.__annotations__ = {'transaction_service': TransactionService}

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()
        if tb is None:
            # No exception
            pass
        else:
            #print("Closing Transaction due to exception: {0} \n traceback: \n {1}".format(type, tb))
            return False

    def query(self, query, infer=True):
        """ Execute a Graql query, inference is optionally enabled """
        return self._tx_service.query(query, infer)
    query.__annotations__ = {'query': str}

    def commit(self):
        """ Commit and close this transaction, persisting changes to Grakn """
        self._tx_service.commit()
        self.close()

    def close(self):
        """ Close this transaction without committing """
        self._tx_service.close() # close the service

    def is_closed(self):
        """ Check if this transaction is closed """
        return self._tx_service.is_closed()

    def get_concept(self, concept_id):
        """ Retrieve a concept by Concept ID (string) """
        return self._tx_service.get_concept(concept_id)
    get_concept.__annotations__ = {'concept_id': str}

    def get_schema_concept(self, label):
        """ Retrieve a schema concept by its label (eg. those defined using `define` or tx.put...() """
        return self._tx_service.get_schema_concept(label)
    get_schema_concept.__annotations__ = {'label': str}

    def get_attributes_by_value(self, attribute_value, data_type):
        """ Retrieve atttributes with a specific value and datatype

        :param any attribute_value: the value to match
        :param grakn.DataType data_type: The data type of the value in Grakn, as given by the grakn.DataType enum
        """
        return self._tx_service.get_attributes_by_value(attribute_value, data_type)

    def put_entity_type(self, label):
        """ Define a new entity type with the given label """
        return self._tx_service.put_entity_type(label)
    put_entity_type.__annotations__ = {'label': str}

    def put_relationship_type(self, label):
        """ Define a new relationship type with the given label """
        return self._tx_service.put_relationship_type(label)
    put_relationship_type.__annotations__ = {'label': str}

    def put_attribute_type(self, label, data_type):
        """ Define a new attribute type with the given label and data type 

        :param str label: the label of the attribute type
        :param grakn.DataType data_type: the data type of the value to be stored, as given by the grakn.DataType enum
        """
        return self._tx_service.put_attribute_type(label, data_type)
    put_attribute_type.__annotations__ = {'label': str}

    def put_role(self, label):
        """ Define a role with the given label """
        return self._tx_service.put_role(label)
    put_role.__annotations__ = {'label': str}

    def put_rule(self, label, when, then):
        """ Define a new rule with the given label, when and then clauses """
        return self._tx_service.put_rule(label, when, then)
    put_rule.__annotations__ = {'label': str, 'when': str, 'then': str}
