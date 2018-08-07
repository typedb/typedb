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

import queue
from typing import Type

from grakn.service.Session.util.RequestBuilder import RequestBuilder
import grakn.service.Session.util.ResponseReader as ResponseReader # for circular import issue
from grakn.service.Session.util import enums
from grakn.exception.GraknError import GraknError

class TransactionService(object):

    def __init__(self, keyspace, tx_type: enums.TxType, credentials, transaction_endpoint):
        self.keyspace = keyspace
        self.tx_type = tx_type.value
        self.credentials = credentials

        self._communicator = Communicator(transaction_endpoint)

        # open the transaction with an 'open' message
        open_req = RequestBuilder.open_tx(keyspace, tx_type, credentials)
        self._communicator.send(open_req)


    # --- Passthrough targets ---
    # targets of top level Transaction class

    def query(self, query: str, infer=True):
        request = RequestBuilder.query(query, infer=infer)
        # print("Query request: {0}".format(request))
        response = self._communicator.send(request)
        # convert `response` into a python iterator
        return ResponseReader.ResponseReader.query(self, response.query_iter) 

    def commit(self):
        request = RequestBuilder.commit()
        self._communicator.send(request)

    def close(self):
        self._communicator.close()

    def is_closed(self):
        return self._communicator._closed

    def get_concept(self, concept_id: str): 
        request = RequestBuilder.get_concept(concept_id)
        response = self._communicator.send(request)
        return ResponseReader.ResponseReader.get_concept(self, response.getConcept_res)

    def get_schema_concept(self, label: str): 
        request = RequestBuilder.get_schema_concept(label)
        response = self._communicator.send(request)
        return ResponseReader.ResponseReader.get_schema_concept(self, response.getSchemaConcept_res)

    def get_attributes_by_value(self, attribute_value, data_type: enums.DataType):
        request = RequestBuilder.get_attributes_by_value(attribute_value, data_type)
        response = self._communicator.send(request)
        return ResponseReader.ResponseReader.get_attributes_by_value(self, response.getAttributes_iter)

    def put_entity_type(self, label: str):
        request = RequestBuilder.put_entity_type(label)
        response = self._communicator.send(request)
        return ResponseReader.ResponseReader.put_entity_type(self, response.putEntityType_res)

    def put_relationship_type(self, label: str):
        request = RequestBuilder.put_relationship_type(label)
        response = self._communicator.send(request)
        return ResponseReader.ResponseReader.put_relationship_type(self, response.putRelationType_res)

    def put_attribute_type(self, label: str, data_type: enums.DataType):
        request = RequestBuilder.put_attribute_type(label, data_type)
        response = self._communicator.send(request)
        return ResponseReader.ResponseReader.put_attribute_type(self, response.putAttributeType_res)

    def put_role(self, label: str):
        request = RequestBuilder.put_role(label)
        response = self._communicator.send(request)
        return ResponseReader.ResponseReader.put_role(self, response.putRole_res)

    def put_rule(self, label: str, when: str, then: str):
        request = RequestBuilder.put_rule(label, when, then)
        response = self._communicator.send(request)
        return ResponseReader.ResponseReader.put_rule(self, response.putRule_res)

    # --- Transaction Messages ---

    def run_concept_method(self, concept_id, grpc_concept_method_req):
        # wrap method_req into a transaction message
        tx_request = RequestBuilder.concept_method_req_to_tx_req(concept_id, grpc_concept_method_req)
        response = self._communicator.send(tx_request)
        return response.conceptMethod_res.response


    def iterate(self, iterator_id: int):
        request = RequestBuilder.next_iter(iterator_id)
        response = self._communicator.send(request)
        return response.iterate_res 


class Communicator(object):
    """ An iterator and interface for GRPC stream """

    def __init__(self, grpc_stream_constructor):
        self._queue = queue.Queue()
        self._response_iterator = grpc_stream_constructor(self)
        self._closed = False

    def _add_request(self, request):
        self._queue.put(request)

    def __next__(self):
        # print("`next` called on Communicator")
        # print("Current queue: {0}".format(list(self._queue.queue)))
        next_item = self._queue.get(block=True)
        if next_item is None:
            raise StopIteration()
        return next_item

    def __iter__(self):
        return self

    def send(self, request):
        if self._closed:
            # TODO integrate this into TransactionService to throw a "Transaction is closed" rather than "connection is closed..."
            raise GraknError("This connection is closed")
        try:
            self._add_request(request)
            response = next(self._response_iterator)
        except Exception as e: # specialize into different gRPC exceptions?
            # on any GRPC exception, close the stream
            self.close()
            raise GraknError("Server/network error: {0}\n\n generated from request: {1}".format(e, request))

        if response is None:
            raise GraknError("No response received")
        
        return response

    def close(self):
        with self._queue.mutex: # probably don't even need the mutex
            self._queue.queue.clear()
        self._queue.put(None)
        self._closed = True
