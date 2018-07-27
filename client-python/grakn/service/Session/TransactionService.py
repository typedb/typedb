import queue
from typing import Type

from grakn.service.Session.util.RequestBuilder import RequestBuilder
# from src.service.Session.util.ResponseConverter import ResponseConverter
import grakn.service.Session.util.ResponseConverter as ResponseConverter
from grakn.service.Session.util import enums

class TransactionService(object):

    def __init__(self, keyspace, tx_type: enums.TxType, transaction_endpoint):
        self.keyspace = keyspace
        self.tx_type = tx_type.value

        self._communicator = Communicator(transaction_endpoint)

        # open the transaction with an 'open' message
        open_req = RequestBuilder.open_tx(keyspace, tx_type)
        self._communicator.send(open_req)


    # --- Passthrough targets ---
    # targets of top level Transaction class

    def query(self, query: str):
        request = RequestBuilder.query(query)
        response = self._communicator.send(request)
        # convert `response` into a python iterator
        return ResponseConverter.ResponseConverter.query(self, response.query_iter) 

    def commit(self):
        request = RequestBuilder.commit()
        self._communicator.send(request)

    def close(self):
        self._communicator.close()

    def get_concept(self, concept_id: str): 
        request = RequestBuilder.get_concept(concept_id)
        response = self._communicator.send(request)
        return ResponseConverter.ResponseConverter.get_concept(self, response.getConcept_res)

    def get_schema_concept(self, label: str): 
        request = RequestBuilder.get_schema_concept(label)
        response = self._communicator.send(request)
        return ResponseConverter.ResponseConverter.get_schema_concept(self, response.getSchemaConcept_res)

    def get_attributes_by_value(self, attribute_value, data_type: enums.DataType):
        request = RequestBuilder.get_attributes_by_value(attribute_value, data_type)
        response = self._communicator.send(request)
        return ResponseConverter.ResponseConverter.get_attributes_by_value(self, response.getAttributes_iter)

    def put_entity_type(self, label: str):
        request = RequestBuilder.put_entity_type(label)
        response = self._communicator.send(request)
        return ResponseConverter.ResponseConverter.put_entity_type(self, response.putEntityType_res)

    def put_relationship_type(self, label: str):
        request = RequestBuilder.put_relationship_type(label)
        response = self._communicator.send(request)
        return ResponseConverter.ResponseConverter.put_relationship_type(self, response.putRelationshipType_res)

    def put_attribute_type(self, label: str, data_type: enums.DataType):
        request = RequestBuilder.put_attribute_type(label, data_type)
        response = self._communicator.send(request)
        return ResponseConverter.ResponseConverter.put_attribute_type(self, response.putAttributeType_res)

    def put_role(self, label: str):
        request = RequestBuilder.put_role(label)
        response = self._communicator.send(request)
        return ResponseConverter.ResponseConverter.put_role(self, response.putRole_res)

    def put_rule(self, label: str, when: str, then: str):
        request = RequestBuilder.put_rule(label, when, then)
        response = self._communicator.send(request)
        return ResponseConverter.ResponseConverter.put_rule(self, response.putRule_res)

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
        self.closed = False

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
        if self.closed:
            #TODO specialize exception
            # TODO integrate this into TransactionService to throw a "Transaction is closed" rather than "connection is closed..."
            raise Exception("Connection is closed")

        try:
            self._add_request(request)
            response = next(self._response_iterator)
        except Exception as e: # TODO specialize exception
            # on any GRPC exception, close the stream
            self.close()
            # TODO specialize exception
            raise Exception("Server/network error: {0}".format(e))

        if response is None:
            # TODO specialize exception
            raise Exception("No response from connection")
        
        return response

    def close(self):
        with self._queue.mutex: # probably don't even need the mutex
            self._queue.queue.clear()
        self._queue.put(None)
        self.closed = True
