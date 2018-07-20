import queue
from typing import Type

from .util.RequestBuilder import RequestBuilder
from .util.ResponseConverter import ResponseConverter
from .util import enums

class TransactionService(object):

    def __init__(self, keyspace, tx_type, transaction_endpoint):
        self.keyspace = keyspace
        self.tx_type = tx_type

        self._communicator = Communicator(transaction_endpoint)
        self._response_converter = ResponseConverter(self)

        # open the transaction with an 'open' message
        open_req = RequestBuilder.open_tx(keyspace, tx_type)
        self._communicator.send(open_req)


    # --- Passthrough targets ---
    # targets of top level Transaction class

    def query(self, query: str):
        # TODO create QueryRequest GRPC object
        request = RequestBuilder.query(query)
        response = self._communicator.send(request)
        # convert `response` into a python iterator
        return self._response_converter.query(response) 

    def commit(self):
        request = RequestBuilder.commit()
        self._communicator.send(request)

    def close(self):
        self._communicator.close()

    def get_concept(self, concept_id: str): 
        request = RequestBuilder.get_concept(concept_id)
        response = self._communicator.send(request)
        return self._response_converter.get_concept(response)

    def get_schema_concept(self, label: str): 
        pass

    def get_attributes_by_value(self, attribute_value, data_type: type(enums.DataType)):
        pass

    def put_entity_type(self, label: str):
        pass

    def put_relationship_type(self, label: str):
        pass

    def put_attribute_type(self, label: str, data_type: type(enums.DataType)):
        pass

    def put_role(self, label: str):
        pass

    def put_rule(self, label: str, when: str, then: str):
        pass

    # --- Transaction Messages ---

    def run_concept_method(self, concept_id, grpc_concept_method_req):
        # wrap method_req into a transaction message
        tx_request = RequestBuilder.concept_method_req_to_tx_req(concept_id, grpc_concept_method_req)
        response = self._communicator.send(tx_request)
        return response


    def iterate(self, iterator_id: int):
        request = RequestBuilder.next_request(iterator_id)
        response = self._communicator.send(request)
        return response 


class Communicator(object):
    """ An iterator and interface for GRPC stream """

    def __init__(self, grpc_stream_constructor):
        self._queue = queue.Queue()
        self._response_iterator = grpc_stream_constructor(self)

    def _add_request(self, request):
        self._queue.put(request)

    def __next__(self):
        print("`next` called on Communicator")
        print("Current queue: {0}".format(list(self._queue.queue)))
        next_item = self._queue.get(block=True)
        if next_item is None:
            raise StopIteration()
        return next_item

    def __iter__(self):
        return self

    def send(self, request):
        self._add_request(request)
        return next(self._response_iterator)

    def close(self):
        self._queue.clear()
        self._queue.append(None)
