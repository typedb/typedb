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
        self._tx_service.commit()
        self.close()

    def close(self):
        self._tx_service.close() # close the service

    def get_concept(self, concept_id: str): 
        return self._tx_service.get_concept(concept_id)

    def get_schema_concept(self, label: str): 
        return self._tx_service.get_schema_concept(label)

    def get_attributes_by_value(self, attribute_value, data_type: type(enums.DataType)):
        return self._tx_service.get_attributes_by_value(attribute_value, data_type)

    def put_entity_type(self, label: str):
        return self._tx_service.put_entity_type(label)

    def put_relationship_type(self, label: str):
        return self._tx_service.put_relationship_type(label)

    def put_attribute_type(self, label: str, data_type: type(enums.DataType)):
        return self._tx_service.put_attribute_type(label, data_type)

    def put_role(self, label: str):
        return self._tx_service.put_role(label)

    def put_rule(self, label: str, when: str, then: str):
        return self._tx_service.put_rule(label, when, then)


    # --- Transaction Messages ---


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
