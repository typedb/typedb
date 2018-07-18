from typing import Type

from .util import RequestBuilder, enums

class TransactionService(object):

    def __init__(self, keyspace, tx_type, communicator):
        self.keyspace = keyspace
        self.tx_type = tx_type
        self._communicator = communicator
        # open the transaction with an 'open' message
        
        open_req = RequestBuilder.open_tx(keyspace, tx_type)
        self._communicator.send(open_req)


    # --- Passthrough targets ---
    # targets of top level Transaction class

    def query(self, query: str):
        # TODO create QueryRequest GRPC object
        request = RequestBuilder.query(query)
        return self._communicator.send(request)

    def commit(self):
        self._tx_service.commit()
        self.close()

    def close(self):
        self._tx_service.close() # close the service

    def get_concept(self, concept_id: str): # TODO annotate return types
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


   

