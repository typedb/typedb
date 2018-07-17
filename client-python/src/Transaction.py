from typing import Type

from Grakn import Grakn
from Session.TransactionService import TransactionService

class Transaction(object):
    

    def __init__(self, transaction_service: Type(TransactionService)):
        """ Want to decouple this from GRPC -- pass in a TransactionService
        which implements requirements and uses whatever protocol"""
        
        self.tx_service = transaction_service


    def query(self, query: str):
        return self.tx_service.query(query)

    def commit(self):
        self.tx_service.commit()
        self.close()

    def close(self):
        self.tx_service.close() # close the service

    def get_concept(self, concept_id: str): # TODO annotate return types
        return self.tx_service.get_concept(concept_id)

    def get_schema_concept(self, label: str): 
        return self.tx_service.get_schema_concept(label)

    def get_attributes_by_value(self, attribute_value, data_type: Type(Grakn.DataType)):
        return self.tx_service.get_attributes_by_value(attribute_value, data_type)

    def put_entity_type(self, label: str):
        return self.tx_service.put_entity_type(label)

    def put_relationship_type(self, label: str):
        return self.tx_service.put_relationship_type(label)

    def put_attribute_type(self, label: str, data_type: Type(Grakn.DataType)):
        return self.tx_service.put_attribute_type(label, data_type)

    def put_role(self, label: str):
        return self.tx_service.put_role(label)

    def put_rule(self, label: str, when: str, then: str):
        return self.tx_service.put_rule(label, when, then)


   

