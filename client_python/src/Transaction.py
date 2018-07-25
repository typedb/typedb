from src.service.Session.TransactionService import TransactionService

class Transaction(object):
    """ Presents the Grakn interface to the user, actual work with GRPC happens in TransactionService """

    def __init__(self, transaction_service: TransactionService):
        self._tx_service = transaction_service


    def query(self, query: str):
        return self._tx_service.query(query)

    def commit(self):
        self._tx_service.commit()
        self.close()

    def close(self):
        self._tx_service.close() # close the service

    def get_concept(self, concept_id: str): # TODO annotate return types
        return self._tx_service.get_concept(concept_id)

    def get_schema_concept(self, label: str): 
        return self._tx_service.get_schema_concept(label)

    def get_attributes_by_value(self, attribute_value, data_type):
        return self._tx_service.get_attributes_by_value(attribute_value, data_type)

    def put_entity_type(self, label: str):
        return self._tx_service.put_entity_type(label)

    def put_relationship_type(self, label: str):
        return self._tx_service.put_relationship_type(label)

    def put_attribute_type(self, label: str, data_type):
        return self._tx_service.put_attribute_type(label, data_type)

    def put_role(self, label: str):
        return self._tx_service.put_role(label)

    def put_rule(self, label: str, when: str, then: str):
        return self._tx_service.put_rule(label, when, then)
