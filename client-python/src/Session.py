from typing import Type

from Grakn import Grakn
from Transaction import Transaction
from Session.SessionService import SessionService 


class Session(object):

    def __init__(self, uri: str, keyspace: str):

        self.keyspace = keyspace
        self.uri = uri
        self._session_service = SessionService(uri, keyspace)

    def transaction(self, tx_type: Type(Grakn.TxType)): 
        # create a transaction service which hides GRPC usage
        transaction_service = self._session_service.transaction(tx_type)
        return Transaction(transaction_service)

    def close(self):
        self._session_service.close()

