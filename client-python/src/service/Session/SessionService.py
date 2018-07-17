import grpc
from typing import Type

from ...Grakn import Grakn
from TransactionService import TranactionService

class SessionService(object):
    """ A lower layer Session that is used to abstract away GRPC details"""

    def __init__(self, uri: str, keyspace: str):
        self.keyspace = keyspace
        self.channel = grpc.insecure_channel(uri) 
        self.stub = SessionServiceStub(channel)


    def transaction(self,  tx_type: Type(Grakn.TxType)) -> Type(TransactionService):
        """ Abstract away use of GRPC into a tx_service """
        tx_service = TransactionService(self.stub.transaction())
        tx_service.openTx(self.keyspace, tx_type)
        return tx_service

    def close(self):
        self.channel.close()
        
    
