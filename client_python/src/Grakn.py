from .Session import Session
from src.service.Session.util.enums import TxType, DataType


def session(self, uri: str, keyspace: str):
    return Session(uri, keyspace)
