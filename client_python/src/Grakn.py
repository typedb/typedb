from .Session import Session

import src.service.Session.util.enums as enums
import Session_pb2 as SessionMessages
import Concept_pb2 as ConceptMessages

class Grakn(object):

    # workaround for Protobuf's non-relative imports
#    TxType = enums.TxType
#    DataType = enums.DataType

    def __init__(self):
        pass

    # stateless method
    def session(self, uri: str, keyspace: str):
        return Session(uri, keyspace)

    # TODO keyspace delete/retrieve

class TxType(object):
    READ = SessionMessages.Transaction.Type.Value('READ')
    WRITE = SessionMessages.Transaction.Type.Value('WRITE')
    BATCH = SessionMessages.Transaction.Type.Value('BATCH')
        
    
class DataType(object):
    STRING = ConceptMessages.AttributeType.DATA_TYPE.Value('STRING')
    BOOLEAN = ConceptMessages.AttributeType.DATA_TYPE.Value('BOOLEAN')
    INTEGER = ConceptMessages.AttributeType.DATA_TYPE.Value('INTEGER')
    LONG = ConceptMessages.AttributeType.DATA_TYPE.Value('LONG')
    FLOAT = ConceptMessages.AttributeType.DATA_TYPE.Value('FLOAT')
    DOUBLE = ConceptMessages.AttributeType.DATA_TYPE.Value('DOUBLE')
    DATE= ConceptMessages.AttributeType.DATA_TYPE.Value('DATE')
