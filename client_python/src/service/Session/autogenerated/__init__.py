
# so this throws strange errors in GRPC about not having the same type
# when packaging messages within each other
# from . import Session_pb2
# from . import Concept_pb2

# this works fine...
import Session_pb2 
import Concept_pb2



