from ..autogenerated import Concept_pb2 as ConceptMessages 
from .ConceptHierarchy import EntityType, RelationshipType, AttributeType, Role, Rule, Entity, Relationship, Attribute, Type

# base type constant names 
CONCEPTS = META_TYPE, ATTRIBUTE_TYPE, RELATIONSHIP_TYPE, ENTITY_TYPE, ENTITY, ATTRIBUTE, RELATIONSHIP, ROLE, RULE = \
    "META_TYPE", "ATTRIBUTE_TYPE", "RELATIONSHIP_TYPE", "ENTITY_TYPE", "ENTITY", "ATTRIBUTE", "RELATIONSHIP", "ROLE", "RULE"

"""
NOTE: the string META_TYPE is the name of the programmatic type of
Thing, Entity, Attribute, Relation IN GRPC. In the original Java server implementation,
these have the type TYPE, but due to bad naming at some point, GRPC
says the base_type of them is META_TYPE. Thus, there will never be
any concepts with base_type TYPE, only META_TYPE on GRPC-connected clients.
To match the server class hierarchy, I here instantiate TYPE objects rather than
META_TYPE, and when the time comes we will rename META_TYPE to TYPE on GRPC connected
clients too.
"""

grpc_base_types = ConceptMessages.Concept.BASE_TYPE
grpc_base_type_to_name = {
    grpc_base_types.Value("META_TYPE"): META_TYPE,
    grpc_base_types.Value("ENTITY_TYPE"): ENTITY_TYPE,
    grpc_base_types.Value("RELATION_TYPE"): RELATIONSHIP_TYPE,
    grpc_base_types.Value("ATTRIBUTE_TYPE"): ATTRIBUTE_TYPE,
    grpc_base_types.Value("ROLE"): ROLE,
    grpc_base_types.Value("RULE"): RULE,
    grpc_base_types.Value("ENTITY"): ENTITY,
    grpc_base_types.Value("RELATION"): RELATIONSHIP,
    grpc_base_types.Value("ATTRIBUTE"): ATTRIBUTE 
}

# reverse lookup of above
# note: assuming one-to-one correspondence
name_to_grpc_base_type = dict(zip(grpc_base_type_to_name.values(), grpc_base_type_to_name.keys()))



