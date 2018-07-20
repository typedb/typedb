from . import BaseTypeMapping
from .ConceptHierarchy import EntityType, RelationshipType, AttributeType, Role, Rule, Entity, Relationship, Attribute, Type

class ConceptFactory(object):
    def __init__(self, tx_service):
        self.tx_service = tx_service
        self.name_to_object = {
            BaseTypeMapping.META_TYPE: Type,
            BaseTypeMapping.ENTITY_TYPE: EntityType,
            BaseTypeMapping.RELATIONSHIP_TYPE: RelationshipType,
            BaseTypeMapping.ATTRIBUTE_TYPE: AttributeType,
            BaseTypeMapping.ROLE: Role,
            BaseTypeMapping.RULE: Rule,
            BaseTypeMapping.ENTITY: Entity,
            BaseTypeMapping.RELATIONSHIP: Relationship,
            BaseTypeMapping.ATTRIBUTE: Attribute
        }

    def create_concept(self, grpc_concept):

        concept_id = grpc_concept.id
        base_type = grpc_concept.baseType
    
        try:
            concept_name = BaseTypeMapping.grpc_base_type_to_name[base_type]
            concept_class = self.name_to_object[concept_name]
        except KeyError as ke:
            raise ke

        return concept_class(concept_id, concept_name, self.tx_service)
