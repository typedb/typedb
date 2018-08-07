#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from grakn.service.Session.Concept import BaseTypeMapping
from grakn.service.Session.Concept.Concept import EntityType, RelationshipType, AttributeType, Role, Rule, Entity, Relationship, Attribute, Type


# map names to ConceptHierarchy types
name_to_object = {
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


def create_concept(tx_service, grpc_concept):
    """ Instantate a local Python object for a Concept corresponding to the .baseType of the GRPC concept object """

    concept_id = grpc_concept.id
    base_type = grpc_concept.baseType

    try:
        concept_name = BaseTypeMapping.grpc_base_type_to_name[base_type]
        concept_class = name_to_object[concept_name]
    except KeyError as ke:
        raise ke

    return concept_class(concept_id, concept_name, tx_service)
