/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::{Thing, Type};
use answer::variable_value::VariableValue;
use concept::error::ConceptReadError;
use concept::thing::thing_manager::ThingManager;
use concept::type_::{KindAPI, TypeAPI};
use concept::type_::annotation::Annotation;
use concept::type_::attribute_type::AttributeType;
use concept::type_::entity_type::EntityType;
use concept::type_::relation_type::RelationType;
use concept::type_::role_type::RoleType;
use concept::type_::type_manager::TypeManager;
use encoding::value::value_type::ValueType;
use storage::snapshot::ReadableSnapshot;

fn encode_variable_value(
    variable_value: &VariableValue<'_>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_thing_types: bool,
) -> typedb_protocol::Answer {
    // let answer = let
    match variable_value {
        VariableValue::Empty => typedb_protocol::answer::Answer::Empty(typedb_protocol::answer::Empty {}),
        VariableValue::Type(type_) => {
            typedb_protocol::answer::Answer::Concept(
                typedb_protocol::Concept { concept: Some(encode_type_concept(type_, snapshot, type_manager)?) }
            )
        }
        VariableValue::Thing(thing) => {
            typedb_protocol::answer::Answer::Concept(
                typedb_protocol::Concept {
                    concept: Some(encode_thing_concept(thing, snapshot, type_manager, thing_manager, include_thing_types)?)
                }
            )
        }
        VariableValue::Value(value) => {}
        VariableValue::ThingList(thing_list) => {}
        VariableValue::ValueList(value_list) => {}
    };
    todo!()
}

fn encode_thing_concept(
    thing: &Thing,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_thing_types: bool,
) -> Result<typedb_protocol::concept::Concept, ConceptReadError> {
    Ok(match thing {
        Thing::Entity(entity) => {
            typedb_protocol::concept::Concept::Entity(
                typedb_protocol::Entity {
                    iid: Vec::from(entity.iid().bytes()),
                    entity_type: if include_thing_types {
                        Some(encode_entity_type(&entity.type_(), snapshot, type_manager)?)
                    } else {
                        None
                    },
                }
            )
        }
        Thing::Relation(relation) => {
            typedb_protocol::concept::Concept::Relation(
                typedb_protocol::Relation {
                    iid: Vec::from(relation.iid().bytes()),
                    relation_type: if include_thing_types {
                        Some(encode_relation_type(&relation.type_(), snapshot, type_manager)?)
                    } else {
                        None
                    },
                }
            )
        }
        Thing::Attribute(attribute) => {
            typedb_protocol::concept::Concept::Attribute(
                typedb_protocol::Attribute {
                    iid: Vec::from(attribute.iid().bytes()),
                    value: encode_value(attribute.get_value(snapshot))
                    attribute_type: if include_thing_types {
                        Some(encode_attribute_type(&attribute.type_(), snapshot, type_manager)?)
                    } else {
                        None
                    },
                }
            )
        }
    })
}

fn encode_type_concept(
    type_: &Type,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::concept::Concept, ConceptReadError> {
    Ok(match type_ {
        Type::Entity(entity) => {
            typedb_protocol::concept::Concept::EntityType(encode_entity_type(entity, snapshot, type_manager)?)
        }
        Type::Relation(relation) => {
            typedb_protocol::concept::Concept::RelationType(encode_relation_type(relation, snapshot, type_manager)?)
        }
        Type::Attribute(attribute) => {
            typedb_protocol::concept::Concept::AttributeType(encode_attribute_type(attribute, snapshot, type_manager)?)
        }
        Type::RoleType(role) => {
            typedb_protocol::concept::Concept::RoleType(encode_role_type(role, snapshot, type_manager)?)
        }
    })
}

fn encode_entity_type(
    entity: &EntityType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::EntityType, ConceptReadError> {
    typedb_protocol::EntityType {
        label: entity.get_label(snapshot, type_manager)?.scoped_name().to_string(),
        annotations: encode_annotations(
            &*entity.get_annotations_declared(snapshot, type_manager)?
                .iter()
                .map(|annotation| Annotation::from(annotation.clone()))
        ),
    }
}

fn encode_relation_type(
    relation: &RelationType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::RelationType, ConceptReadError> {
    typedb_protocol::RelationType {
        label: relation.get_label(snapshot, type_manager)?.scoped_name().to_string(),
        annotations: encode_annotations(
            &*relation.get_annotations_declared(snapshot, type_manager)?
                .iter()
                .map(|annotation| Annotation::from(annotation.clone()))
        ),
    }
}

fn encode_attribute_type(
    attribute: &AttributeType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::AttributeType, ConceptReadError> {
    typedb_protocol::AttributeType {
        label: attribute.get_label(snapshot, type_manager)?.scoped_name().to_string(),
        value_type: {
            attribute
                .get_value_type(snapshot, type_manager)?
                .map(|value_type| encode_value_type(value_type, snapshot, type_manager))
                .transpose()?
        },
        annotations: encode_annotations(
            &*attribute.get_annotations_declared(snapshot, type_manager)?
                .iter()
                .map(|annotation| Annotation::from(annotation.clone()))
        ),
    }
}

fn encode_role_type(
    role: &RoleType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::RoleType, ConceptReadError> {
    typedb_protocol::RoleType {
        name: role.get_label(snapshot, type_manager)?.name().to_string(),
        scope: role.get_label(snapshot, type_manager)?.scope().to_string(),
        annotations: encode_annotations(
            &*role.get_annotations_declared(snapshot, type_manager)?
                .iter()
                .map(|annotation| Annotation::from(annotation.clone()))
        ),
    }
}

fn encode_annotations(annotations: impl Iterator<Item=Annotation>) -> Vec<typedb_protocol::Annotation> {
    todo!()
}

fn encode_value_type(
    value_type: ValueType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::ValueType, ConceptReadError> {
    let value_type_message = match value_type {
        ValueType::Boolean => typedb_protocol::value_type::ValueType::Boolean(typedb_protocol::value_type::Boolean {}),
        ValueType::Long => typedb_protocol::value_type::ValueType::Long(typedb_protocol::value_type::Long {}),
        ValueType::Double => typedb_protocol::value_type::ValueType::Double(typedb_protocol::value_type::Double {}),
        ValueType::Decimal => typedb_protocol::value_type::ValueType::Decimal(typedb_protocol::value_type::Decimal {}),
        ValueType::Date => typedb_protocol::value_type::ValueType::Date(typedb_protocol::value_type::Date {}),
        ValueType::DateTime => typedb_protocol::value_type::ValueType::Datetime(typedb_protocol::value_type::DateTime {}),
        ValueType::DateTimeTZ => typedb_protocol::value_type::ValueType::DatetimeTz(typedb_protocol::value_type::DateTimeTz {}),
        ValueType::Duration => typedb_protocol::value_type::ValueType::Duration(typedb_protocol::value_type::Duration {}),
        ValueType::String => typedb_protocol::value_type::ValueType::String(typedb_protocol::value_type::String {}),
        ValueType::Struct(struct_definition_key) => {
            let name = type_manager.get_struct_definition(snapshot, struct_definition_key)?.name.clone();
            typedb_protocol::value_type::ValueType::Struct(typedb_protocol::value_type::Struct { name })
        }
    };
    Ok(typedb_protocol::ValueType {
        value_type: Some(value_type_message),
    })
}

fn