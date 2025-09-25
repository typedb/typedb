/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;

use answer::{Thing, Type};
use bytes::{util::HexBytesFormatter, Bytes};
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, entity::Entity, relation::Relation, thing_manager::ThingManager, ThingAPI},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, relation_type::RelationType, role_type::RoleType,
        type_manager::TypeManager, TypeAPI,
    },
};
use encoding::value::{value::Value, value_type::ValueType, ValueEncodable};
use error::unimplemented_feature;
use resource::profile::StorageCounters;
use serde::{ser::SerializeStruct, Deserialize, Serialize};
use serde_json::json;
use storage::snapshot::ReadableSnapshot;

// TODO: Should probably be merged with JSON from behaviour/steps/query_answer_context.rs.
// Now, it's easier to have symmetry between two services, and we don't have the capacity to merge
// these (BDDs will check if this code is correct)

macro_rules! count_fields {
    () => { 0 };
    ($head:ident $(, $tail:ident)*) => { 1 + count_fields!($($tail),*) };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", rename = "entity")]
pub struct EntityResponse {
    pub iid: String,
    pub r#type: Option<EntityTypeResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", rename = "relation")]
pub struct RelationResponse {
    pub iid: String,
    pub r#type: Option<RelationTypeResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", rename = "attribute")]
pub struct AttributeResponse {
    pub value: serde_json::Value,
    pub value_type: String,
    pub r#type: Option<AttributeTypeResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", rename = "value")]
pub struct ValueResponse {
    pub value: serde_json::Value,
    pub value_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", rename = "entityType")]
pub struct EntityTypeResponse {
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", rename = "relationType")]
pub struct RelationTypeResponse {
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", rename = "attributeType")]
pub struct AttributeTypeResponse {
    pub label: String,
    pub value_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", rename = "roleType")]
pub struct RoleTypeResponse {
    pub label: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TypeDocument {
    kind: String,
    label: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AttributeTypeDocument {
    kind: String,
    label: String,
    value_type: String,
}

impl Into<serde_json::Value> for AttributeResponse {
    fn into(self) -> serde_json::Value {
        self.value
    }
}

impl Into<serde_json::Value> for ValueResponse {
    fn into(self) -> serde_json::Value {
        self.value
    }
}

impl Into<TypeDocument> for EntityTypeResponse {
    fn into(self) -> TypeDocument {
        TypeDocument { kind: "entity".to_string(), label: self.label }
    }
}

impl Into<TypeDocument> for RelationTypeResponse {
    fn into(self) -> TypeDocument {
        TypeDocument { kind: "relation".to_string(), label: self.label }
    }
}

impl Into<AttributeTypeDocument> for AttributeTypeResponse {
    fn into(self) -> AttributeTypeDocument {
        AttributeTypeDocument {
            kind: "attribute".to_string(),
            label: self.label,
            value_type: self.value_type.unwrap_or_else(|| "none".to_string()),
        }
    }
}

impl Into<TypeDocument> for RoleTypeResponse {
    fn into(self) -> TypeDocument {
        TypeDocument { kind: "relation:role".to_string(), label: self.label }
    }
}

pub fn encode_thing_concept(
    thing: &Thing,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
    storage_counters: StorageCounters,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    let response = match thing {
        Thing::Entity(entity) => {
            serde_json::to_value(encode_entity(entity, snapshot, type_manager, include_instance_types)?)
                .expect("Expected json value conversion")
        }
        Thing::Relation(relation) => {
            serde_json::to_value(encode_relation(relation, snapshot, type_manager, include_instance_types)?)
                .expect("Expected json value conversion")
        }
        Thing::Attribute(attribute) => serde_json::to_value(encode_attribute(
            attribute,
            snapshot,
            type_manager,
            thing_manager,
            include_instance_types,
            storage_counters,
        )?)
        .expect("Expected json value conversion"),
    };
    Ok(response)
}

pub fn encode_entity(
    entity: &Entity,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    include_instance_types: bool,
) -> Result<EntityResponse, Box<ConceptReadError>> {
    Ok(EntityResponse {
        iid: encode_iid(entity.iid()),
        r#type: if include_instance_types {
            Some(encode_entity_type(&entity.type_(), snapshot, type_manager)?)
        } else {
            None
        },
    })
}

pub fn encode_relation(
    relation: &Relation,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    include_instance_types: bool,
) -> Result<RelationResponse, Box<ConceptReadError>> {
    Ok(RelationResponse {
        iid: encode_iid(relation.iid()),
        r#type: if include_instance_types {
            Some(encode_relation_type(&relation.type_(), snapshot, type_manager)?)
        } else {
            None
        },
    })
}

pub fn encode_attribute(
    attribute: &Attribute,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
    storage_counters: StorageCounters,
) -> Result<AttributeResponse, Box<ConceptReadError>> {
    let value = attribute.get_value(snapshot, thing_manager, storage_counters)?;
    Ok(AttributeResponse {
        value_type: encode_value_value_type(&value),
        value: encode_value_value(value),
        r#type: if include_instance_types {
            Some(encode_attribute_type(&attribute.type_(), snapshot, type_manager)?)
        } else {
            None
        },
    })
}

fn encode_iid<const ARRAY_INLINE_SIZE: usize>(iid: Bytes<'_, ARRAY_INLINE_SIZE>) -> String {
    HexBytesFormatter::owned(Vec::from(iid)).format_iid()
}

pub fn encode_type_concept(
    type_: &Type,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    let encoded = match type_ {
        Type::Entity(entity) => {
            json!(encode_entity_type(entity, snapshot, type_manager)?)
        }
        Type::Relation(relation) => {
            json!(encode_relation_type(relation, snapshot, type_manager)?)
        }
        Type::Attribute(attribute) => {
            json!(encode_attribute_type(attribute, snapshot, type_manager)?)
        }
        Type::RoleType(role) => {
            json!(encode_role_type(role, snapshot, type_manager)?)
        }
    };
    Ok(encoded)
}

pub fn encode_entity_type(
    entity: &EntityType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<EntityTypeResponse, Box<ConceptReadError>> {
    Ok(EntityTypeResponse { label: entity.get_label(snapshot, type_manager)?.scoped_name().as_str().to_string() })
}

pub fn encode_relation_type(
    relation: &RelationType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<RelationTypeResponse, Box<ConceptReadError>> {
    Ok(RelationTypeResponse { label: relation.get_label(snapshot, type_manager)?.scoped_name().as_str().to_string() })
}

pub fn encode_attribute_type(
    attribute: &AttributeType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<AttributeTypeResponse, Box<ConceptReadError>> {
    Ok(AttributeTypeResponse {
        label: attribute.get_label(snapshot, type_manager)?.scoped_name().as_str().to_string(),
        value_type: {
            attribute
                .get_value_type_without_source(snapshot, type_manager)?
                .map(|value_type| encode_value_type(value_type, snapshot, type_manager))
                .transpose()?
        },
    })
}

pub fn encode_role_type(
    role: &RoleType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<RoleTypeResponse, Box<ConceptReadError>> {
    Ok(RoleTypeResponse { label: role.get_label(snapshot, type_manager)?.scoped_name().as_str().to_string() })
}

pub fn encode_value(value: Value<'_>) -> ValueResponse {
    let value_type = encode_value_value_type(&value);
    ValueResponse { value: encode_value_value(value), value_type }
}

pub fn encode_value_value_type(value: &Value<'_>) -> String {
    value.value_type().to_string()
}

pub fn encode_value_value(value: Value<'_>) -> serde_json::Value {
    match value {
        Value::Boolean(bool) => json!(bool),
        Value::Integer(integer) => json!(integer),
        Value::Double(double) => json!(double),
        Value::String(cow) => json!(match cow {
            Cow::Borrowed(s) => s.to_string(),
            Cow::Owned(s) => s.clone(),
        }),
        Value::Decimal(_) | Value::Date(_) | Value::DateTime(_) | Value::DateTimeTZ(_) | Value::Duration(_) => {
            json!(value.to_string())
        }
        Value::Struct(_) => unimplemented_feature!(Structs),
    }
}

pub fn encode_value_type(
    value_type: ValueType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<String, Box<ConceptReadError>> {
    let value_type = match value_type {
        value_type @ (ValueType::Boolean
        | ValueType::Integer
        | ValueType::Double
        | ValueType::Decimal
        | ValueType::Date
        | ValueType::DateTime
        | ValueType::DateTimeTZ
        | ValueType::Duration
        | ValueType::String) => value_type.category().name().to_string(),
        ValueType::Struct(struct_definition_key) => {
            type_manager.get_struct_definition(snapshot, struct_definition_key)?.name.clone()
        }
    };
    Ok(value_type)
}
