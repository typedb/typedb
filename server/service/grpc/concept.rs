/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::{Thing, Type};
use chrono::{Datelike, NaiveDateTime, Timelike};
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, entity::Entity, relation::Relation, thing_manager::ThingManager, ThingAPI},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, relation_type::RelationType, role_type::RoleType,
        type_manager::TypeManager, TypeAPI,
    },
};
use encoding::value::{timezone::TimeZone, value::Value, value_type::ValueType};
use error::unimplemented_feature;
use storage::snapshot::ReadableSnapshot;

pub(crate) fn encode_thing_concept(
    thing: &Thing,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
) -> Result<typedb_protocol::Concept, Box<ConceptReadError>> {
    let encoded = match thing {
        Thing::Entity(entity) => typedb_protocol::concept::Concept::Entity(encode_entity(
            entity,
            snapshot,
            type_manager,
            include_instance_types,
        )?),
        Thing::Relation(relation) => typedb_protocol::concept::Concept::Relation(encode_relation(
            relation,
            snapshot,
            type_manager,
            include_instance_types,
        )?),
        Thing::Attribute(attribute) => typedb_protocol::concept::Concept::Attribute(encode_attribute(
            attribute,
            snapshot,
            type_manager,
            thing_manager,
            include_instance_types,
        )?),
    };
    Ok(typedb_protocol::Concept { concept: Some(encoded) })
}

pub(crate) fn encode_entity(
    entity: &Entity,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    include_instance_types: bool,
) -> Result<typedb_protocol::Entity, Box<ConceptReadError>> {
    Ok(typedb_protocol::Entity {
        iid: Vec::from(entity.iid()),
        entity_type: if include_instance_types {
            Some(encode_entity_type(&entity.type_(), snapshot, type_manager)?)
        } else {
            None
        },
    })
}

pub(crate) fn encode_relation(
    relation: &Relation,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    include_instance_types: bool,
) -> Result<typedb_protocol::Relation, Box<ConceptReadError>> {
    Ok(typedb_protocol::Relation {
        iid: Vec::from(relation.iid()),
        relation_type: if include_instance_types {
            Some(encode_relation_type(&relation.type_(), snapshot, type_manager)?)
        } else {
            None
        },
    })
}

pub(crate) fn encode_attribute(
    attribute: &Attribute,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
) -> Result<typedb_protocol::Attribute, Box<ConceptReadError>> {
    Ok(typedb_protocol::Attribute {
        iid: Vec::from(attribute.iid()),
        value: Some(encode_value(attribute.get_value(snapshot, thing_manager)?)),
        attribute_type: if include_instance_types {
            Some(encode_attribute_type(&attribute.type_(), snapshot, type_manager)?)
        } else {
            None
        },
    })
}

pub(crate) fn encode_type_concept(
    type_: &Type,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::Concept, Box<ConceptReadError>> {
    let encoded = match type_ {
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
    };
    Ok(typedb_protocol::Concept { concept: Some(encoded) })
}

pub(crate) fn encode_entity_type(
    entity: &EntityType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::EntityType, Box<ConceptReadError>> {
    Ok(typedb_protocol::EntityType {
        label: entity.get_label(snapshot, type_manager)?.scoped_name().as_str().to_string(),
    })
}

pub(crate) fn encode_relation_type(
    relation: &RelationType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::RelationType, Box<ConceptReadError>> {
    Ok(typedb_protocol::RelationType {
        label: relation.get_label(snapshot, type_manager)?.scoped_name().as_str().to_string(),
    })
}

pub(crate) fn encode_attribute_type(
    attribute: &AttributeType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::AttributeType, Box<ConceptReadError>> {
    Ok(typedb_protocol::AttributeType {
        label: attribute.get_label(snapshot, type_manager)?.scoped_name().as_str().to_string(),
        value_type: {
            attribute
                .get_value_type_without_source(snapshot, type_manager)?
                .map(|value_type| encode_value_type(value_type, snapshot, type_manager))
                .transpose()?
        },
    })
}

pub(crate) fn encode_role_type(
    role: &RoleType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::RoleType, Box<ConceptReadError>> {
    Ok(typedb_protocol::RoleType { label: role.get_label(snapshot, type_manager)?.scoped_name().as_str().to_owned() })
}

pub(crate) fn encode_value_type(
    value_type: ValueType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::ValueType, Box<ConceptReadError>> {
    let value_type_message = match value_type {
        ValueType::Boolean => typedb_protocol::value_type::ValueType::Boolean(typedb_protocol::value_type::Boolean {}),
        ValueType::Integer => typedb_protocol::value_type::ValueType::Integer(typedb_protocol::value_type::Integer {}),
        ValueType::Double => typedb_protocol::value_type::ValueType::Double(typedb_protocol::value_type::Double {}),
        ValueType::Decimal => typedb_protocol::value_type::ValueType::Decimal(typedb_protocol::value_type::Decimal {}),
        ValueType::Date => typedb_protocol::value_type::ValueType::Date(typedb_protocol::value_type::Date {}),
        ValueType::DateTime => {
            typedb_protocol::value_type::ValueType::Datetime(typedb_protocol::value_type::DateTime {})
        }
        ValueType::DateTimeTZ => {
            typedb_protocol::value_type::ValueType::DatetimeTz(typedb_protocol::value_type::DateTimeTz {})
        }
        ValueType::Duration => {
            typedb_protocol::value_type::ValueType::Duration(typedb_protocol::value_type::Duration {})
        }
        ValueType::String => typedb_protocol::value_type::ValueType::String(typedb_protocol::value_type::String {}),
        ValueType::Struct(struct_definition_key) => {
            let name = type_manager.get_struct_definition(snapshot, struct_definition_key)?.name.clone();
            typedb_protocol::value_type::ValueType::Struct(typedb_protocol::value_type::Struct { name })
        }
    };
    Ok(typedb_protocol::ValueType { value_type: Some(value_type_message) })
}

pub(crate) fn encode_value(value: Value<'_>) -> typedb_protocol::Value {
    let value_message = match value {
        Value::Boolean(bool) => typedb_protocol::value::Value::Boolean(bool),
        Value::Integer(integer) => typedb_protocol::value::Value::Integer(integer),
        Value::Double(double) => typedb_protocol::value::Value::Double(double),
        Value::Decimal(decimal) => typedb_protocol::value::Value::Decimal(typedb_protocol::value::Decimal {
            integer: decimal.integer_part(),
            fractional: decimal.fractional_part(),
        }),
        Value::Date(date) => typedb_protocol::value::Value::Date(typedb_protocol::value::Date {
            num_days_since_ce: Datelike::num_days_from_ce(&date),
        }),
        Value::DateTime(date_time) => typedb_protocol::value::Value::Datetime(encode_date_time(date_time)),
        Value::DateTimeTZ(date_time_tz) => {
            typedb_protocol::value::Value::DatetimeTz(typedb_protocol::value::DatetimeTz {
                datetime: Some(encode_date_time(date_time_tz.naive_utc())),
                timezone: Some(encode_time_zone(date_time_tz.timezone())),
            })
        }
        Value::Duration(duration) => typedb_protocol::value::Value::Duration(typedb_protocol::value::Duration {
            months: duration.months,
            days: duration.days,
            nanos: duration.nanos,
        }),
        Value::String(string) => typedb_protocol::value::Value::String(string.to_string()),
        Value::Struct(struct_) => {
            unimplemented_feature!(Structs)
        }
    };
    typedb_protocol::Value { value: Some(value_message) }
}

fn encode_date_time(date_time: NaiveDateTime) -> typedb_protocol::value::Datetime {
    typedb_protocol::value::Datetime { seconds: date_time.and_utc().timestamp(), nanos: date_time.nanosecond() }
}

fn encode_time_zone(timezone: TimeZone) -> typedb_protocol::value::datetime_tz::Timezone {
    match timezone {
        TimeZone::IANA(tz) => typedb_protocol::value::datetime_tz::Timezone::Named(tz.name().to_string()),
        TimeZone::Fixed(fixed) => typedb_protocol::value::datetime_tz::Timezone::Offset(fixed.local_minus_utc()),
    }
}
