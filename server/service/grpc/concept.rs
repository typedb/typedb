/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::str::FromStr;

use answer::{Thing, Type};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, TimeZone as ChronoTimeZone, Timelike};
use chrono_tz::Tz;
use concept::{
    error::{ConceptDecodeError, ConceptReadError},
    thing::{attribute::Attribute, entity::Entity, relation::Relation, thing_manager::ThingManager, ThingAPI},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, relation_type::RelationType, role_type::RoleType,
        type_manager::TypeManager, TypeAPI,
    },
};
use encoding::value::{
    decimal_value::Decimal, duration_value::Duration, timezone::TimeZone, value::Value, value_type::ValueType,
};
use error::unimplemented_feature;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

pub(crate) fn encode_thing_concept(
    thing: &Thing,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
    storage_counters: StorageCounters,
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
            storage_counters,
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
    storage_counters: StorageCounters,
) -> Result<typedb_protocol::Attribute, Box<ConceptReadError>> {
    Ok(typedb_protocol::Attribute {
        iid: Vec::from(attribute.iid()),
        value: Some(encode_value(attribute.get_value(snapshot, thing_manager, storage_counters)?)),
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
    use typedb_protocol::value::Value as ValueProto;
    let value_message = match value {
        Value::Boolean(boolean) => ValueProto::Boolean(boolean),
        Value::Integer(integer) => ValueProto::Integer(integer),
        Value::Double(double) => ValueProto::Double(double),
        Value::Decimal(decimal) => ValueProto::Decimal(encode_decimal(decimal)),
        Value::Date(date) => ValueProto::Date(encode_date(date)),
        Value::Datetime(date_time) => ValueProto::Datetime(encode_datetime(date_time)),
        Value::DatetimeTz(datetime_tz) => ValueProto::DatetimeTz(encode_datetime_tz(datetime_tz)),
        Value::Duration(duration) => ValueProto::Duration(encode_duration(duration)),
        Value::String(string) => ValueProto::String(string.to_string()),
        Value::Struct(_struct) => unimplemented_feature!(Structs),
    };
    typedb_protocol::Value { value: Some(value_message) }
}

pub(crate) fn encode_decimal(decimal: Decimal) -> typedb_protocol::value::Decimal {
    typedb_protocol::value::Decimal { integer: decimal.integer_part(), fractional: decimal.fractional_part() }
}

pub(crate) fn decode_decimal(proto: typedb_protocol::value::Decimal) -> Result<Decimal, Box<ConceptDecodeError>> {
    Ok(Decimal::new(proto.integer, proto.fractional))
}

pub(crate) fn encode_date(date: NaiveDate) -> typedb_protocol::value::Date {
    typedb_protocol::value::Date { num_days_since_ce: Datelike::num_days_from_ce(&date) }
}

pub(crate) fn decode_date(proto: typedb_protocol::value::Date) -> Result<NaiveDate, Box<ConceptDecodeError>> {
    NaiveDate::from_num_days_from_ce_opt(proto.num_days_since_ce)
        .ok_or_else(|| Box::new(ConceptDecodeError::InvalidDate { days: proto.num_days_since_ce }))
}

pub(crate) fn encode_datetime(datetime: NaiveDateTime) -> typedb_protocol::value::Datetime {
    typedb_protocol::value::Datetime { seconds: datetime.and_utc().timestamp(), nanos: datetime.nanosecond() }
}

pub(crate) fn decode_datetime(
    proto: typedb_protocol::value::Datetime,
) -> Result<NaiveDateTime, Box<ConceptDecodeError>> {
    let seconds = proto.seconds;
    let nanos = proto.nanos;
    DateTime::from_timestamp(seconds, nanos)
        .map(|value| value.naive_utc())
        .ok_or_else(|| Box::new(ConceptDecodeError::InvalidDatetime { seconds, nanos }))
}

pub(crate) fn decode_datetime_from_millis(millis: i64) -> Result<NaiveDateTime, Box<ConceptDecodeError>> {
    DateTime::from_timestamp_millis(millis)
        .map(|value| value.naive_utc())
        .ok_or_else(|| Box::new(ConceptDecodeError::InvalidDatetimeMillis { millis }))
}

pub(crate) fn encode_datetime_tz(datetime_tz: DateTime<TimeZone>) -> typedb_protocol::value::DatetimeTz {
    typedb_protocol::value::DatetimeTz {
        datetime: Some(encode_datetime(datetime_tz.naive_utc())),
        timezone: Some(encode_timezone(datetime_tz.timezone())),
    }
}

pub(crate) fn decode_datetime_tz(
    proto: typedb_protocol::value::DatetimeTz,
) -> Result<DateTime<TimeZone>, Box<ConceptDecodeError>> {
    let datetime_proto = proto.datetime.ok_or_else(|| Box::new(ConceptDecodeError::MissingDatetimeTzDatetime {}))?;
    let datetime = decode_datetime(datetime_proto)?;
    let timezone_proto = proto.timezone.ok_or_else(|| Box::new(ConceptDecodeError::MissingDatetimeTzTimezone {}))?;
    let timezone = decode_timezone(timezone_proto)?;
    Ok(timezone.from_utc_datetime(&datetime))
}

fn encode_timezone(timezone: TimeZone) -> typedb_protocol::value::datetime_tz::Timezone {
    match timezone {
        TimeZone::IANA(tz) => typedb_protocol::value::datetime_tz::Timezone::Named(tz.name().to_string()),
        TimeZone::Fixed(fixed) => typedb_protocol::value::datetime_tz::Timezone::Offset(fixed.local_minus_utc()),
    }
}

fn decode_timezone(proto: typedb_protocol::value::datetime_tz::Timezone) -> Result<TimeZone, Box<ConceptDecodeError>> {
    match proto {
        typedb_protocol::value::datetime_tz::Timezone::Named(name) => {
            let tz = Tz::from_str(&name).map_err(|_| Box::new(ConceptDecodeError::InvalidDatetimeTzName { name }))?;
            Ok(TimeZone::IANA(tz))
        }
        typedb_protocol::value::datetime_tz::Timezone::Offset(offset_seconds) => {
            let fixed_offset = if offset_seconds >= 0 {
                FixedOffset::east_opt(offset_seconds)
            } else {
                FixedOffset::west_opt(-offset_seconds)
            }
            .ok_or_else(|| Box::new(ConceptDecodeError::InvalidDatetimeTzOffset { offset_seconds }))?;
            Ok(TimeZone::Fixed(fixed_offset))
        }
    }
}

pub(crate) fn encode_duration(duration: Duration) -> typedb_protocol::value::Duration {
    typedb_protocol::value::Duration { months: duration.months, days: duration.days, nanos: duration.nanos }
}

pub(crate) fn decode_duration(proto: typedb_protocol::value::Duration) -> Result<Duration, Box<ConceptDecodeError>> {
    Ok(Duration { months: proto.months, days: proto.days, nanos: proto.nanos })
}
