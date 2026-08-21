/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::borrow::Cow;

use concept::error::ConceptDecodeError;
use database::migration::{Checksums, item::MigrationItem};
use encoding::value::{label::Label, value::Value};
use error::unimplemented_feature;
use typedb_protocol::{
    migration,
    migration::{Item, MigrationValue, item},
};

use crate::service::grpc::concept::{
    decode_date, decode_datetime, decode_datetime_from_millis, decode_datetime_tz, decode_decimal, decode_duration,
    encode_date, encode_datetime, encode_datetime_tz, encode_decimal, encode_duration,
};

pub(crate) enum EncodedItem {
    Schema(String),
    Item(Item),
}

pub(crate) fn encode_item(item: MigrationItem) -> EncodedItem {
    let encoded = match item {
        MigrationItem::Schema(schema) => return EncodedItem::Schema(schema),
        MigrationItem::Header { typedb_version, original_database } => {
            item::Item::Header(item::Header { typedb_version, original_database })
        }
        MigrationItem::Entity { id, label, owned_attributes } => item::Item::Entity(item::Entity {
            id,
            label: label.to_string(),
            attributes: encode_owned_attributes(owned_attributes),
        }),
        MigrationItem::Relation { id, label, owned_attributes, related_role_players } => {
            item::Item::Relation(item::Relation {
                id,
                label: label.to_string(),
                attributes: encode_owned_attributes(owned_attributes),
                roles: related_role_players
                    .into_iter()
                    .map(|(label, players)| item::relation::Role {
                        label: label.to_string(),
                        players: players.into_iter().map(|id| item::relation::role::Player { id }).collect(),
                    })
                    .collect(),
            })
        }
        MigrationItem::Attribute { id, label, value } => item::Item::Attribute(item::Attribute {
            id,
            label: label.to_string(),
            attributes: vec![], // attributes cannot own attributes anymore
            value: Some(encode_migration_value(value)),
        }),
        MigrationItem::Checksums(checksums) => item::Item::Checksums(item::Checksums {
            entity_count: checksums.entity_count,
            attribute_count: checksums.attribute_count,
            relation_count: checksums.relation_count,
            role_count: checksums.role_count,
            ownership_count: checksums.ownership_count,
        }),
    };
    EncodedItem::Item(Item { item: Some(encoded) })
}

pub(crate) fn decode_item(item_proto: Item) -> Result<MigrationItem, ItemDecodeError> {
    let Item { item } = item_proto;
    let item = item.ok_or(ItemDecodeError::EmptyItem)?;
    let decoded = match item {
        item::Item::Header(item::Header { typedb_version, original_database }) => {
            MigrationItem::Header { typedb_version, original_database }
        }
        item::Item::Entity(item::Entity { id, label, attributes }) => MigrationItem::Entity {
            id,
            label: Label::parse_from(&label, None),
            owned_attributes: decode_owned_attributes(attributes),
        },
        item::Item::Relation(item::Relation { id, label, attributes, roles }) => MigrationItem::Relation {
            id,
            label: Label::parse_from(&label, None),
            owned_attributes: decode_owned_attributes(attributes),
            related_role_players: roles
                .into_iter()
                .map(|item::relation::Role { label, players }| {
                    (
                        Label::parse_from(&label, None),
                        players.into_iter().map(|item::relation::role::Player { id }| id).collect(),
                    )
                })
                .collect(),
        },
        item::Item::Attribute(item::Attribute { id, label, attributes, value }) => {
            if !attributes.is_empty() {
                return Err(ItemDecodeError::AttributesOwningAttributes);
            }
            let value = decode_migration_value(value.ok_or(ItemDecodeError::AbsentAttributeValue)?)
                .map_err(|typedb_source| ItemDecodeError::ConceptDecode { typedb_source })?;
            MigrationItem::Attribute { id, label: Label::parse_from(&label, None), value }
        }
        item::Item::Checksums(checksums) => MigrationItem::Checksums(Checksums {
            entity_count: checksums.entity_count,
            attribute_count: checksums.attribute_count,
            relation_count: checksums.relation_count,
            role_count: checksums.role_count,
            ownership_count: checksums.ownership_count,
        }),
    };
    Ok(decoded)
}

pub(crate) enum ItemDecodeError {
    EmptyItem,
    AbsentAttributeValue,
    AttributesOwningAttributes,
    ConceptDecode { typedb_source: Box<ConceptDecodeError> },
}

fn encode_owned_attributes(owned_attributes: Vec<String>) -> Vec<item::OwnedAttribute> {
    owned_attributes.into_iter().map(|id| item::OwnedAttribute { id }).collect()
}

fn decode_owned_attributes(attributes: Vec<item::OwnedAttribute>) -> Vec<String> {
    attributes.into_iter().map(|item::OwnedAttribute { id }| id).collect()
}

fn encode_migration_value(value: Value<'static>) -> MigrationValue {
    use migration::migration_value::Value as ValueProto;
    // TODO: We depend on grpc crate (concept), but this format of migration is generic and just
    // happens to be a grpc message. Is this dependency healthy? Resolve in embedded
    let value_message = match value {
        Value::Boolean(boolean) => ValueProto::Boolean(boolean),
        Value::Integer(integer) => ValueProto::Integer(integer),
        Value::Double(double) => ValueProto::Double(double),
        Value::Decimal(decimal) => ValueProto::Decimal(encode_decimal(decimal)),
        Value::Date(date) => ValueProto::Date(encode_date(date)),
        Value::DateTime(date_time) => ValueProto::Datetime(encode_datetime(date_time)),
        Value::DateTimeTZ(datetime_tz) => ValueProto::DatetimeTz(encode_datetime_tz(datetime_tz)),
        Value::Duration(duration) => ValueProto::Duration(encode_duration(duration)),
        Value::String(string) => ValueProto::String(string.to_string()),
        Value::Struct(_struct) => unimplemented_feature!(Structs),
    };
    MigrationValue { value: Some(value_message) }
}

fn decode_migration_value(value_proto: MigrationValue) -> Result<Value<'static>, Box<ConceptDecodeError>> {
    use migration::migration_value::Value as ValueProto;
    let value_proto = value_proto.value.ok_or_else(|| Box::new(ConceptDecodeError::NoValue {}))?;
    let value = match value_proto {
        ValueProto::Boolean(boolean) => Value::Boolean(boolean),
        ValueProto::Integer(integer) => Value::Integer(integer),
        ValueProto::Double(double) => Value::Double(double),
        ValueProto::Decimal(decimal) => Value::Decimal(decode_decimal(decimal)?),
        ValueProto::Date(date) => Value::Date(decode_date(date)?),
        ValueProto::Datetime(date_time) => Value::DateTime(decode_datetime(date_time)?),
        ValueProto::DatetimeTz(datetime_tz) => Value::DateTimeTZ(decode_datetime_tz(datetime_tz)?),
        ValueProto::DatetimeMillis(millis) => Value::DateTime(decode_datetime_from_millis(millis)?),
        ValueProto::Duration(duration) => Value::Duration(decode_duration(duration)?),
        ValueProto::String(string) => Value::String(Cow::Owned(string)),
        ValueProto::Struct(_struct) => unimplemented_feature!(Structs),
    };
    Ok(value)
}
