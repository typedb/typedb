/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::borrow::Cow;

use concept::error::ConceptDecodeError;
use database::migration::{Checksums, item::MigrationItem};
use encoding::value::{label::Label, value::Value};
use error::{typedb_error, unimplemented_feature};
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
                roles: encode_relation_roles(related_role_players),
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
    let item = item.ok_or(ItemDecodeError::EmptyItem {})?;
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
            related_role_players: decode_relation_roles(roles),
        },
        item::Item::Attribute(item::Attribute { id, label, attributes, value }) => {
            if !attributes.is_empty() {
                return Err(ItemDecodeError::AttributesOwningAttributes {});
            }
            let value = decode_migration_value(value.ok_or(ItemDecodeError::AbsentAttributeValue {})?)
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

typedb_error! {
    pub ItemDecodeError(component = "Migration item decode", prefix = "MID") {
        EmptyItem(1, "An empty item was received."),
        AbsentAttributeValue(2, "An attribute item without a value was received."),
        AttributesOwningAttributes(3, "An item with attributes owning attributes was received."),
        ConceptDecode(4, "Error decoding an item's concept.", typedb_source: Box<ConceptDecodeError>),
    }
}

fn encode_owned_attributes(owned_attributes: Vec<String>) -> Vec<item::OwnedAttribute> {
    owned_attributes.into_iter().map(|id| item::OwnedAttribute { id }).collect()
}

fn decode_owned_attributes(attributes: Vec<item::OwnedAttribute>) -> Vec<String> {
    attributes.into_iter().map(|item::OwnedAttribute { id }| id).collect()
}

fn encode_relation_roles(related_role_players: Vec<(Label, Vec<String>)>) -> Vec<item::relation::Role> {
    related_role_players
        .into_iter()
        .map(|(label, players)| item::relation::Role {
            label: label.to_string(),
            players: players.into_iter().map(|id| item::relation::role::Player { id }).collect(),
        })
        .collect()
}

fn decode_relation_roles(roles: Vec<item::relation::Role>) -> Vec<(Label, Vec<String>)> {
    roles
        .into_iter()
        .map(|item::relation::Role { label, players }| {
            (
                Label::parse_from(&label, None),
                players.into_iter().map(|item::relation::role::Player { id }| id).collect(),
            )
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn items_map_to_the_expected_wire_messages() {
        for (item, expected) in samples() {
            match &item {
                MigrationItem::Schema(_)
                | MigrationItem::Header { .. }
                | MigrationItem::Entity { .. }
                | MigrationItem::Relation { .. }
                | MigrationItem::Attribute { .. }
                | MigrationItem::Checksums(_) => (),
            }
            assert_eq!(wire_item(encode_item(item)), expected);
            let decoded = decode_item(expected.clone()).expect("decoded item");
            assert_eq!(wire_item(encode_item(decoded)), expected, "decoding is not the inverse of encoding");
        }
    }

    #[test]
    fn the_schema_is_not_a_wire_item() {
        match encode_item(MigrationItem::Schema("define entity person;".to_owned())) {
            EncodedItem::Schema(schema) => assert_eq!(schema, "define entity person;"),
            EncodedItem::Item(item) => panic!("the schema must not be encoded as an item: {item:?}"),
        }
    }

    fn samples() -> Vec<(MigrationItem, Item)> {
        vec![
            (
                MigrationItem::Header { typedb_version: "3.12.3".to_owned(), original_database: "source".to_owned() },
                wire(item::Item::Header(item::Header {
                    typedb_version: "3.12.3".to_owned(),
                    original_database: "source".to_owned(),
                })),
            ),
            (
                MigrationItem::Entity {
                    id: "e1".to_owned(),
                    label: Label::build("person", None),
                    owned_attributes: vec!["a1".to_owned(), "a2".to_owned()],
                },
                wire(item::Item::Entity(item::Entity {
                    id: "e1".to_owned(),
                    label: "person".to_owned(),
                    attributes: vec![
                        item::OwnedAttribute { id: "a1".to_owned() },
                        item::OwnedAttribute { id: "a2".to_owned() },
                    ],
                })),
            ),
            (
                MigrationItem::Relation {
                    id: "r1".to_owned(),
                    label: Label::build("friendship", None),
                    owned_attributes: vec!["a1".to_owned()],
                    related_role_players: vec![(
                        Label::build_scoped("friend", "friendship", None),
                        vec!["e1".to_owned(), "e2".to_owned()],
                    )],
                },
                wire(item::Item::Relation(item::Relation {
                    id: "r1".to_owned(),
                    label: "friendship".to_owned(),
                    attributes: vec![item::OwnedAttribute { id: "a1".to_owned() }],
                    roles: vec![item::relation::Role {
                        label: "friendship:friend".to_owned(),
                        players: vec![
                            item::relation::role::Player { id: "e1".to_owned() },
                            item::relation::role::Player { id: "e2".to_owned() },
                        ],
                    }],
                })),
            ),
            (
                MigrationItem::Attribute {
                    id: "a1".to_owned(),
                    label: Label::build("name", None),
                    value: Value::String(Cow::Borrowed("Alice")),
                },
                wire(item::Item::Attribute(item::Attribute {
                    id: "a1".to_owned(),
                    label: "name".to_owned(),
                    attributes: vec![],
                    value: Some(MigrationValue {
                        value: Some(migration::migration_value::Value::String("Alice".to_owned())),
                    }),
                })),
            ),
            (
                MigrationItem::Checksums(Checksums {
                    entity_count: 1,
                    attribute_count: 2,
                    relation_count: 3,
                    role_count: 4,
                    ownership_count: 5,
                }),
                wire(item::Item::Checksums(item::Checksums {
                    entity_count: 1,
                    attribute_count: 2,
                    relation_count: 3,
                    role_count: 4,
                    ownership_count: 5,
                })),
            ),
        ]
    }

    fn wire(item: item::Item) -> Item {
        Item { item: Some(item) }
    }

    fn wire_item(encoded: EncodedItem) -> Item {
        match encoded {
            EncodedItem::Item(item) => item,
            EncodedItem::Schema(schema) => panic!("expected a wire item, not a schema: {schema}"),
        }
    }
}
