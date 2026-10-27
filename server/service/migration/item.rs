/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::borrow::Cow;

use concept::error::ConceptDecodeError;
use database::migration::{
    Checksums, Counts,
    database_exporter::ExportMessage,
    item::{
        AttributeByValue, AttributeItem, EntityItem, Guarantees, ItemOrder, MigrationItem, OwnedAttribute, Player,
        PlayerByLabelScopedId, RelationItem, RolePlayers,
    },
};
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

pub(crate) fn encode_item(item: ExportMessage) -> EncodedItem {
    let encoded = match item {
        ExportMessage::Schema(schema) => return EncodedItem::Schema(schema),
        ExportMessage::Item(item) => match item {
            MigrationItem::Header { typedb_version, original_database, guarantees } => {
                item::Item::Header(item::Header {
                    typedb_version,
                    original_database,
                    guarantees: Some(encode_guarantees(guarantees)),
                })
            }
            MigrationItem::Entity(EntityItem { id, label, label_scoped_id, owned_attributes }) => {
                item::Item::Entity(item::Entity {
                    id,
                    label: label.to_string(),
                    label_scoped_id,
                    attributes: encode_owned_attributes(owned_attributes),
                })
            }
            MigrationItem::Relation(RelationItem {
                id,
                label,
                label_scoped_id,
                owned_attributes,
                related_role_players,
            }) => item::Item::Relation(item::Relation {
                id,
                label: label.to_string(),
                label_scoped_id,
                attributes: encode_owned_attributes(owned_attributes),
                roles: encode_relation_roles(related_role_players),
            }),
            MigrationItem::Attribute(AttributeItem { id, label, value }) => item::Item::Attribute(item::Attribute {
                id,
                label: label.to_string(),
                attributes: vec![], // attributes used to own attributes in 2.x, not anymore
                value: Some(encode_migration_value(value)),
            }),
            MigrationItem::Checksums(checksums) => {
                let Counts { entity_count, attribute_count, relation_count, role_count, ownership_count } =
                    checksums.counts();
                item::Item::Checksums(item::Checksums {
                    entity_count,
                    attribute_count,
                    relation_count,
                    role_count,
                    ownership_count,
                })
            }
        },
    };
    EncodedItem::Item(Item { item: Some(encoded) })
}

pub(crate) fn decode_item(item_proto: Item) -> Result<MigrationItem, ItemDecodeError> {
    let Item { item } = item_proto;
    let item = item.ok_or(ItemDecodeError::EmptyItem {})?;
    let decoded = match item {
        item::Item::Header(item::Header { typedb_version, original_database, guarantees }) => MigrationItem::Header {
            typedb_version,
            original_database,
            guarantees: guarantees.map(decode_guarantees).transpose()?.unwrap_or_default(),
        },
        item::Item::Entity(item::Entity { id, label, label_scoped_id, attributes }) => {
            MigrationItem::Entity(EntityItem {
                id,
                label: Label::parse_from(&label, None),
                label_scoped_id,
                owned_attributes: decode_owned_attributes(attributes)?,
            })
        }
        item::Item::Relation(item::Relation { id, label, label_scoped_id, attributes, roles }) => {
            MigrationItem::Relation(RelationItem {
                id,
                label: Label::parse_from(&label, None),
                label_scoped_id,
                owned_attributes: decode_owned_attributes(attributes)?,
                related_role_players: decode_relation_roles(roles),
            })
        }
        item::Item::Attribute(item::Attribute { id, label, attributes, value }) => {
            if !attributes.is_empty() {
                return Err(ItemDecodeError::AttributesOwningAttributes {});
            }
            let value = decode_migration_value(value.ok_or(ItemDecodeError::AbsentAttributeValue {})?)
                .map_err(|typedb_source| ItemDecodeError::ConceptDecode { typedb_source })?;
            MigrationItem::Attribute(AttributeItem { id, label: Label::parse_from(&label, None), value })
        }
        item::Item::Checksums(item::Checksums {
            entity_count,
            attribute_count,
            relation_count,
            role_count,
            ownership_count,
        }) => MigrationItem::Checksums(Checksums::from_counts(Counts {
            entity_count,
            attribute_count,
            relation_count,
            role_count,
            ownership_count,
        })),
    };
    Ok(decoded)
}

typedb_error! {
    pub ItemDecodeError(component = "Migration item decode", prefix = "MID") {
        EmptyItem(1, "An empty concept item was received. It is a sign of a corrupted file or a client bug."),
        AbsentAttributeValue(2, "An attribute item without a value was received."),
        AttributesOwningAttributes(3, "Invalid migration item received: attributes cannot own attributes in this version of TypeDB (this was deprecated). Please modify your data accordingly and reexport the original database before trying again."),
        ConceptDecode(4, "Error decoding an item's concept.", typedb_source: Box<ConceptDecodeError>),
        AbsentOwnedAttributeValue(5, "An owned attribute referenced by value, but without a value, was received."),
        UnknownOrdering(6, "A header with an unknown item ordering '{ordering}' was received. Ensure using compatible client protocol.", ordering: i32),
    }
}

fn encode_guarantees(guarantees: Guarantees) -> item::header::Guarantees {
    let Guarantees { ordering, attributes_unique, players_by_label_scoped_ids, owned_attributes_by_value } = guarantees;
    let ordering = match ordering {
        ItemOrder::Unspecified => item::header::guarantees::Ordering::Unspecified,
        ItemOrder::AttributeEntityRelation => item::header::guarantees::Ordering::AttributesEntitiesRelations,
    };
    item::header::Guarantees {
        ordering: ordering as i32,
        attributes_unique,
        players_by_label_scoped_ids,
        owned_attributes_by_value,
    }
}

fn decode_guarantees(guarantees: item::header::Guarantees) -> Result<Guarantees, ItemDecodeError> {
    let item::header::Guarantees {
        ordering,
        attributes_unique,
        players_by_label_scoped_ids,
        owned_attributes_by_value,
    } = guarantees;
    let ordering = match item::header::guarantees::Ordering::try_from(ordering) {
        Ok(item::header::guarantees::Ordering::Unspecified) => ItemOrder::Unspecified,
        Ok(item::header::guarantees::Ordering::AttributesEntitiesRelations) => ItemOrder::AttributeEntityRelation,
        Err(_) => return Err(ItemDecodeError::UnknownOrdering { ordering }),
    };
    Ok(Guarantees { ordering, attributes_unique, players_by_label_scoped_ids, owned_attributes_by_value })
}

fn encode_owned_attributes(owned_attributes: Vec<OwnedAttribute>) -> Vec<item::OwnedAttribute> {
    owned_attributes
        .into_iter()
        .map(|OwnedAttribute { id, attribute_by_value }| item::OwnedAttribute {
            id,
            attribute_by_value: attribute_by_value.map(|AttributeByValue { label, value }| {
                item::owned_attribute::AttributeByValue {
                    label: label.to_string(),
                    value: Some(encode_migration_value(value)),
                }
            }),
        })
        .collect()
}

fn decode_owned_attributes(attributes: Vec<item::OwnedAttribute>) -> Result<Vec<OwnedAttribute>, ItemDecodeError> {
    attributes
        .into_iter()
        .map(|item::OwnedAttribute { id, attribute_by_value }| {
            let attribute_by_value = attribute_by_value
                .map(|item::owned_attribute::AttributeByValue { label, value }| {
                    let value = decode_migration_value(value.ok_or(ItemDecodeError::AbsentOwnedAttributeValue {})?)
                        .map_err(|typedb_source| ItemDecodeError::ConceptDecode { typedb_source })?;
                    Ok(AttributeByValue { label: Label::parse_from(&label, None), value })
                })
                .transpose()?;
            Ok(OwnedAttribute { id, attribute_by_value })
        })
        .collect()
}

fn encode_relation_roles(related_role_players: Vec<RolePlayers>) -> Vec<item::relation::Role> {
    related_role_players
        .into_iter()
        .map(|RolePlayers { role, players }| item::relation::Role {
            label: role.to_string(),
            players: players
                .into_iter()
                .map(|Player { id, player_by_label_scoped_id }| item::relation::role::Player {
                    id,
                    player_by_label_scoped_id: player_by_label_scoped_id.map(
                        |PlayerByLabelScopedId { label, label_scoped_id }| {
                            item::relation::role::player::PlayerByLabelScopedId {
                                label: label.to_string(),
                                label_scoped_id,
                            }
                        },
                    ),
                })
                .collect(),
        })
        .collect()
}

fn decode_relation_roles(roles: Vec<item::relation::Role>) -> Vec<RolePlayers> {
    roles
        .into_iter()
        .map(|item::relation::Role { label, players }| RolePlayers {
            role: Label::parse_from(&label, None),
            players: players
                .into_iter()
                .map(|item::relation::role::Player { id, player_by_label_scoped_id }| Player {
                    id,
                    player_by_label_scoped_id: player_by_label_scoped_id.map(
                        |item::relation::role::player::PlayerByLabelScopedId { label, label_scoped_id }| {
                            PlayerByLabelScopedId { label: Label::parse_from(&label, None), label_scoped_id }
                        },
                    ),
                })
                .collect(),
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
                MigrationItem::Header { .. }
                | MigrationItem::Entity(_)
                | MigrationItem::Relation(_)
                | MigrationItem::Attribute(_)
                | MigrationItem::Checksums(_) => (),
            }
            assert_eq!(wire_item(encode_item(ExportMessage::Item(item))), expected);
            let decoded = decode_item(expected.clone()).expect("decoded item");
            assert_eq!(
                wire_item(encode_item(ExportMessage::Item(decoded))),
                expected,
                "decoding is not the inverse of encoding"
            );
        }
    }

    #[test]
    fn a_header_without_guarantees_promises_nothing() {
        let header = wire(item::Item::Header(item::Header {
            typedb_version: "3.12.3".to_owned(),
            original_database: "source".to_owned(),
            guarantees: None,
        }));
        match decode_item(header).expect("decoded header") {
            MigrationItem::Header { guarantees, .. } => assert_eq!(guarantees, Guarantees::default()),
            other => panic!("expected a header, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_ordering_is_rejected() {
        let header = wire(item::Item::Header(item::Header {
            typedb_version: "3.12.3".to_owned(),
            original_database: "source".to_owned(),
            guarantees: Some(item::header::Guarantees { ordering: 99, ..Default::default() }),
        }));
        assert!(matches!(decode_item(header), Err(ItemDecodeError::UnknownOrdering { ordering: 99 })));
    }

    #[test]
    fn the_schema_is_not_a_wire_item() {
        match encode_item(ExportMessage::Schema("define entity person;".to_owned())) {
            EncodedItem::Schema(schema) => assert_eq!(schema, "define entity person;"),
            EncodedItem::Item(item) => panic!("the schema must not be encoded as an item: {item:?}"),
        }
    }

    fn samples() -> Vec<(MigrationItem, Item)> {
        vec![
            (
                MigrationItem::Header {
                    typedb_version: "3.12.3".to_owned(),
                    original_database: "source".to_owned(),
                    guarantees: Guarantees {
                        ordering: ItemOrder::AttributeEntityRelation,
                        attributes_unique: true,
                        players_by_label_scoped_ids: true,
                        owned_attributes_by_value: false,
                    },
                },
                wire(item::Item::Header(item::Header {
                    typedb_version: "3.12.3".to_owned(),
                    original_database: "source".to_owned(),
                    guarantees: Some(item::header::Guarantees {
                        ordering: item::header::guarantees::Ordering::AttributesEntitiesRelations as i32,
                        attributes_unique: true,
                        players_by_label_scoped_ids: true,
                        owned_attributes_by_value: false,
                    }),
                })),
            ),
            (
                MigrationItem::Header {
                    typedb_version: "3.12.3".to_owned(),
                    original_database: "source".to_owned(),
                    guarantees: Guarantees::default(),
                },
                wire(item::Item::Header(item::Header {
                    typedb_version: "3.12.3".to_owned(),
                    original_database: "source".to_owned(),
                    guarantees: Some(item::header::Guarantees::default()),
                })),
            ),
            (
                MigrationItem::Entity(EntityItem {
                    id: "e1".to_owned(),
                    label: Label::build("person", None),
                    label_scoped_id: Some(3),
                    owned_attributes: vec![
                        OwnedAttribute::by_id("a1".to_owned()),
                        OwnedAttribute {
                            id: "a2".to_owned(),
                            attribute_by_value: Some(AttributeByValue {
                                label: Label::build("age", None),
                                value: Value::Integer(42),
                            }),
                        },
                    ],
                }),
                wire(item::Item::Entity(item::Entity {
                    id: "e1".to_owned(),
                    label: "person".to_owned(),
                    label_scoped_id: Some(3),
                    attributes: vec![
                        item::OwnedAttribute { id: "a1".to_owned(), attribute_by_value: None },
                        item::OwnedAttribute {
                            id: "a2".to_owned(),
                            attribute_by_value: Some(item::owned_attribute::AttributeByValue {
                                label: "age".to_owned(),
                                value: Some(MigrationValue {
                                    value: Some(migration::migration_value::Value::Integer(42)),
                                }),
                            }),
                        },
                    ],
                })),
            ),
            (
                MigrationItem::Relation(RelationItem {
                    id: "r1".to_owned(),
                    label: Label::build("friendship", None),
                    label_scoped_id: None,
                    owned_attributes: vec![OwnedAttribute::by_id("a1".to_owned())],
                    related_role_players: vec![RolePlayers {
                        role: Label::build_scoped("friend", "friendship", None),
                        players: vec![
                            Player::by_id("e1".to_owned()),
                            Player {
                                id: "e2".to_owned(),
                                player_by_label_scoped_id: Some(PlayerByLabelScopedId {
                                    label: Label::build("person", None),
                                    label_scoped_id: 7,
                                }),
                            },
                        ],
                    }],
                }),
                wire(item::Item::Relation(item::Relation {
                    id: "r1".to_owned(),
                    label: "friendship".to_owned(),
                    label_scoped_id: None,
                    attributes: vec![item::OwnedAttribute { id: "a1".to_owned(), attribute_by_value: None }],
                    roles: vec![item::relation::Role {
                        label: "friendship:friend".to_owned(),
                        players: vec![
                            item::relation::role::Player { id: "e1".to_owned(), player_by_label_scoped_id: None },
                            item::relation::role::Player {
                                id: "e2".to_owned(),
                                player_by_label_scoped_id: Some(item::relation::role::player::PlayerByLabelScopedId {
                                    label: "person".to_owned(),
                                    label_scoped_id: 7,
                                }),
                            },
                        ],
                    }],
                })),
            ),
            (
                MigrationItem::Attribute(AttributeItem {
                    id: "a1".to_owned(),
                    label: Label::build("name", None),
                    value: Value::String(Cow::Borrowed("Alice")),
                }),
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
                MigrationItem::Checksums(Checksums::from_counts(Counts {
                    entity_count: 1,
                    attribute_count: 2,
                    relation_count: 3,
                    role_count: 4,
                    ownership_count: 5,
                })),
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
