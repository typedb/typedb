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
        AttributeItem, AttributeReferencedByValue, EntityItem, Guarantees, ItemOrder, LabelScopedId, MigrationItem,
        OwnedAttribute, Player, RelationItem, Role,
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

pub enum EncodedItem {
    Schema(String),
    Item(Item),
}

pub fn encode_item(item: ExportMessage) -> EncodedItem {
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
            MigrationItem::Entity(EntityItem { globally_unique_id, label, label_scoped_id, attributes }) => {
                item::Item::Entity(item::Entity {
                    globally_unique_id,
                    label: label.to_string(),
                    label_scoped_id,
                    attributes: encode_owned_attributes(attributes),
                })
            }
            MigrationItem::Relation(RelationItem { globally_unique_id, label, label_scoped_id, attributes, roles }) => {
                item::Item::Relation(item::Relation {
                    globally_unique_id,
                    label: label.to_string(),
                    label_scoped_id,
                    attributes: encode_owned_attributes(attributes),
                    roles: encode_relation_roles(roles),
                })
            }
            MigrationItem::Attribute(AttributeItem { globally_unique_id, label, value }) => {
                item::Item::Attribute(item::Attribute {
                    globally_unique_id,
                    label: label.to_string(),
                    attributes: vec![], // attributes used to own attributes in 2.x, not anymore
                    value: Some(encode_migration_value(value)),
                })
            }
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

pub fn decode_item(item_proto: Item) -> Result<MigrationItem, ItemDecodeError> {
    let Item { item } = item_proto;
    let item = item.ok_or(ItemDecodeError::EmptyItem {})?;
    let decoded = match item {
        item::Item::Header(item::Header { typedb_version, original_database, guarantees }) => MigrationItem::Header {
            typedb_version,
            original_database,
            guarantees: guarantees.map(decode_guarantees).transpose()?.unwrap_or_default(),
        },
        item::Item::Entity(item::Entity { globally_unique_id, label, label_scoped_id, attributes }) => {
            MigrationItem::Entity(EntityItem {
                globally_unique_id,
                label: Label::parse_from(&label, None),
                label_scoped_id,
                attributes: decode_owned_attributes(attributes)?,
            })
        }
        item::Item::Relation(item::Relation { globally_unique_id, label, label_scoped_id, attributes, roles }) => {
            MigrationItem::Relation(RelationItem {
                globally_unique_id,
                label: Label::parse_from(&label, None),
                label_scoped_id,
                attributes: decode_owned_attributes(attributes)?,
                roles: decode_relation_roles(roles),
            })
        }
        item::Item::Attribute(item::Attribute { globally_unique_id, label, attributes, value }) => {
            if !attributes.is_empty() {
                return Err(ItemDecodeError::AttributesOwningAttributes {});
            }
            let value = decode_migration_value(value.ok_or(ItemDecodeError::AbsentAttributeValue {})?)
                .map_err(|typedb_source| ItemDecodeError::ConceptDecode { typedb_source })?;
            MigrationItem::Attribute(AttributeItem {
                globally_unique_id,
                label: Label::parse_from(&label, None),
                value,
            })
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
        UnknownItemOrder(6, "A header with an unknown item order '{item_order}' was received. Ensure using compatible client protocol.", item_order: i32),
    }
}

fn encode_guarantees(guarantees: Guarantees) -> item::header::Guarantees {
    let Guarantees {
        item_order,
        attributes_unique,
        label_scoped_ids_present,
        owned_inlined_attributes_referenced_by_value,
    } = guarantees;
    let item_order = match item_order {
        ItemOrder::Unspecified => item::header::guarantees::ItemOrder::OrderUnspecified,
        ItemOrder::AttributesEntitiesRelations => item::header::guarantees::ItemOrder::OrderAttributesEntitiesRelations,
    };
    item::header::Guarantees {
        item_order: item_order as i32,
        attributes_unique,
        label_scoped_ids_present,
        owned_inlined_attributes_referenced_by_value,
    }
}

fn decode_guarantees(guarantees: item::header::Guarantees) -> Result<Guarantees, ItemDecodeError> {
    let item::header::Guarantees {
        item_order,
        attributes_unique,
        label_scoped_ids_present,
        owned_inlined_attributes_referenced_by_value,
    } = guarantees;
    let item_order = match item::header::guarantees::ItemOrder::try_from(item_order) {
        Ok(item::header::guarantees::ItemOrder::OrderUnspecified) => ItemOrder::Unspecified,
        Ok(item::header::guarantees::ItemOrder::OrderAttributesEntitiesRelations) => {
            ItemOrder::AttributesEntitiesRelations
        }
        Err(_) => return Err(ItemDecodeError::UnknownItemOrder { item_order }),
    };
    Ok(Guarantees {
        item_order,
        attributes_unique,
        label_scoped_ids_present,
        owned_inlined_attributes_referenced_by_value,
    })
}

fn encode_owned_attributes(owned_attributes: Vec<OwnedAttribute>) -> Vec<item::OwnedAttribute> {
    owned_attributes
        .into_iter()
        .map(|OwnedAttribute { globally_unique_id, attribute_by_value }| item::OwnedAttribute {
            globally_unique_id,
            attribute_by_value: attribute_by_value.map(|AttributeReferencedByValue { label, value }| {
                item::owned_attribute::AttributeReferencedByValue {
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
        .map(|item::OwnedAttribute { globally_unique_id, attribute_by_value }| {
            let attribute_by_value = attribute_by_value
                .map(|item::owned_attribute::AttributeReferencedByValue { label, value }| {
                    let value = decode_migration_value(value.ok_or(ItemDecodeError::AbsentOwnedAttributeValue {})?)
                        .map_err(|typedb_source| ItemDecodeError::ConceptDecode { typedb_source })?;
                    Ok(AttributeReferencedByValue { label: Label::parse_from(&label, None), value })
                })
                .transpose()?;
            Ok(OwnedAttribute { globally_unique_id, attribute_by_value })
        })
        .collect()
}

fn encode_relation_roles(roles: Vec<Role>) -> Vec<item::relation::Role> {
    roles
        .into_iter()
        .map(|Role { label, players }| item::relation::Role {
            label: label.to_string(),
            players: players
                .into_iter()
                .map(|Player { globally_unique_id, label_scoped_id }| item::relation::role::Player {
                    globally_unique_id,
                    label_scoped_id: label_scoped_id.map(|LabelScopedId { label, label_scoped_id }| {
                        item::relation::role::player::LabelScopedId { label: label.to_string(), label_scoped_id }
                    }),
                })
                .collect(),
        })
        .collect()
}

fn decode_relation_roles(roles: Vec<item::relation::Role>) -> Vec<Role> {
    roles
        .into_iter()
        .map(|item::relation::Role { label, players }| Role {
            label: Label::parse_from(&label, None),
            players: players
                .into_iter()
                .map(|item::relation::role::Player { globally_unique_id, label_scoped_id }| Player {
                    globally_unique_id,
                    label_scoped_id: label_scoped_id.map(
                        |item::relation::role::player::LabelScopedId { label, label_scoped_id }| LabelScopedId {
                            label: Label::parse_from(&label, None),
                            label_scoped_id,
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
    fn an_unknown_item_order_is_rejected() {
        let header = wire(item::Item::Header(item::Header {
            typedb_version: "3.12.3".to_owned(),
            original_database: "source".to_owned(),
            guarantees: Some(item::header::Guarantees { item_order: 99, ..Default::default() }),
        }));
        assert!(matches!(decode_item(header), Err(ItemDecodeError::UnknownItemOrder { item_order: 99 })));
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
                        item_order: ItemOrder::AttributesEntitiesRelations,
                        attributes_unique: true,
                        label_scoped_ids_present: true,
                        owned_inlined_attributes_referenced_by_value: false,
                    },
                },
                wire(item::Item::Header(item::Header {
                    typedb_version: "3.12.3".to_owned(),
                    original_database: "source".to_owned(),
                    guarantees: Some(item::header::Guarantees {
                        item_order: item::header::guarantees::ItemOrder::OrderAttributesEntitiesRelations as i32,
                        attributes_unique: true,
                        label_scoped_ids_present: true,
                        owned_inlined_attributes_referenced_by_value: false,
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
                    globally_unique_id: "e1".to_owned(),
                    label: Label::build("person", None),
                    label_scoped_id: Some(3),
                    attributes: vec![
                        OwnedAttribute::by_id("a1".to_owned()),
                        OwnedAttribute {
                            globally_unique_id: "a2".to_owned(),
                            attribute_by_value: Some(AttributeReferencedByValue {
                                label: Label::build("age", None),
                                value: Value::Integer(42),
                            }),
                        },
                    ],
                }),
                wire(item::Item::Entity(item::Entity {
                    globally_unique_id: "e1".to_owned(),
                    label: "person".to_owned(),
                    label_scoped_id: Some(3),
                    attributes: vec![
                        item::OwnedAttribute { globally_unique_id: "a1".to_owned(), attribute_by_value: None },
                        item::OwnedAttribute {
                            globally_unique_id: "a2".to_owned(),
                            attribute_by_value: Some(item::owned_attribute::AttributeReferencedByValue {
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
                    globally_unique_id: "r1".to_owned(),
                    label: Label::build("friendship", None),
                    label_scoped_id: None,
                    attributes: vec![OwnedAttribute::by_id("a1".to_owned())],
                    roles: vec![Role {
                        label: Label::build_scoped("friend", "friendship", None),
                        players: vec![
                            Player::by_id("e1".to_owned()),
                            Player {
                                globally_unique_id: "e2".to_owned(),
                                label_scoped_id: Some(LabelScopedId {
                                    label: Label::build("person", None),
                                    label_scoped_id: 7,
                                }),
                            },
                        ],
                    }],
                }),
                wire(item::Item::Relation(item::Relation {
                    globally_unique_id: "r1".to_owned(),
                    label: "friendship".to_owned(),
                    label_scoped_id: None,
                    attributes: vec![item::OwnedAttribute {
                        globally_unique_id: "a1".to_owned(),
                        attribute_by_value: None,
                    }],
                    roles: vec![item::relation::Role {
                        label: "friendship:friend".to_owned(),
                        players: vec![
                            item::relation::role::Player { globally_unique_id: "e1".to_owned(), label_scoped_id: None },
                            item::relation::role::Player {
                                globally_unique_id: "e2".to_owned(),
                                label_scoped_id: Some(item::relation::role::player::LabelScopedId {
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
                    globally_unique_id: "a1".to_owned(),
                    label: Label::build("name", None),
                    value: Value::String(Cow::Borrowed("Alice")),
                }),
                wire(item::Item::Attribute(item::Attribute {
                    globally_unique_id: "a1".to_owned(),
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
