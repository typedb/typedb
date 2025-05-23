use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
};

use bytes::util::HexBytesFormatter;
use concept::{
    error::{ConceptDecodeError, ConceptReadError},
    thing::{
        attribute::Attribute, entity::Entity, object::ObjectAPI, relation::Relation, thing_manager::ThingManager,
        ThingAPI,
    },
    type_::{role_type::RoleType, type_manager::TypeManager, TypeAPI},
};
use encoding::value::value::Value;
use error::unimplemented_feature;
use itertools::Itertools;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;
use typedb_protocol::{
    migration,
    migration::{item, Item, MigrationValue},
    value,
};

use crate::service::grpc::{
    concept::{
        decode_date, decode_datetime, decode_datetime_from_millis, decode_datetime_tz, decode_decimal, decode_duration,
        encode_date, encode_datetime, encode_datetime_tz, encode_decimal, encode_duration,
    },
    migration::Checksums,
};

pub(crate) fn encode_entity_item(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    checksums: &mut Checksums,
    entity: Entity,
) -> Result<Item, Box<ConceptReadError>> {
    Ok(encode_item(item::Item::Entity(item::Entity {
        id: encode_thing_iid(&entity),
        label: encode_type_label(snapshot, type_manager, entity.type_())?,
        attributes: encode_owned_attributes(snapshot, thing_manager, checksums, entity.clone())?,
    })))
}

pub(crate) fn encode_relation_item(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    checksums: &mut Checksums,
    relation: Relation,
) -> Result<Item, Box<ConceptReadError>> {
    Ok(encode_item(item::Item::Relation(item::Relation {
        id: encode_thing_iid(&relation),
        label: encode_type_label(snapshot, type_manager, relation.type_())?,
        attributes: encode_owned_attributes(snapshot, thing_manager, checksums, relation.clone())?,
        roles: encode_relation_roles(snapshot, type_manager, thing_manager, checksums, relation)?,
    })))
}

pub(crate) fn encode_attribute_item(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    attribute: Attribute,
) -> Result<Item, Box<ConceptReadError>> {
    Ok(encode_item(item::Item::Attribute(item::Attribute {
        id: encode_thing_iid(&attribute),
        label: encode_type_label(snapshot, type_manager, attribute.type_())?,
        attributes: vec![], // attributes cannot own attributes anymore
        value: Some(encode_migration_value(snapshot, thing_manager, &attribute)?),
    })))
}

pub(crate) fn encode_header_item(typedb_version: String, original_database: String) -> Item {
    encode_item(item::Item::Header(item::Header { typedb_version, original_database }))
}

pub(crate) fn encode_checksums_item(checksums: &Checksums) -> Item {
    encode_item(item::Item::Checksums(item::Checksums {
        entity_count: checksums.entity_count,
        attribute_count: checksums.attribute_count,
        relation_count: checksums.relation_count,
        role_count: checksums.role_count,
        ownership_count: checksums.ownership_count,
    }))
}

fn encode_item(inner_item: item::Item) -> Item {
    Item { item: Some(inner_item) }
}

fn encode_thing_iid(thing: &impl ThingAPI) -> String {
    HexBytesFormatter::borrowed(thing.iid().borrow()).format_iid()
}

fn encode_type_label(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: impl TypeAPI,
) -> Result<String, Box<ConceptReadError>> {
    let label = type_.get_label(snapshot, type_manager)?;
    Ok(label.to_string())
}

fn encode_owned_attributes(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    checksums: &mut Checksums,
    object: impl ObjectAPI,
) -> Result<Vec<item::OwnedAttribute>, Box<ConceptReadError>> {
    let mut item_owned_attributes = Vec::new();
    // TODO: Cover has ordering
    let all_has = object.get_has_unordered(snapshot, thing_manager, StorageCounters::DISABLED)?;
    for has in all_has {
        let (has, count) = has?;
        for _ in 0..count {
            item_owned_attributes.push(encode_owned_attribute(&has.attribute()));
            checksums.ownership_count += 1;
        }
    }
    Ok(item_owned_attributes)
}

fn encode_relation_roles(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    checksums: &mut Checksums,
    relation: Relation,
) -> Result<Vec<item::relation::Role>, Box<ConceptReadError>> {
    // TODO: Cover role players ordering
    let mut item_players: HashMap<RoleType, Vec<item::relation::role::Player>> = HashMap::new();
    let all_players = relation.get_players(snapshot, thing_manager, StorageCounters::DISABLED);
    for player in all_players {
        let (role_player, count) = player?;
        for _ in 0..count {
            item_players.entry(role_player.role_type()).or_default().push(encode_role_player(&role_player.player()));
            checksums.role_count += 1;
        }
    }

    Ok(item_players
        .into_iter()
        .map(|(role_type, players)| encode_role(snapshot, type_manager, role_type, players))
        .try_collect()?)
}

fn encode_role(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    role_type: RoleType,
    players: Vec<item::relation::role::Player>,
) -> Result<item::relation::Role, Box<ConceptReadError>> {
    let label = encode_type_label(snapshot, type_manager, role_type)?;
    Ok(item::relation::Role { label, players })
}

fn encode_role_player(object: &impl ObjectAPI) -> item::relation::role::Player {
    item::relation::role::Player { id: encode_thing_iid(object) }
}

fn encode_owned_attribute(attribute: &Attribute) -> item::OwnedAttribute {
    item::OwnedAttribute { id: encode_thing_iid(attribute) }
}

fn encode_migration_value(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    attribute: &Attribute,
) -> Result<MigrationValue, Box<ConceptReadError>> {
    use migration::migration_value::Value as ValueProto;
    let value_message = match attribute.get_value(snapshot, thing_manager, StorageCounters::DISABLED)? {
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
    Ok(MigrationValue { value: Some(value_message) })
}

pub(crate) fn decode_migration_value(value_proto: MigrationValue) -> Result<Value<'static>, Box<ConceptDecodeError>> {
    use migration::migration_value::Value as ValueProto;
    let value_proto = value_proto.value.ok_or_else(|| Box::new(ConceptDecodeError::NoValue {}))?;
    let value = match value_proto {
        ValueProto::Boolean(boolean) => Value::Boolean(boolean),
        ValueProto::Integer(integer) => Value::Integer(integer),
        ValueProto::Double(double) => Value::Double(double),
        ValueProto::Decimal(decimal) => Value::Decimal(decode_decimal(decimal)?),
        ValueProto::Date(date) => Value::Date(decode_date(date)?),
        ValueProto::Datetime(date_time) => Value::Datetime(decode_datetime(date_time)?),
        ValueProto::DatetimeTz(datetime_tz) => Value::DatetimeTz(decode_datetime_tz(datetime_tz)?),
        ValueProto::DatetimeMillis(millis) => Value::Datetime(decode_datetime_from_millis(millis)?),
        ValueProto::Duration(duration) => Value::Duration(decode_duration(duration)?),
        ValueProto::String(string) => Value::String(Cow::Owned(string)),
        ValueProto::Struct(_struct) => unimplemented_feature!(Structs),
    };
    Ok(value)
}

pub(crate) fn decode_checksums(checksums_proto: item::Checksums) -> Checksums {
    Checksums {
        entity_count: checksums_proto.entity_count,
        attribute_count: checksums_proto.attribute_count,
        relation_count: checksums_proto.relation_count,
        role_count: checksums_proto.role_count,
        ownership_count: checksums_proto.ownership_count,
    }
}
