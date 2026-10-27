/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Borrow,
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
};

use bytes::util::Base64Formatter;
use concept::{
    error::ConceptReadError,
    thing::{
        ThingAPI, attribute::Attribute, entity::Entity, object::ObjectAPI, relation::Relation,
        thing_manager::ThingManager,
    },
    type_::{TypeAPI, role_type::RoleType, type_manager::TypeManager},
};
use encoding::value::{label::Label, value::Value};
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::migration::Checksums;

#[derive(Debug)]
pub enum MigrationItem {
    Header { typedb_version: String, original_database: String, guarantees: Guarantees },
    Entity(EntityItem),
    Relation(RelationItem),
    Attribute(AttributeItem),
    Checksums(Checksums),
}

#[derive(Debug)]
pub struct EntityItem {
    pub id: String,
    pub label: Label,
    pub label_scoped_id: Option<u64>,
    pub owned_attributes: Vec<OwnedAttribute>,
}

#[derive(Debug)]
pub struct RelationItem {
    pub id: String,
    pub label: Label,
    pub label_scoped_id: Option<u64>,
    pub owned_attributes: Vec<OwnedAttribute>,
    pub related_role_players: Vec<RolePlayers>,
}

#[derive(Debug)]
pub struct AttributeItem {
    pub id: String,
    pub label: Label,
    pub value: Value<'static>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum ItemOrder {
    #[default]
    Unspecified,
    AttributeEntityRelation,
}

impl Display for ItemOrder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct Guarantees {
    pub ordering: ItemOrder,
    pub attributes_unique: bool,
    pub players_by_label_scoped_ids: bool,
    pub owned_attributes_by_value: bool,
}

#[derive(Debug)]
pub struct OwnedAttribute {
    pub id: String,
    pub attribute_by_value: Option<AttributeByValue>,
}

impl OwnedAttribute {
    pub fn by_id(id: String) -> Self {
        Self { id, attribute_by_value: None }
    }
}

#[derive(Debug)]
pub struct AttributeByValue {
    pub label: Label,
    pub value: Value<'static>,
}

#[derive(Debug)]
pub struct RolePlayers {
    pub role: Label,
    pub players: Vec<Player>,
}

#[derive(Debug)]
pub struct Player {
    pub id: String,
    pub player_by_label_scoped_id: Option<PlayerByLabelScopedId>,
}

impl Player {
    pub fn by_id(id: String) -> Self {
        Self { id, player_by_label_scoped_id: None }
    }
}

#[derive(Debug)]
pub struct PlayerByLabelScopedId {
    pub label: Label,
    pub label_scoped_id: u64,
}

pub(crate) fn encode_entity(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    checksums: &mut Checksums,
    entity: Entity,
) -> Result<MigrationItem, Box<ConceptReadError>> {
    Ok(MigrationItem::Entity(EntityItem {
        id: encode_thing_iid(&entity),
        label: encode_type_label(snapshot, type_manager, entity.type_())?,
        label_scoped_id: None,
        owned_attributes: encode_owned_attributes(snapshot, thing_manager, checksums, entity)?,
    }))
}

pub(crate) fn encode_relation(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    checksums: &mut Checksums,
    relation: Relation,
) -> Result<MigrationItem, Box<ConceptReadError>> {
    Ok(MigrationItem::Relation(RelationItem {
        id: encode_thing_iid(&relation),
        label: encode_type_label(snapshot, type_manager, relation.type_())?,
        label_scoped_id: None,
        owned_attributes: encode_owned_attributes(snapshot, thing_manager, checksums, relation)?,
        related_role_players: encode_relation_roles(snapshot, type_manager, thing_manager, checksums, relation)?,
    }))
}

pub(crate) fn encode_attribute(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    attribute: Attribute,
) -> Result<MigrationItem, Box<ConceptReadError>> {
    let value = attribute.get_value(snapshot, thing_manager, StorageCounters::DISABLED)?.into_owned();
    Ok(MigrationItem::Attribute(AttributeItem {
        id: encode_thing_iid(&attribute),
        label: encode_type_label(snapshot, type_manager, attribute.type_())?,
        value,
    }))
}

fn encode_thing_iid(thing: &impl ThingAPI) -> String {
    Base64Formatter::borrowed(thing.iid().borrow()).format()
}

fn encode_type_label(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: impl TypeAPI,
) -> Result<Label, Box<ConceptReadError>> {
    Ok(type_.get_label(snapshot, type_manager)?.clone())
}

fn encode_owned_attributes(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    checksums: &mut Checksums,
    object: impl ObjectAPI,
) -> Result<Vec<OwnedAttribute>, Box<ConceptReadError>> {
    let mut owned_attributes = Vec::new();
    // TODO: Cover has ordering
    let all_has = object.get_has_unordered(snapshot, thing_manager, StorageCounters::DISABLED)?;
    for has in all_has {
        let (has, count) = has?;
        for _ in 0..count {
            owned_attributes.push(OwnedAttribute::by_id(encode_thing_iid(&has.attribute())));
            checksums.increment_ownership_count();
        }
    }
    Ok(owned_attributes)
}

fn encode_relation_roles(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    checksums: &mut Checksums,
    relation: Relation,
) -> Result<Vec<RolePlayers>, Box<ConceptReadError>> {
    // TODO: Cover role players ordering
    let mut players_by_role: BTreeMap<RoleType, Vec<Player>> = BTreeMap::new();
    let all_players = relation.get_players(snapshot, thing_manager, StorageCounters::DISABLED);
    for player in all_players {
        let (role_player, count) = player?;
        for _ in 0..count {
            players_by_role
                .entry(role_player.role_type())
                .or_default()
                .push(Player::by_id(encode_thing_iid(&role_player.player())));
            checksums.increment_role_count();
        }
    }

    let mut roles = Vec::with_capacity(players_by_role.len());
    for (role_type, players) in players_by_role {
        roles.push(RolePlayers { role: encode_type_label(snapshot, type_manager, role_type)?, players });
    }
    Ok(roles)
}
