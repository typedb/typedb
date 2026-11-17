/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use bytes::util::Base64Formatter;
use concept::{
    error::ConceptReadError,
    thing::{
        ThingAPI, ThingKind,
        attribute::Attribute,
        entity::Entity,
        has::Has,
        object::{Object, ObjectAPI},
        relation::{Links, Relation},
        thing_manager::ThingManager,
    },
    type_::{TypeAPI, role_type::RoleType, type_manager::TypeManager},
};
use encoding::value::{label::Label, value::Value};
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::migration::{Checksums, MigrationOperationError};

#[derive(Debug)]
pub enum MigrationItem {
    Header { typedb_version: String, original_database: String, guarantees: Guarantees },
    Entity(EntityItem),
    Relation(RelationItem),
    Attribute(AttributeItem),
    Checksums(Checksums),
}

#[derive(Debug, Clone)]
pub struct EntityItem {
    pub globally_unique_id: String,
    pub label: Arc<Label>,
    pub label_scoped_id: Option<u64>,
    pub attributes: Vec<OwnedAttribute>,
}

#[derive(Debug, Clone)]
pub struct RelationItem {
    pub globally_unique_id: String,
    pub label: Arc<Label>,
    pub label_scoped_id: Option<u64>,
    pub attributes: Vec<OwnedAttribute>,
    pub roles: Vec<Role>,
}

#[derive(Debug, Clone)]
pub struct AttributeItem {
    pub globally_unique_id: String,
    pub label: Arc<Label>,
    pub value: Value<'static>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum ItemOrder {
    #[default]
    Unspecified,
    AttributesEntitiesRelations,
}

impl Display for ItemOrder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct Guarantees {
    pub item_order: ItemOrder,
    pub attributes_unique: bool,
    pub label_scoped_ids_present: bool,
    pub owned_inlined_attributes_referenced_by_value: bool,
}

#[derive(Debug, Clone)]
pub struct OwnedAttribute {
    pub globally_unique_id: String,
    pub attribute_by_value: Option<AttributeReferencedByValue>,
}

impl OwnedAttribute {
    pub fn by_id(globally_unique_id: String) -> Self {
        Self { globally_unique_id, attribute_by_value: None }
    }
}

#[derive(Debug, Clone)]
pub struct AttributeReferencedByValue {
    pub label: Arc<Label>,
    pub value: Value<'static>,
}

#[derive(Debug, Clone)]
pub struct Role {
    pub label: Arc<Label>,
    pub players: Vec<Player>,
}

#[derive(Debug, Clone)]
pub struct Player {
    pub globally_unique_id: String,
    pub label_scoped_id: Option<LabelScopedId>,
}

impl Player {
    pub fn by_id(globally_unique_id: String) -> Self {
        Self { globally_unique_id, label_scoped_id: None }
    }
}

#[derive(Debug, Clone)]
pub struct LabelScopedId {
    pub label: Arc<Label>,
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
        globally_unique_id: encode_thing_iid(&entity),
        label: encode_type_label(snapshot, type_manager, entity.type_())?,
        label_scoped_id: Some(encode_label_scoped_id(&entity)),
        attributes: encode_owned_attributes(snapshot, type_manager, thing_manager, checksums, entity)?,
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
        globally_unique_id: encode_thing_iid(&relation),
        label: encode_type_label(snapshot, type_manager, relation.type_())?,
        label_scoped_id: Some(encode_label_scoped_id(&relation)),
        attributes: encode_owned_attributes(snapshot, type_manager, thing_manager, checksums, relation)?,
        roles: encode_relation_roles(snapshot, type_manager, thing_manager, checksums, relation)?,
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
        globally_unique_id: encode_thing_iid(&attribute),
        label: encode_type_label(snapshot, type_manager, attribute.type_())?,
        value,
    }))
}

fn encode_thing_iid(thing: &impl ThingAPI) -> String {
    Base64Formatter::borrowed(thing.iid().borrow()).format()
}

fn encode_label_scoped_id(object: &impl ObjectAPI) -> u64 {
    object.vertex().object_id().as_u64()
}

fn encode_type_label(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: impl TypeAPI,
) -> Result<Arc<Label>, Box<ConceptReadError>> {
    Ok(Arc::new(type_.get_label(snapshot, type_manager)?.clone()))
}

fn encode_owned_attributes(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
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
            owned_attributes.push(encode_owned_attribute(snapshot, type_manager, thing_manager, has.attribute())?);
            checksums.increment_ownership_count();
        }
    }
    Ok(owned_attributes)
}

fn encode_owned_attribute(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    attribute: Attribute,
) -> Result<OwnedAttribute, Box<ConceptReadError>> {
    let globally_unique_id = encode_thing_iid(&attribute);
    let attribute_by_value = if attribute.vertex().attribute_id().is_inline() {
        Some(AttributeReferencedByValue {
            label: encode_type_label(snapshot, type_manager, attribute.type_())?,
            value: attribute.get_value(snapshot, thing_manager, StorageCounters::DISABLED)?.into_owned(),
        })
    } else {
        None
    };
    Ok(OwnedAttribute { globally_unique_id, attribute_by_value })
}

fn encode_relation_roles(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    checksums: &mut Checksums,
    relation: Relation,
) -> Result<Vec<Role>, Box<ConceptReadError>> {
    // TODO: Cover role players ordering
    let mut players_by_role: BTreeMap<RoleType, Vec<Player>> = BTreeMap::new();
    let all_players = relation.get_players(snapshot, thing_manager, StorageCounters::DISABLED);
    for player in all_players {
        let (role_player, count) = player?;
        for _ in 0..count {
            players_by_role.entry(role_player.role_type()).or_default().push(encode_player(
                snapshot,
                type_manager,
                role_player.player(),
            )?);
            checksums.increment_role_count();
        }
    }

    let mut roles = Vec::with_capacity(players_by_role.len());
    for (role_type, players) in players_by_role {
        roles.push(Role { label: encode_type_label(snapshot, type_manager, role_type)?, players });
    }
    Ok(roles)
}

fn encode_player(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    player: Object,
) -> Result<Player, Box<ConceptReadError>> {
    Ok(Player {
        globally_unique_id: encode_thing_iid(&player),
        label_scoped_id: Some(LabelScopedId {
            label: encode_type_label(snapshot, type_manager, player.type_())?,
            label_scoped_id: encode_label_scoped_id(&player),
        }),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemKind {
    Header,
    Checksums,
    Entity,
    Relation,
    Attribute,
}

impl ItemKind {
    // instances are imported by the workers, and the other items by the importer itself
    pub(crate) fn thing_kind(&self) -> Option<ThingKind> {
        match self {
            Self::Header | Self::Checksums => None,
            Self::Entity => Some(ThingKind::Entity),
            Self::Relation => Some(ThingKind::Relation),
            Self::Attribute => Some(ThingKind::Attribute),
        }
    }
}

// decoded labels by their wire string: an import names only the schema's few types and roles,
// so each label is parsed once and then shared, instead of once per item, role and player
#[derive(Debug, Default)]
pub struct LabelCache {
    labels: HashMap<String, Arc<Label>>,
}

impl LabelCache {
    pub fn get(&mut self, label: String) -> Arc<Label> {
        if let Some(cached) = self.labels.get(&label) {
            return cached.clone();
        }
        let parsed = Arc::new(Label::parse_from(&label, None));
        self.labels.insert(label, parsed.clone());
        parsed
    }
}

pub type ItemDecoder<T> = fn(T, &mut LabelCache) -> Result<MigrationItem, MigrationOperationError>;

// an item as it was received, routed by its kind and decoded by the worker that writes it,
// so that decoding is spread across the workers
pub struct DecodableItem<T: 'static> {
    source: T,
    kind: ItemKind,
    estimated_write_bytes: usize,
    decoder: ItemDecoder<T>,
}

impl<T: 'static> DecodableItem<T> {
    pub fn new(source: T, kind: ItemKind, estimated_write_bytes: usize, decoder: ItemDecoder<T>) -> Self {
        Self { source, kind, estimated_write_bytes, decoder }
    }

    pub(crate) fn kind(&self) -> ItemKind {
        self.kind
    }

    pub(crate) fn estimated_write_bytes(&self) -> usize {
        self.estimated_write_bytes
    }

    pub(crate) fn decode(self, labels: &mut LabelCache) -> Result<MigrationItem, MigrationOperationError> {
        (self.decoder)(self.source, labels)
    }
}

impl DecodableItem<MigrationItem> {
    pub fn decoded(item: MigrationItem) -> Self {
        let (kind, estimated_write_bytes) = match &item {
            MigrationItem::Header { .. } => (ItemKind::Header, 0),
            MigrationItem::Checksums(_) => (ItemKind::Checksums, 0),
            MigrationItem::Entity(EntityItem { attributes, .. }) => {
                (ItemKind::Entity, estimated_object_write_bytes(attributes.len(), 0))
            }
            MigrationItem::Relation(RelationItem { attributes, roles, .. }) => {
                let players = roles.iter().map(|role| role.players.len()).sum();
                (ItemKind::Relation, estimated_object_write_bytes(attributes.len(), players))
            }
            MigrationItem::Attribute(AttributeItem { value, .. }) => {
                (ItemKind::Attribute, Attribute::estimated_write_bytes(value))
            }
        };
        Self::new(item, kind, estimated_write_bytes, |item, _| Ok(item))
    }
}

// items are batched by the bytes they write before they are decoded, so this estimates it from their contents
pub fn estimated_object_write_bytes(owned_attributes: usize, players: usize) -> usize {
    Object::ESTIMATED_WRITE_BYTES
        + owned_attributes * Has::ESTIMATED_WRITE_BYTES
        + players * Links::ESTIMATED_WRITE_BYTES
}
