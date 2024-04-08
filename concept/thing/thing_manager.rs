/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::borrow::Cow;
use std::rc::Rc;
use std::sync::Mutex;

use bytes::Bytes;
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeHasReverse, ThingEdgeRelationIndex, ThingEdgeRolePlayer},
            vertex_attribute::AttributeVertex,
            vertex_generator::{LongAttributeID, StringAttributeID, ThingVertexGenerator},
            vertex_object::ObjectVertex,
        },
        type_::vertex::TypeVertex,
        Typed,
    },
    layout::prefix::{Prefix, PrefixID},
    value::{
        decode_value_u64, encode_value_u64,
        long::Long, string::StringBytes, value_type::ValueType,
    },
    Keyable,
};
use primitive::prefix_range::PrefixRange;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::snapshot::{ReadableSnapshot,  WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        attribute::{Attribute, AttributeIterator},
        entity::{Entity, EntityIterator},
        relation::{IndexedPlayersIterator, Relation, RelationIterator, RelationRoleIterator, RolePlayerIterator},
        value::Value,
        object::Object,
    },
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, type_manager::TypeManager,
        relation_type::RelationType,
        role_type::RoleType,
        TypeAPI,
    },
};
use crate::thing::ObjectAPI;

pub struct ThingManager<'txn, Snapshot> {
    snapshot: Rc<Snapshot>,
    vertex_generator: &'txn ThingVertexGenerator,
    type_manager: Rc<TypeManager<'txn, Snapshot>>,
    relation_lock: Mutex<()>,
}

impl<'txn, Snapshot: ReadableSnapshot> ThingManager<'txn, Snapshot> {
    pub fn new(
        snapshot: Rc<Snapshot>,
        vertex_generator: &'txn ThingVertexGenerator,
        type_manager: Rc<TypeManager<'txn, Snapshot>>,
    ) -> Self {
        ThingManager { snapshot, vertex_generator, type_manager, relation_lock: Mutex::new(()) }
    }

    pub(crate) fn type_manager(&self) -> &TypeManager<'txn, Snapshot> {
        self.type_manager.as_ref()
    }

    pub fn get_entities(&self) -> EntityIterator<'_, 1> {
        let prefix = ObjectVertex::build_prefix_prefix(Prefix::VertexEntity.prefix_id());
        let snapshot_iterator = self.snapshot.iterate_range(PrefixRange::new_within(prefix));
        EntityIterator::new(snapshot_iterator)
    }

    pub fn get_relations(&self) -> RelationIterator<'_, 1> {
        let prefix = ObjectVertex::build_prefix_prefix(Prefix::VertexRelation.prefix_id());
        let snapshot_iterator = self.snapshot.iterate_range(PrefixRange::new_within(prefix));
        RelationIterator::new(snapshot_iterator)
    }

    pub fn get_attributes(&self) -> AttributeIterator<'_, 1> {
        let start = AttributeVertex::build_prefix_prefix(PrefixID::VERTEX_ATTRIBUTE_MIN);
        let end = AttributeVertex::build_prefix_prefix(PrefixID::VERTEX_ATTRIBUTE_MAX);
        let snapshot_iterator = self.snapshot.iterate_range(PrefixRange::new_inclusive(start, end));
        AttributeIterator::new(snapshot_iterator)
    }

    pub fn get_attributes_in(
        &self,
        attribute_type: AttributeType<'_>,
    ) -> Result<AttributeIterator<'_, 3>, ConceptReadError> {
        Ok(attribute_type
            .get_value_type(self.type_manager.as_ref())?
            .map(|value_type| {
                let prefix = AttributeVertex::build_prefix_type(value_type, attribute_type.vertex().type_id_());
                let snapshot_iterator = self.snapshot.iterate_range(PrefixRange::new_within(prefix));
                AttributeIterator::new(snapshot_iterator)
            })
            .unwrap_or_else(AttributeIterator::new_empty))
    }

    pub(crate) fn get_attribute_value(&self, attribute: &Attribute<'_>) -> Result<Value<'static>, ConceptReadError> {
        match attribute.value_type() {
            ValueType::Boolean => {
                todo!()
            }
            ValueType::Long => {
                let attribute_id = LongAttributeID::new(attribute.vertex().attribute_id().unwrap_bytes_8());
                Ok(Value::Long(Long::new(attribute_id.bytes()).as_i64()))
            }
            ValueType::Double => {
                todo!()
            }
            ValueType::String => {
                let attribute_id = StringAttributeID::new(attribute.vertex().attribute_id().unwrap_bytes_17());
                if attribute_id.is_inline() {
                    Ok(Value::String(Cow::Owned(
                        String::from(attribute_id.get_inline_string_bytes().as_str()).into_boxed_str()
                    )))
                } else {
                    Ok(self
                        .snapshot
                        .get_mapped(attribute.vertex().as_storage_key().as_reference(), |bytes| {
                            Value::String(Cow::Owned(
                                String::from(StringBytes::new(Bytes::<1>::Reference(bytes)).as_str()).into_boxed_str(),
                            ))
                        })
                        .map_err(|error| ConceptReadError::SnapshotGet { source: error })?
                        .unwrap())
                }
            }
        }
    }

    pub(crate) fn get_indexed_players(
        &self, from: Object<'_>,
    ) -> IndexedPlayersIterator<'_, { ThingEdgeRelationIndex::LENGTH_PREFIX_FROM }> {
        let prefix = ThingEdgeRelationIndex::prefix_from(from.vertex());
        IndexedPlayersIterator::new(self.snapshot.iterate_range(PrefixRange::new_within(prefix)))
    }

    // --- storage operations ---
    pub(crate) fn storage_get_has<'this, 'a>(
        &'this self,
        owner: impl ObjectAPI<'a>,
    ) -> AttributeIterator<'this, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        let prefix = ThingEdgeHas::prefix_from_object(owner.into_vertex());
        AttributeIterator::new(self.snapshot.iterate_range(PrefixRange::new_within(prefix)))
    }

    pub(crate) fn storage_get_relations<'this, 'a>(
        &'this self,
        player: impl ObjectAPI<'a>,
    ) -> RelationRoleIterator<'this, { ThingEdgeRolePlayer::LENGTH_PREFIX_FROM }> {
        let prefix = ThingEdgeRolePlayer::prefix_reverse_from_player(player.into_vertex());
        RelationRoleIterator::new(self.snapshot.iterate_range(PrefixRange::new_within(prefix)))
    }

    pub(crate) fn storage_get_role_players<'this, 'a>(
        &'this self, relation: impl ObjectAPI<'a>,
    ) -> RolePlayerIterator<'txn, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> where 'this: 'txn {
        let prefix = ThingEdgeRolePlayer::prefix_from_relation(relation.into_vertex());
        RolePlayerIterator::new(self.snapshot.iterate_range(PrefixRange::new_within(prefix)))
    }
}

impl<'txn, Snapshot: WritableSnapshot> ThingManager<'txn, Snapshot> {
    pub fn create_entity(&self, entity_type: EntityType<'static>) -> Result<Entity<'_>, ConceptWriteError> {
        Ok(Entity::new(self.vertex_generator.create_entity(entity_type.vertex().type_id_(), self.snapshot.as_ref())))
    }

    pub fn create_relation(&self, relation_type: RelationType<'static>) -> Result<Relation<'_>, ConceptWriteError> {
        Ok(Relation::new(self.vertex_generator.create_relation(relation_type.vertex().type_id_(), self.snapshot.as_ref())))
    }

    pub fn create_attribute(
        &self,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<Attribute<'_>, ConceptWriteError> {
        let value_type = attribute_type.get_value_type(self.type_manager.as_ref())?;
        if Some(value.value_type()) == value_type {
            let vertex = match value {
                Value::Boolean(_bool) => {
                    todo!()
                }
                Value::Long(long) => {
                    let encoded_long = Long::build(long);
                    self.vertex_generator.create_attribute_long(
                       attribute_type.vertex().type_id_(),
                        encoded_long,
                        self.snapshot.as_ref(),
                    )
                }
                Value::Double(_double) => {
                    todo!()
                }
                Value::String(string) => {
                    let encoded_string: StringBytes<'_, BUFFER_KEY_INLINE> = StringBytes::build_ref(&string);
                    self.vertex_generator.create_attribute_string(
                       attribute_type.vertex().type_id_(),
                        encoded_string,
                        self.snapshot.as_ref(),
                    )
                }
            };
            Ok(Attribute::new(vertex))
        } else {
            Err(ConceptWriteError::ValueTypeMismatch { expected: value_type, provided: value.value_type() })
        }
    }

    // --- storage operations ---
    pub(crate) fn storage_set_has<'a>(&self, owner: impl ObjectAPI<'a>, attribute: Attribute<'_>) {
        let has = ThingEdgeHas::build(owner.vertex(), attribute.vertex());
        self.snapshot.as_ref().put(has.into_storage_key().into_owned_array());
        let has_reverse = ThingEdgeHasReverse::build(attribute.into_vertex(), owner.into_vertex());
        self.snapshot.as_ref().put(has_reverse.into_storage_key().into_owned_array());
    }

    pub fn storage_set_role_player<'a>(&self, relation: Relation<'_>, player: impl ObjectAPI<'a>, role_type: RoleType<'_>) {
        let role_player = ThingEdgeRolePlayer::build_role_player(
            relation.vertex(), player.vertex(), role_type.clone().into_vertex(),
        );
        let count: u64 = 1;
        self.snapshot.as_ref().put_val(role_player.into_storage_key().into_owned_array(), encode_value_u64(count));
        let role_player_reverse = ThingEdgeRolePlayer::build_role_player_reverse(
            player.into_vertex(), relation.into_vertex(), role_type.into_vertex(),
        );
        // must be idempotent, so no lock required -- cannot fail
        self.snapshot.as_ref().put_val(role_player_reverse.into_storage_key().into_owned_array(), encode_value_u64(count));
    }

    pub fn storage_increment_role_player<'a>(&self,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType<'_>,
    ) -> u64 {
        let role_player = ThingEdgeRolePlayer::build_role_player(
            relation.vertex(), player.vertex(), role_type.clone().into_vertex(),
        );
        let role_player_reverse = ThingEdgeRolePlayer::build_role_player_reverse(
            player.vertex(), relation.vertex(), role_type.into_vertex(),
        );

        let mut count = 0;
        {
            let lock = self.relation_lock.lock().unwrap();
            let rp_count = self.snapshot.as_ref()
                .get_mapped(role_player.as_storage_key().as_reference(), |val| {
                    decode_value_u64(val)
                }).unwrap();
            let rp_reverse_count = self.snapshot.as_ref()
                .get_mapped(role_player_reverse.as_storage_key().as_reference(), |val| {
                decode_value_u64(val)
            }).unwrap();
            debug_assert_eq!(&rp_count, &rp_reverse_count);

            count = rp_count.unwrap_or(0) + 1;
            let reverse_count = rp_reverse_count.unwrap_or(0) + 1;
            self.snapshot.as_ref().put_val(role_player.as_storage_key().into_owned_array(), encode_value_u64(count));
            self.snapshot.as_ref()
                .put_val(role_player_reverse.as_storage_key().into_owned_array(), encode_value_u64(reverse_count));
        }

        // must lock to fail concurrent transactions updating the same counter
        self.snapshot.as_ref().record_lock(role_player.into_storage_key());
        count
    }

    pub fn relation_index_new_player(
        &self,
        relation: Relation<'_>,
        player: Object<'_>,
        role_type: RoleType<'_>,
        duplicates_allowed: bool,
        player_count: u64,
    ) {
        let _lock = self.relation_lock.lock().unwrap();
        let mut players = relation.get_players(self);
        if !duplicates_allowed {
            let encoded_count = encode_value_u64(1);
            let mut role_player = players.next().transpose().unwrap();
            while let Some((rp, count)) = role_player.as_ref() {
                debug_assert_eq!(*count, 1);
                if rp.player() != player {
                    let index = ThingEdgeRelationIndex::build(
                        player.vertex(),
                        rp.player().vertex(),
                        relation.vertex(),
                       role_type.vertex().type_id_(),
                       rp.role_type().vertex().type_id_(),
                    );
                    self.snapshot.as_ref().put_val(index.as_storage_key().into_owned_array(), encoded_count.clone());
                    let index_reverse = ThingEdgeRelationIndex::build(
                        rp.player().vertex(),
                        player.vertex(),
                        relation.vertex(),
                       rp.role_type().vertex().type_id_(),
                       role_type.vertex().type_id_(),
                    );
                    self.snapshot.as_ref()
                        .put_val(index_reverse.as_storage_key().into_owned_array(), encoded_count.clone());
                }
                role_player = players.next().transpose().unwrap();
            }
        } else {
            /// for duplicate players, if we have the same (role: player) entry N times,
            /// we should end up with a repetition of N-1
            ///
            /// for different players, if we have N (role: player) and M (role2: player2)
            /// both directions of the index should have N*M
            let mut role_player = players.next().transpose().unwrap();
            while let Some((rp, count)) = role_player.as_ref() {
                debug_assert_eq!(*count, 1);
                if rp.player() == player && rp.role_type() == role_type && *count < player_count {
                    // only update index if another writer hasn't already incremented the player count and it will handle the index update
                    let repetitions = player_count - 1;
                    let index = ThingEdgeRelationIndex::build(
                        player.vertex(),
                        player.vertex(),
                        relation.vertex(),
                       role_type.vertex().type_id_(),
                       role_type.vertex().type_id_(),
                    );
                    self.snapshot.as_ref()
                        .put_val(index.as_storage_key().into_owned_array(), encode_value_u64(repetitions));
                } else {
                    let repetitions = player_count * count;
                    let index = ThingEdgeRelationIndex::build(
                        player.vertex(),
                        rp.player().vertex(),
                        relation.vertex(),
                       role_type.vertex().type_id_(),
                       rp.role_type().vertex().type_id_(),
                    );
                    self.snapshot.as_ref()
                        .put_val(index.as_storage_key().into_owned_array(), encode_value_u64(repetitions));
                    let index_reverse = ThingEdgeRelationIndex::build(
                        rp.player().vertex(),
                        player.vertex(),
                        relation.vertex(),
                        rp.role_type().vertex().type_id_(),
                       role_type.vertex().type_id_(),
                    );
                    self.snapshot.as_ref()
                        .put_val(index_reverse.as_storage_key().into_owned_array(), encode_value_u64(repetitions));
                }
                role_player = players.next().transpose().unwrap();
            }
        }
    }
}
