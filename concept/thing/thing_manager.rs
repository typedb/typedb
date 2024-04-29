/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::HashSet,
    sync::{Arc, Mutex, MutexGuard},
};

use bytes::{byte_array::ByteArray, Bytes};
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeHasReverse, ThingEdgeRelationIndex, ThingEdgeRolePlayer},
            property::HAS_ORDER_PROPERTY_FACTORY,
            vertex_attribute::{AttributeID, AttributeVertex},
            vertex_generator::ThingVertexGenerator,
            vertex_object::ObjectVertex,
        },
        Typed,
    },
    layout::prefix::Prefix,
    value::{
        decode_value_u64, encode_value_u64, long_bytes::LongBytes, string_bytes::StringBytes, value_type::ValueType,
        ValueEncodable,
    },
    Keyable,
};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_range::KeyRange,
    key_value::StorageKey,
    snapshot::{write::Write, ReadableSnapshot, WritableSnapshot},
};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        attribute::{Attribute, AttributeIterator, AttributeOwnerIterator},
        decode_attribute_ids, encode_attribute_ids,
        entity::{Entity, EntityIterator},
        object::{HasAttributeIterator, Object},
        relation::{IndexedPlayersIterator, Relation, RelationIterator, RelationRoleIterator, RolePlayerIterator},
        value::Value,
        ObjectAPI, ThingAPI,
    },
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, relation_type::RelationType, role_type::RoleType,
        type_manager::TypeManager, TypeAPI,
    },
    ConceptStatus,
};

pub struct ThingManager<Snapshot> {
    vertex_generator: Arc<ThingVertexGenerator>,
    type_manager: Arc<TypeManager<Snapshot>>,
    relation_lock: Mutex<()>,
    snapshot: PhantomData<Snapshot>,
}

impl<Snapshot: ReadableSnapshot> ThingManager<Snapshot> {
    pub fn new(vertex_generator: Arc<ThingVertexGenerator>, type_manager: Arc<TypeManager<Snapshot>>) -> Self {
        ThingManager { vertex_generator, type_manager, relation_lock: Mutex::new(()), snapshot: PhantomData::default() }
    }

    pub(crate) fn type_manager(&self) -> &TypeManager<Snapshot> {
        &self.type_manager
    }

    pub fn get_entities<'this>(&'this self, snapshot: &'this Snapshot) -> EntityIterator<'_, 1> {
        let prefix = ObjectVertex::build_prefix_prefix(Prefix::VertexEntity.prefix_id());
        let snapshot_iterator =
            snapshot.iterate_range(KeyRange::new_within(prefix, Prefix::VertexEntity.fixed_width_keys()));
        EntityIterator::new(snapshot_iterator)
    }

    pub fn get_relations<'this>(&'this self, snapshot: &'this Snapshot) -> RelationIterator<'_, 1> {
        let prefix = ObjectVertex::build_prefix_prefix(Prefix::VertexRelation.prefix_id());
        let snapshot_iterator =
            snapshot.iterate_range(KeyRange::new_within(prefix, Prefix::VertexRelation.fixed_width_keys()));
        RelationIterator::new(snapshot_iterator)
    }

    pub fn get_attributes<'this>(&'this self, snapshot: &'this Snapshot) -> AttributeIterator<'_, Snapshot, 1, 2> {
        let start = AttributeVertex::build_prefix_prefix(Prefix::ATTRIBUTE_MIN);
        let end = AttributeVertex::build_prefix_prefix(Prefix::ATTRIBUTE_MAX);
        let attribute_iterator = snapshot.iterate_range(KeyRange::new_inclusive(start, end));

        let has_reverse_start = ThingEdgeHasReverse::prefix_from_prefix(Prefix::ATTRIBUTE_MIN);
        let has_reverse_end = ThingEdgeHasReverse::prefix_from_prefix(Prefix::ATTRIBUTE_MAX);
        let has_reverse_iterator =
            snapshot.iterate_range(KeyRange::new_inclusive(has_reverse_start, has_reverse_end));
        AttributeIterator::new(attribute_iterator, has_reverse_iterator, snapshot, self.type_manager())
    }

    pub fn get_attributes_in<'this>(
        &'this self,
        snapshot: &'this Snapshot,
        attribute_type: AttributeType<'_>,
    ) -> Result<AttributeIterator<'_, Snapshot, 3, 4>, ConceptReadError> {
        Ok(attribute_type
            .get_value_type(snapshot, self.type_manager.as_ref())?
            .map(|value_type| {
                let attribute_value_type_prefix = AttributeVertex::value_type_to_prefix_type(value_type);
                let prefix =
                    AttributeVertex::build_prefix_type(attribute_value_type_prefix, attribute_type.vertex().type_id_());
                let attribute_iterator = snapshot
                    .iterate_range(KeyRange::new_within(prefix, attribute_value_type_prefix.fixed_width_keys()));

                let has_reverse_prefix = ThingEdgeHasReverse::prefix_from_type(
                    attribute_value_type_prefix,
                    attribute_type.vertex().type_id_(),
                );
                let has_reverse_iterator = snapshot
                    .iterate_range(KeyRange::new_within(has_reverse_prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING));
                AttributeIterator::new(attribute_iterator, has_reverse_iterator, snapshot, self.type_manager())
            })
            .unwrap_or_else(AttributeIterator::new_empty))
    }

    pub(crate) fn get_attribute_value(
        &self,
        snapshot: &Snapshot,
        attribute: &Attribute<'_>,
    ) -> Result<Value<'static>, ConceptReadError> {
        match attribute.value_type() {
            ValueType::Boolean => {
                todo!()
            }
            ValueType::Long => {
                let attribute_id = attribute.vertex().attribute_id().unwrap_long();
                Ok(Value::Long(LongBytes::new(attribute_id.bytes()).as_i64()))
            }
            ValueType::Double => {
                todo!()
            }
            ValueType::String => {
                let attribute_id = attribute.vertex().attribute_id().unwrap_string();
                if attribute_id.is_inline() {
                    Ok(Value::String(Cow::Owned(String::from(attribute_id.get_inline_string_bytes().as_str()))))
                } else {
                    Ok(snapshot
                        .get_mapped(attribute.vertex().as_storage_key().as_reference(), |bytes| {
                            Value::String(Cow::Owned(String::from(
                                StringBytes::new(Bytes::<1>::Reference(bytes)).as_str(),
                            )))
                        })
                        .map_err(|error| ConceptReadError::SnapshotGet { source: error })?
                        .unwrap())
                }
            }
        }
    }

    pub(crate) fn get_attribute_with_value(
        &self,
        snapshot: &Snapshot,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<Option<Attribute<'static>>, ConceptReadError> {
        match value.value_type() {
            ValueType::Boolean => todo!(),
            ValueType::Long => {
                debug_assert!(AttributeID::is_inlineable(value.as_reference()));
                self.get_attribute_with_value_inline(snapshot, attribute_type, value)
            }
            ValueType::Double => todo!(),
            ValueType::String => {
                if AttributeID::is_inlineable(value.as_reference()) {
                    self.get_attribute_with_value_inline(snapshot, attribute_type, value)
                } else {
                    self.vertex_generator
                        .find_attribute_id_string_noinline(
                            attribute_type.vertex().type_id_(),
                            value.encode_string::<256>(),
                            snapshot,
                        )
                        .map_err(|err| ConceptReadError::SnapshotIterate { source: err })
                        .map(|id| {
                            id.map(|id| {
                                Attribute::new(AttributeVertex::new(Bytes::Array(ByteArray::copy(&id.bytes()))))
                            })
                        })
                }
            }
        }
    }

    fn get_attribute_with_value_inline(
        &self,
        snapshot: &Snapshot,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<Option<Attribute<'static>>, ConceptReadError> {
        debug_assert!(AttributeID::is_inlineable(value.as_reference()));
        let vertex = AttributeVertex::build(
            value.value_type(),
            attribute_type.vertex().type_id_(),
            AttributeID::build_inline(value),
        );
        snapshot
            .get_mapped(vertex.as_storage_key().as_reference(), |_| Attribute::new(vertex.clone()))
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })
    }

    pub(crate) fn has_attribute<'a>(
        &self,
        snapshot: &Snapshot,
        owner: impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<bool, ConceptReadError> {
        let value_type = value.value_type();
        let vertex = if AttributeID::is_inlineable(value.as_reference()) {
            // don't need to do an extra lookup to get the attribute vertex - if it exists, it will have this ID
            AttributeVertex::build(value_type, attribute_type.vertex().type_id_(), AttributeID::build_inline(value))
        } else {
            // non-inline attributes require an extra lookup before checking for the has edge existence
            let attribute = self.get_attribute_with_value(snapshot, attribute_type, value)?;
            match attribute {
                Some(attribute) => attribute.into_vertex(),
                None => return Ok(false),
            }
        };

        let has = ThingEdgeHas::build(owner.vertex(), vertex);
        let has_exists = snapshot
            .get_mapped(has.into_storage_key().as_reference(), |_value| true)
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?
            .unwrap_or(false);
        Ok(has_exists)
    }

    pub(crate) fn get_has_unordered<'this, 'a>(
        &'this self,
        snapshot: &'this Snapshot,
        owner: impl ObjectAPI<'a>,
    ) -> HasAttributeIterator<'this, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        let prefix = ThingEdgeHas::prefix_from_object(owner.into_vertex());
        HasAttributeIterator::new(
            snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeHas::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn get_has_type_unordered<'this, 'a>(
        &'this self,
        snapshot: &'this Snapshot,
        owner: impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
    ) -> Result<HasAttributeIterator<'this, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE }>, ConceptReadError>
    {
        let value_type = match attribute_type.get_value_type(snapshot, self.type_manager())? {
            None => {
                todo!("Handle missing value type - for abstract attributes. Or assume this will never happen")
            }
            Some(value_type) => value_type,
        };
        let prefix =
            ThingEdgeHas::prefix_from_object_to_type(owner.into_vertex(), value_type, attribute_type.into_vertex());
        Ok(HasAttributeIterator::new(
            snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeHas::FIXED_WIDTH_ENCODING)),
        ))
    }

    pub(crate) fn get_has_type_ordered<'this, 'a>(
        &'this self,
        snapshot: &Snapshot,
        owner: impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Vec<Attribute<'static>>, ConceptReadError> {
        let key = HAS_ORDER_PROPERTY_FACTORY.build(owner.into_vertex(), attribute_type.vertex());
        let value_type = match attribute_type.get_value_type(snapshot, self.type_manager())? {
            None => {
                todo!("Handle missing value type - for abstract attributes. Or assume this will never happen")
            }
            Some(value_type) => value_type,
        };
        let attributes = snapshot
            .get_mapped(key.as_storage_key().as_reference(), |bytes| {
                decode_attribute_ids(value_type, bytes.bytes())
                    .map(|id| Attribute::new(AttributeVertex::new(Bytes::Array(ByteArray::copy(id.bytes())))))
                    .collect()
            })
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?
            .unwrap_or_else(Vec::new);
        Ok(attributes)
    }

    pub(crate) fn get_owners<'this, 'a>(
        &'this self,
        snapshot: &'this Snapshot,
        attribute: Attribute<'a>,
    ) -> AttributeOwnerIterator<'this, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }> {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute.into_vertex());
        AttributeOwnerIterator::new(
            snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn has_owners<'a>(
        &self,
        snapshot: &Snapshot,
        attribute: Attribute<'a>,
        buffered_only: bool,
    ) -> bool {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute.into_vertex());
        snapshot
            .any_in_range(KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING), buffered_only)
    }

    pub(crate) fn get_relations_roles<'this, 'a>(
        &'this self,
        snapshot: &'this Snapshot,
        player: impl ObjectAPI<'a>,
    ) -> RelationRoleIterator<'this, { ThingEdgeRolePlayer::LENGTH_PREFIX_FROM }> {
        let prefix = ThingEdgeRolePlayer::prefix_reverse_from_player(player.into_vertex());
        RelationRoleIterator::new(
            snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeRolePlayer::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn has_role_players<'a>(
        &self,
        snapshot: &Snapshot,
        relation: Relation<'a>,
        buffered_only: bool, // FIXME use enums
    ) -> bool {
        let prefix = ThingEdgeRolePlayer::prefix_from_relation(relation.into_vertex());
        snapshot
            .any_in_range(KeyRange::new_within(prefix, ThingEdgeRolePlayer::FIXED_WIDTH_ENCODING), buffered_only)
    }

    pub(crate) fn get_role_players<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        relation: impl ObjectAPI<'a>,
    ) -> RolePlayerIterator<'_, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        let prefix = ThingEdgeRolePlayer::prefix_from_relation(relation.into_vertex());
        RolePlayerIterator::new(
            snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeRolePlayer::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn get_indexed_players<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        from: Object<'_>,
    ) -> IndexedPlayersIterator<'_, { ThingEdgeRelationIndex::LENGTH_PREFIX_FROM }> {
        let prefix = ThingEdgeRelationIndex::prefix_from(from.vertex());
        IndexedPlayersIterator::new(
            snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeRelationIndex::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn get_status(
        &self,
        snapshot: &Snapshot,
        key: StorageKey<'_, BUFFER_KEY_INLINE>,
    ) -> ConceptStatus {
        snapshot
            .get_buffered_write_mapped(key.as_reference(), |write| match write {
                Write::Insert { .. } => ConceptStatus::Inserted,
                Write::Put { .. } => ConceptStatus::Put,
                Write::Delete => ConceptStatus::Deleted,
            })
            .unwrap_or_else(|| {
                debug_assert!(snapshot.get::<BUFFER_KEY_INLINE>(key.as_reference()).unwrap().is_some());
                ConceptStatus::Persisted
            })
    }
}

impl<'txn, Snapshot: WritableSnapshot> ThingManager<Snapshot> {
    pub(crate) fn relation_compound_update_mutex(&self) -> &Mutex<()> {
        &self.relation_lock
    }

    pub(crate) fn lock_existing<'a>(
        &self,
        snapshot: &mut Snapshot,
        object: impl ObjectAPI<'a>,
    ) {
        snapshot.unmodifiable_lock_add(object.into_vertex().as_storage_key().into_owned_array())
    }

    pub fn finalise(self, snapshot: &mut Snapshot) -> Result<(), Vec<ConceptWriteError>> {
        self.cleanup_relations(snapshot).map_err(|err| Vec::from([err]))?;
        self.cleanup_attributes(snapshot).map_err(|err| Vec::from([err]))?;
        let thing_errors = self.thing_errors(snapshot);
        match thing_errors {
            Ok(errors) => {
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors)
                }
            }
            Err(error) => Err(Vec::from([ConceptWriteError::ConceptRead { source: error }])),
        }
    }

    fn cleanup_relations(&self, snapshot: &mut Snapshot) -> Result<(), ConceptWriteError> {
        let mut any_deleted = true;
        while any_deleted {
            any_deleted = false;
            for (key, _) in snapshot
                .iterate_writes_range(KeyRange::new_within(
                    ThingEdgeRolePlayer::prefix().into_byte_array_or_ref(),
                    ThingEdgeRolePlayer::FIXED_WIDTH_ENCODING,
                ))
                .filter(|(_, write)| matches!(write, Write::Delete))
                .collect::<Vec<_>>()
                .into_iter()
            {
                let edge = ThingEdgeRolePlayer::new(Bytes::Reference(key.byte_array().as_ref()));
                let relation = Relation::new(edge.to());
                if !relation.has_players(snapshot, self) {
                    relation.delete(snapshot, self)?;
                    any_deleted = true;
                }
            }
        }
        Ok(())
    }

    fn cleanup_attributes(&self, snapshot: &mut Snapshot) -> Result<(), ConceptWriteError> {
        for (key, _) in snapshot
            .iterate_writes_range(KeyRange::new_within(
                ThingEdgeHas::prefix().into_byte_array_or_ref(),
                ThingEdgeHas::FIXED_WIDTH_ENCODING,
            ))
            .filter(|(_, write)| matches!(write, Write::Delete))
            .collect::<Vec<_>>()
            .into_iter()
        {
            let edge = ThingEdgeHas::new(Bytes::Reference(key.byte_array().as_ref()));
            let attribute = Attribute::new(edge.to());
            let is_independent = attribute
                .type_()
                .is_independent(snapshot, self.type_manager())
                .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
            if !is_independent && !attribute.has_owners(snapshot, self) {
                attribute.delete(snapshot, self)?;
            }
        }
        Ok(())
    }

    fn thing_errors(&self, snapshot: &mut Snapshot) -> Result<Vec<ConceptWriteError>, ConceptReadError> {
        let mut errors = Vec::new();
        let mut relations_validated = HashSet::new();
        for (key, _) in snapshot.iterate_writes_range(KeyRange::new_within(
            ThingEdgeRolePlayer::prefix().into_byte_array_or_ref(),
            ThingEdgeRolePlayer::FIXED_WIDTH_ENCODING,
        )) {
            let edge = ThingEdgeRolePlayer::new(Bytes::Reference(key.byte_array().as_ref()));
            let relation = Relation::new(edge.from());
            if !relations_validated.contains(&relation) {
                errors.extend(relation.errors(snapshot, self)?);
                relations_validated.insert(relation.into_owned());
            }
        }
        Ok(errors)
    }

    pub fn create_entity(
        &self,
        snapshot: &mut Snapshot,
        entity_type: EntityType<'static>,
    ) -> Result<Entity<'_>, ConceptWriteError> {
        Ok(Entity::new(self.vertex_generator.create_entity(entity_type.vertex().type_id_(), snapshot)))
    }

    pub fn create_relation(
        &self,
        snapshot: &mut Snapshot,
        relation_type: RelationType<'static>,
    ) -> Result<Relation<'_>, ConceptWriteError> {
        Ok(Relation::new(
            self.vertex_generator.create_relation(relation_type.vertex().type_id_(), snapshot),
        ))
    }

    pub fn create_attribute(
        &self,
        snapshot: &mut Snapshot,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<Attribute<'_>, ConceptWriteError> {
        let value_type = attribute_type.get_value_type(snapshot, self.type_manager.as_ref())?;
        if Some(value.value_type()) == value_type {
            let vertex = match value {
                Value::Boolean(_bool) => {
                    todo!()
                }
                Value::Long(long) => {
                    let encoded_long = LongBytes::build(long);
                    self.vertex_generator.create_attribute_long(
                        attribute_type.vertex().type_id_(),
                        encoded_long,
                        snapshot,
                    )
                }
                Value::Double(_double) => {
                    todo!()
                }
                Value::String(string) => {
                    let encoded_string: StringBytes<'_, BUFFER_KEY_INLINE> = StringBytes::build_ref(&string);
                    self.vertex_generator
                        .create_attribute_string(
                            attribute_type.vertex().type_id_(),
                            encoded_string,
                            snapshot,
                        )
                        .map_err(|err| ConceptWriteError::SnapshotIterate { source: err })?
                }
            };
            Ok(Attribute::new(vertex))
        } else {
            Err(ConceptWriteError::ValueTypeMismatch { expected: value_type, provided: value.value_type() })
        }
    }

    pub(crate) fn delete_entity(&self, snapshot: &mut Snapshot, entity: Entity<'_>) {
        let key = entity.into_vertex().into_storage_key().into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        snapshot.delete(key)
    }

    pub(crate) fn delete_relation(&self, snapshot: &mut Snapshot, relation: Relation<'_>) {
        let key = relation.into_vertex().into_storage_key().into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        snapshot.delete(key)
    }

    pub(crate) fn delete_attribute(&self, snapshot: &mut Snapshot, attribute: Attribute<'_>) {
        let key = attribute.into_vertex().into_storage_key().into_owned_array();
        snapshot.delete(key);
    }

    pub(crate) fn set_has<'a>(&self, snapshot: &mut Snapshot, owner: impl ObjectAPI<'a>, attribute: Attribute<'_>) {
        // TODO: handle duplicates
        // note: we always re-put the attribute. TODO: optimise knowing when the attribute pre-exists.
        snapshot.put(attribute.vertex().as_storage_key().into_owned_array());
        owner.set_modified(snapshot, self);
        let has = ThingEdgeHas::build(owner.vertex(), attribute.vertex());
        snapshot.put_val(has.into_storage_key().into_owned_array(), encode_value_u64(1));
        let has_reverse = ThingEdgeHasReverse::build(attribute.into_vertex(), owner.into_vertex());
        snapshot.put_val(has_reverse.into_storage_key().into_owned_array(), encode_value_u64(1));
    }

    pub(crate) fn delete_has<'a>(&self, snapshot: &mut Snapshot, owner: impl ObjectAPI<'a>, attribute: Attribute<'_>) {
        owner.set_modified(snapshot, self);
        let has = ThingEdgeHas::build(owner.vertex(), attribute.vertex());
        snapshot.delete(has.into_storage_key().into_owned_array());
        let has_reverse = ThingEdgeHasReverse::build(attribute.into_vertex(), owner.into_vertex());
        snapshot.delete(has_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn set_has_ordered<'a>(
        &self,
        snapshot: &mut Snapshot,
        owner: impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
        attributes: Vec<Attribute<'_>>,
    ) {
        owner.set_modified(snapshot, self);
        let key = HAS_ORDER_PROPERTY_FACTORY.build(owner.into_vertex(), attribute_type.into_vertex());
        let value = encode_attribute_ids(attributes.into_iter().map(|attr| attr.into_vertex().attribute_id()));
        snapshot.put_val(key.into_storage_key().into_owned_array(), value)
    }

    pub(crate) fn increment_has<'a>(&self, snapshot: &mut Snapshot, owner: impl ObjectAPI<'a>, attribute: Attribute<'_>) {
        todo!()
    }

    pub(crate) fn decrement_has<'a>(
        &self,
        snapshot: &mut Snapshot,
        owner: impl ObjectAPI<'a>,
        attribute: Attribute<'a>,
        decrement_count: u64,
    ) {
        todo!()
    }

    pub(crate) fn delete_has_ordered<'a>(
        &self,
        snapshot: &mut Snapshot,
        owner: impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
    ) {
        owner.set_modified(snapshot, self);
        let order_property = HAS_ORDER_PROPERTY_FACTORY.build(owner.into_vertex(), attribute_type.into_vertex());
        snapshot.delete(order_property.into_storage_key().into_owned_array())
    }

    pub fn set_role_player<'a>(
        &self,
        snapshot: &mut Snapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType<'_>,
    ) -> Result<(), ConceptWriteError> {
        let role_player =
            ThingEdgeRolePlayer::build_role_player(relation.vertex(), player.vertex(), role_type.clone().into_vertex());
        let count: u64 = 1;
        snapshot.put_val(role_player.into_storage_key().into_owned_array(), encode_value_u64(count));
        let role_player_reverse = ThingEdgeRolePlayer::build_role_player_reverse(
            player.clone().into_vertex(),
            relation.clone().into_vertex(),
            role_type.clone().into_vertex(),
        );
        // must be idempotent, so no lock required -- cannot fail
        snapshot.put_val(role_player_reverse.into_storage_key().into_owned_array(), encode_value_u64(count));

        if self.type_manager.relation_index_available(snapshot, relation.type_())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })? {
            self.relation_index_player_regenerate(snapshot, relation, Object::new(player.vertex()), role_type, 1)?;
        }
        Ok(())
    }

    ///
    /// Delete all counts of the specific role player in a given relation, and update indexes if required
    ///
    pub fn delete_role_player<'a>(
        &self,
        snapshot: &mut Snapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType<'_>,
    ) -> Result<(), ConceptWriteError> {
        let role_player =
            ThingEdgeRolePlayer::build_role_player(relation.vertex(), player.vertex(), role_type.clone().into_vertex());
        snapshot.delete(role_player.into_storage_key().into_owned_array());
        let role_player_reverse = ThingEdgeRolePlayer::build_role_player_reverse(
            player.clone().into_vertex(),
            relation.clone().into_vertex(),
            role_type.clone().into_vertex(),
        );
        snapshot.delete(role_player_reverse.into_storage_key().into_owned_array());

        if self.type_manager.relation_index_available(snapshot, relation.type_())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })? {
            self.relation_index_player_deleted(snapshot, relation, player, role_type)?;
        }
        Ok(())
    }

    ///
    /// Add a player to a relation that supports duplicates
    /// Caller must provide a lock that prevents race conditions on the player counts on the relation
    ///
    pub(crate) fn increment_role_player<'a>(
        &self,
        snapshot: &mut Snapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType<'_>,
    ) -> Result<(), ConceptWriteError> {
        let role_player =
            ThingEdgeRolePlayer::build_role_player(relation.vertex(), player.vertex(), role_type.clone().into_vertex());
        let role_player_reverse =
            ThingEdgeRolePlayer::build_role_player_reverse(player.vertex(), relation.vertex(), role_type.clone().into_vertex());

        let mut count = 0;
        let rp_count =
            snapshot.get_mapped(role_player.as_storage_key().as_reference(), |val| decode_value_u64(val)).unwrap();
        let rp_reverse_count = snapshot
            .get_mapped(role_player_reverse.as_storage_key().as_reference(), |val| decode_value_u64(val))
            .unwrap();
        debug_assert_eq!(&rp_count, &rp_reverse_count);

        count = rp_count.unwrap_or(0) + 1;
        let reverse_count = rp_reverse_count.unwrap_or(0) + 1;
        snapshot.put_val(role_player.as_storage_key().into_owned_array(), encode_value_u64(count));
        snapshot.put_val(role_player_reverse.as_storage_key().into_owned_array(), encode_value_u64(reverse_count));

        // must lock to fail concurrent transactions updating the same counters
        snapshot.exclusive_lock_add(role_player.into_storage_key().into_owned_array().into_byte_array());

        if self.type_manager.relation_index_available(snapshot, relation.type_())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })? {
            self.relation_index_player_regenerate(snapshot, relation, Object::new(player.vertex()), role_type, count)?;
        }
        Ok(())
    }

    ///
    /// Remove a player to a relation that supports duplicates
    /// Caller must provide a lock that prevents race conditions on the player counts on the relation
    ///
    pub(crate) fn decrement_role_player<'a>(
        &self,
        snapshot: &mut Snapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType<'_>,
        decrement_count: u64,
    ) -> Result<(), ConceptWriteError> {
        let role_player =
            ThingEdgeRolePlayer::build_role_player(relation.vertex(), player.vertex(), role_type.clone().into_vertex());
        let role_player_reverse =
            ThingEdgeRolePlayer::build_role_player_reverse(player.vertex(), relation.vertex(), role_type.clone().into_vertex());

        let rp_count =
            snapshot.get_mapped(role_player.as_storage_key().as_reference(), |val| decode_value_u64(val)).unwrap();
        let rp_reverse_count = snapshot
            .get_mapped(role_player_reverse.as_storage_key().as_reference(), |val| decode_value_u64(val))
            .unwrap();
        debug_assert_eq!(&rp_count, &rp_reverse_count);

        let count = rp_count.unwrap() - decrement_count;
        debug_assert!(count >= 0);
        let reverse_count = rp_reverse_count.unwrap() - decrement_count;
        debug_assert!(reverse_count >= 0);
        if count == 0 {
            snapshot.delete(role_player.as_storage_key().into_owned_array());
            snapshot.delete(role_player_reverse.as_storage_key().into_owned_array());
        } else {
            snapshot.put_val(role_player.as_storage_key().into_owned_array(), encode_value_u64(count));
            snapshot
                .put_val(role_player_reverse.as_storage_key().into_owned_array(), encode_value_u64(reverse_count));
        }

        // must lock to fail concurrent transactions updating the same counters
        snapshot.exclusive_lock_add(role_player.into_storage_key().into_owned_array().into_byte_array());

        if self.type_manager.relation_index_available(snapshot, relation.type_())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })? {
            self.relation_index_player_regenerate(snapshot, relation, Object::new(player.vertex()), role_type, count)?
        }
        Ok(())
    }

    ///
    /// Clean up all parts of a relation index to do with a specific role player
    /// after the player has been deleted.
    ///
    pub(crate) fn relation_index_player_deleted<'a>(
        &self,
        snapshot: &mut Snapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType<'_>,
    ) -> Result<(), ConceptWriteError> {
        let players = relation.get_players(snapshot, self)
            .collect_cloned_vec(|(roleplayer, count)| (roleplayer.player().into_owned(), roleplayer.role_type()))
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        for (rp_player, rp_role_type) in players {
            debug_assert!(!(rp_player == Object::new(rp_player.vertex()) && role_type == rp_role_type));
            let index = ThingEdgeRelationIndex::build(
                player.vertex(),
                rp_player.vertex(),
                relation.vertex(),
                role_type.vertex().type_id_(),
                rp_role_type.vertex().type_id_(),
            );
            snapshot.delete(index.as_storage_key().into_owned_array());
            let index_reverse = ThingEdgeRelationIndex::build(
                rp_player.vertex(),
                player.vertex(),
                relation.vertex(),
                rp_role_type.vertex().type_id_(),
                role_type.vertex().type_id_(),
            );
            snapshot.delete(index_reverse.as_storage_key().into_owned_array());
        }
        Ok(())
    }

    ///
    /// For N duplicate role players, the self-edges are available N-1 times.
    /// For N duplicate player 1, and M duplicate player 2 - from N to M has M index repetitions, while M to N has N index repetitions
    ///
    pub(crate) fn relation_index_player_regenerate(
        &self,
        snapshot: &mut Snapshot,
        relation: Relation<'_>,
        player: Object<'_>,
        role_type: RoleType<'_>,
        total_player_count: u64,
    ) -> Result<(), ConceptWriteError> {
        debug_assert_ne!(total_player_count, 0);
        let players = relation.get_players(snapshot, self)
            .collect_cloned_vec(|(roleplayer, count)| (roleplayer.player().into_owned(), roleplayer.role_type(), count))
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        for (rp_player, rp_role_type, rp_count) in players {
            let is_same_rp = rp_player == player && rp_role_type == role_type;
            if is_same_rp && total_player_count > 1 {
                let repetitions = total_player_count - 1;
                let index = ThingEdgeRelationIndex::build(
                    player.vertex(),
                    player.vertex(),
                    relation.vertex(),
                    role_type.vertex().type_id_(),
                    role_type.vertex().type_id_(),
                );
                snapshot.put_val(index.as_storage_key().into_owned_array(), encode_value_u64(repetitions));
            } else if !is_same_rp {
                let rp_repetitions = rp_count;
                let index = ThingEdgeRelationIndex::build(
                    player.vertex(),
                    rp_player.vertex(),
                    relation.vertex(),
                    role_type.vertex().type_id_(),
                    rp_role_type.vertex().type_id_(),
                );
                snapshot.put_val(index.as_storage_key().into_owned_array(), encode_value_u64(rp_repetitions));
                let player_repetitions = total_player_count;
                let index_reverse = ThingEdgeRelationIndex::build(
                    rp_player.vertex(),
                    player.vertex(),
                    relation.vertex(),
                    rp_role_type.vertex().type_id_(),
                    role_type.vertex().type_id_(),
                );
                snapshot
                    .put_val(index_reverse.as_storage_key().into_owned_array(), encode_value_u64(player_repetitions));
            }
        }
        Ok(())
    }
}
