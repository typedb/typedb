/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, collections::HashSet, marker::PhantomData, sync::Arc};

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
        boolean_bytes::BooleanBytes, date_time_bytes::DateTimeBytes, decode_value_u64, double_bytes::DoubleBytes,
        encode_value_u64, long_bytes::LongBytes, string_bytes::StringBytes, value_type::ValueType, ValueEncodable,
    },
    Keyable,
};
use itertools::Itertools;
use regex::Regex;
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
        object::{HasAttributeIterator, Object, ObjectAPI, ObjectIterator},
        relation::{IndexedPlayersIterator, Relation, RelationIterator, RelationRoleIterator, RolePlayerIterator},
        value::Value,
        ThingAPI,
    },
    type_::{
        annotation::AnnotationKey,
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::EntityType,
        object_type::ObjectType,
        owns::OwnsAnnotation,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::TypeManager,
        ObjectTypeAPI, OwnerAPI, TypeAPI,
    },
    ConceptStatus,
};

pub struct ThingManager<Snapshot> {
    vertex_generator: Arc<ThingVertexGenerator>,
    type_manager: Arc<TypeManager<Snapshot>>,
    snapshot: PhantomData<Snapshot>,
}

impl<Snapshot: ReadableSnapshot> ThingManager<Snapshot> {
    pub fn new(vertex_generator: Arc<ThingVertexGenerator>, type_manager: Arc<TypeManager<Snapshot>>) -> Self {
        ThingManager { vertex_generator, type_manager, snapshot: PhantomData }
    }

    pub(crate) fn type_manager(&self) -> &TypeManager<Snapshot> {
        &self.type_manager
    }

    pub fn get_entities<'this>(&'this self, snapshot: &'this Snapshot) -> EntityIterator<'_, 1> {
        let prefix = ObjectVertex::build_prefix_prefix(Prefix::VertexEntity);
        let snapshot_iterator =
            snapshot.iterate_range(KeyRange::new_within(prefix, Prefix::VertexEntity.fixed_width_keys()));
        EntityIterator::new(snapshot_iterator)
    }

    pub fn get_objects_in<'this>(
        &'this self,
        snapshot: &'this Snapshot,
        object_type: ObjectType<'_>,
    ) -> ObjectIterator<'_, 3> {
        let vertex_prefix = match object_type {
            ObjectType::Entity(_) => Prefix::VertexEntity,
            ObjectType::Relation(_) => Prefix::VertexRelation,
        };
        let prefix = ObjectVertex::build_prefix_type(vertex_prefix.prefix_id(), object_type.vertex().type_id_());
        let snapshot_iterator = snapshot.iterate_range(KeyRange::new_within(prefix, vertex_prefix.fixed_width_keys()));
        ObjectIterator::new(snapshot_iterator)
    }

    pub fn get_entities_in<'this>(
        &'this self,
        snapshot: &'this Snapshot,
        entity_type: EntityType<'_>,
    ) -> EntityIterator<'_, 3> {
        let prefix = ObjectVertex::build_prefix_type(Prefix::VertexEntity.prefix_id(), entity_type.vertex().type_id_());
        let snapshot_iterator =
            snapshot.iterate_range(KeyRange::new_within(prefix, Prefix::VertexEntity.fixed_width_keys()));
        EntityIterator::new(snapshot_iterator)
    }

    pub fn get_relations<'this>(&'this self, snapshot: &'this Snapshot) -> RelationIterator<'_, 1> {
        let prefix = ObjectVertex::build_prefix_prefix(Prefix::VertexRelation);
        let snapshot_iterator =
            snapshot.iterate_range(KeyRange::new_within(prefix, Prefix::VertexRelation.fixed_width_keys()));
        RelationIterator::new(snapshot_iterator)
    }

    pub fn get_relations_in<'this>(
        &'this self,
        snapshot: &'this Snapshot,
        relation_type: RelationType<'_>,
    ) -> RelationIterator<'_, 3> {
        let prefix =
            ObjectVertex::build_prefix_type(Prefix::VertexRelation.prefix_id(), relation_type.vertex().type_id_());
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
        let has_reverse_iterator = snapshot.iterate_range(KeyRange::new_inclusive(has_reverse_start, has_reverse_end));
        AttributeIterator::new(attribute_iterator, has_reverse_iterator, snapshot, self.type_manager())
    }

    pub fn get_attributes_in<'this>(
        &'this self,
        snapshot: &'this Snapshot,
        attribute_type: AttributeType<'_>,
    ) -> Result<AttributeIterator<'this, Snapshot, 3, 4>, ConceptReadError> {
        let Some(value_type) = attribute_type.get_value_type(snapshot, self.type_manager.as_ref())? else {
            return Ok(AttributeIterator::new_empty());
        };

        let attribute_value_type_prefix = AttributeVertex::value_type_to_prefix_type(value_type);
        let prefix =
            AttributeVertex::build_prefix_type(attribute_value_type_prefix, attribute_type.vertex().type_id_());
        let attribute_iterator =
            snapshot.iterate_range(KeyRange::new_within(prefix, attribute_value_type_prefix.fixed_width_keys()));

        let has_reverse_prefix =
            ThingEdgeHasReverse::prefix_from_type(attribute_value_type_prefix, attribute_type.vertex().type_id_());
        let has_reverse_iterator =
            snapshot.iterate_range(KeyRange::new_within(has_reverse_prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING));

        Ok(AttributeIterator::new(attribute_iterator, has_reverse_iterator, snapshot, self.type_manager()))
    }

    pub(crate) fn get_attribute_value(
        &self,
        snapshot: &Snapshot,
        attribute: &Attribute<'_>,
    ) -> Result<Value<'static>, ConceptReadError> {
        match attribute.value_type() {
            ValueType::Boolean => {
                let attribute_id = attribute.vertex().attribute_id().unwrap_boolean();
                Ok(Value::Boolean(BooleanBytes::new(attribute_id.bytes()).as_bool()))
            }
            ValueType::Long => {
                let attribute_id = attribute.vertex().attribute_id().unwrap_long();
                Ok(Value::Long(LongBytes::new(attribute_id.bytes()).as_i64()))
            }
            ValueType::Double => {
                let attribute_id = attribute.vertex().attribute_id().unwrap_double();
                Ok(Value::Double(DoubleBytes::new(attribute_id.bytes()).as_f64()))
            }
            ValueType::DateTime => {
                let attribute_id = attribute.vertex().attribute_id().unwrap_date_time();
                Ok(Value::DateTime(DateTimeBytes::new(attribute_id.bytes()).as_naive_date_time()))
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

    pub fn get_attribute_with_value(
        &self,
        snapshot: &Snapshot,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<Option<Attribute<'static>>, ConceptReadError> {
        let attribute = match value.value_type() {
            ValueType::Boolean | ValueType::Long | ValueType::Double | ValueType::DateTime => {
                debug_assert!(AttributeID::is_inlineable(value.as_reference()));
                match self.get_attribute_with_value_inline(snapshot, attribute_type, value) {
                    Ok(Some(attribute)) => attribute,
                    fail => return fail,
                }
            }
            ValueType::String => {
                if AttributeID::is_inlineable(value.as_reference()) {
                    match self.get_attribute_with_value_inline(snapshot, attribute_type, value) {
                        Ok(Some(attribute)) => attribute,
                        fail => return fail,
                    }
                } else {
                    match self.vertex_generator.find_attribute_id_string_noinline(
                        attribute_type.vertex().type_id_(),
                        value.encode_string::<256>(),
                        snapshot,
                    ) {
                        Ok(Some(id)) => Attribute::new(AttributeVertex::new(Bytes::copy(&id.bytes()))),
                        Ok(None) => return Ok(None),
                        Err(err) => return Err(ConceptReadError::SnapshotIterate { source: err }),
                    }
                }
            }
        };

        let is_independent = attribute.type_().is_independent(snapshot, self.type_manager())?;
        if is_independent || attribute.has_owners(snapshot, self) {
            Ok(Some(attribute))
        } else {
            Ok(None)
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
        owner: &impl ObjectAPI<'a>,
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
        owner: &impl ObjectAPI<'a>,
    ) -> HasAttributeIterator<'this, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        let prefix = ThingEdgeHas::prefix_from_object(owner.vertex());
        HasAttributeIterator::new(
            snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeHas::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn get_has_type_unordered<'this, 'a>(
        &'this self,
        snapshot: &'this Snapshot,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
    ) -> Result<HasAttributeIterator<'this, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE }>, ConceptReadError>
    {
        let value_type = match attribute_type.get_value_type(snapshot, self.type_manager())? {
            None => {
                todo!("Handle missing value type - for abstract attributes. Or assume this will never happen")
            }
            Some(value_type) => value_type,
        };
        let prefix = ThingEdgeHas::prefix_from_object_to_type(owner.vertex(), value_type, attribute_type.into_vertex());
        Ok(HasAttributeIterator::new(
            snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeHas::FIXED_WIDTH_ENCODING)),
        ))
    }

    pub(crate) fn get_has_type_ordered<'this, 'a>(
        &'this self,
        snapshot: &Snapshot,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Vec<Attribute<'static>>, ConceptReadError> {
        let key = HAS_ORDER_PROPERTY_FACTORY.build(owner.vertex(), attribute_type.vertex());
        let value_type = match attribute_type.get_value_type(snapshot, self.type_manager())? {
            None => {
                todo!("Handle missing value type - for abstract attributes. Or assume this will never happen")
            }
            Some(value_type) => value_type,
        };
        let attributes = snapshot
            .get_mapped(key.as_storage_key().as_reference(), |bytes| {
                decode_attribute_ids(value_type, bytes.bytes())
                    .map(|id| Attribute::new(AttributeVertex::new(Bytes::copy(id.bytes()))))
                    .collect()
            })
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?
            .unwrap_or_else(Vec::new);
        Ok(attributes)
    }

    pub(crate) fn get_owners<'this>(
        &'this self,
        snapshot: &'this Snapshot,
        attribute: Attribute<'_>,
    ) -> AttributeOwnerIterator<'this, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }> {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute.into_vertex());
        AttributeOwnerIterator::new(
            snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn get_owners_by_type<'this, 'a>(
        &'this self,
        snapshot: &'this Snapshot,
        attribute: Attribute<'_>,
        owner_type: impl ObjectTypeAPI<'a>,
    ) -> AttributeOwnerIterator<'this, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM_TO_TYPE }> {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute_to_type(attribute.into_vertex(), owner_type.vertex());
        AttributeOwnerIterator::new(
            snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn has_owners(&self, snapshot: &Snapshot, attribute: Attribute<'_>, buffered_only: bool) -> bool {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute.into_vertex());
        snapshot.any_in_range(KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING), buffered_only)
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

    pub(crate) fn has_role_players(
        &self,
        snapshot: &Snapshot,
        relation: Relation<'_>,
        buffered_only: bool, // FIXME use enums
    ) -> bool {
        let prefix = ThingEdgeRolePlayer::prefix_from_relation(relation.into_vertex());
        snapshot.any_in_range(KeyRange::new_within(prefix, ThingEdgeRolePlayer::FIXED_WIDTH_ENCODING), buffered_only)
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

    pub(crate) fn get_status(&self, snapshot: &Snapshot, key: StorageKey<'_, BUFFER_KEY_INLINE>) -> ConceptStatus {
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

    pub(crate) fn object_exists<'a>(
        &self,
        snapshot: &Snapshot,
        owner: &impl ObjectAPI<'a>,
    ) -> Result<bool, ConceptReadError> {
        match snapshot.get::<0>(owner.vertex().as_storage_key().as_reference()) {
            Ok(value) => Ok(value.is_some()),
            Err(error) => Err(ConceptReadError::SnapshotGet { source: error }),
        }
    }
}

impl<'txn, Snapshot: WritableSnapshot> ThingManager<Snapshot> {
    pub(crate) fn lock_existing<'a>(&self, snapshot: &mut Snapshot, object: impl ObjectAPI<'a>) {
        snapshot.unmodifiable_lock_add(object.into_vertex().as_storage_key().into_owned_array())
    }

    pub fn finalise(self, snapshot: &mut Snapshot) -> Result<(), Vec<ConceptWriteError>> {
        self.cleanup_relations(snapshot).map_err(|err| vec![err])?;
        self.cleanup_attributes(snapshot).map_err(|err| vec![err])?;
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
            for (key, write) in snapshot
                .iterate_buffered_writes_range(KeyRange::new_within(
                    ThingEdgeRolePlayer::prefix(),
                    ThingEdgeRolePlayer::FIXED_WIDTH_ENCODING,
                ))
                .into_range()
                .into_iter()
            {
                if matches!(write, Write::Delete) {
                    let edge = ThingEdgeRolePlayer::new(Bytes::Reference(key.byte_array().as_ref()));
                    let relation = Relation::new(edge.from());
                    if relation.get_status(snapshot, self) == ConceptStatus::Deleted {
                        continue;
                    }
                    if !relation.has_players(snapshot, self) {
                        relation.delete(snapshot, self)?;
                        any_deleted = true;
                    }
                }
            }
        }
        Ok(())
    }

    fn cleanup_attributes(&self, snapshot: &mut Snapshot) -> Result<(), ConceptWriteError> {
        for (key, _value) in snapshot
            .iterate_writes_range(KeyRange::new_exclusive(
                Bytes::inline(Prefix::ATTRIBUTE_MIN.prefix_id().bytes(), 1),
                Bytes::inline(Prefix::ATTRIBUTE_MAX.prefix_id().bytes(), 1),
            ))
            .filter_map(|(key, write)| match write {
                Write::Put { value, .. } => Some((key, value)),
                _ => None,
            })
            .collect_vec()
            .into_iter()
        {
            let attribute = Attribute::new(AttributeVertex::new(Bytes::reference(key.bytes())));
            let is_independent = attribute
                .type_()
                .is_independent(snapshot, self.type_manager())
                .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
            if !is_independent && !attribute.has_owners(snapshot, self) {
                self.unput_attribute(snapshot, attribute)?;
            }
        }

        for (key, _write) in snapshot
            .iterate_buffered_writes_range(KeyRange::new_within(
                ThingEdgeHas::prefix(),
                ThingEdgeHas::FIXED_WIDTH_ENCODING,
            ))
            .into_range()
            .into_iter()
            .filter(|(_, write)| matches!(write, Write::Delete))
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

        self.validate_ownerships(&mut errors, snapshot)?;

        for key in snapshot
            .iterate_writes_range(KeyRange::new_within(
                Bytes::<0>::reference(ObjectVertex::build_prefix_prefix(Prefix::VertexEntity).bytes()),
                Prefix::VertexEntity.fixed_width_keys(),
            ))
            .filter_map(|(key, write)| match write {
                Write::Insert { .. } => Some(key),
                Write::Delete => None,
                Write::Put { .. } => unreachable!("Encountered a Put for an entity"),
            })
        {
            let entity = Entity::new(ObjectVertex::new(Bytes::reference(key.bytes())));
            let entity_type = entity.type_();
            for owns in &entity_type.get_owns(snapshot, self.type_manager())? {
                if owns.get_annotations(snapshot, self.type_manager())?.contains(&OwnsAnnotation::Key(AnnotationKey))
                    && entity.get_has_type(snapshot, self, owns.attribute())?.next().is_none()
                {
                    errors.push(ConceptWriteError::KeyMissing {})
                }
            }
        }

        let mut relations_validated = HashSet::new();
        for (key, _) in snapshot
            .iterate_buffered_writes_range(KeyRange::new_within(
                ThingEdgeRolePlayer::prefix(),
                ThingEdgeRolePlayer::FIXED_WIDTH_ENCODING,
            ))
            .into_range()
        {
            let edge = ThingEdgeRolePlayer::new(Bytes::reference(key.bytes()));
            let relation = Relation::new(edge.from());
            if !relations_validated.contains(&relation) {
                errors.extend(relation.errors(snapshot, self)?);
                relations_validated.insert(relation.into_owned());
            }
        }

        Ok(errors)
    }

    fn validate_ownerships(
        &self,
        errors: &mut Vec<ConceptWriteError>,
        snapshot: &Snapshot,
    ) -> Result<(), ConceptReadError> {
        for (key, write) in snapshot
            .iterate_buffered_writes_range(KeyRange::new_within(
                ThingEdgeHas::prefix(),
                ThingEdgeHas::FIXED_WIDTH_ENCODING,
            ))
            .into_range()
            .into_iter()
        {
            let edge = ThingEdgeHas::new(Bytes::Reference(key.byte_array().as_ref()));

            let owner = Object::new(edge.from());
            if !self.object_exists(snapshot, &owner)? {
                continue;
            }
            let owner_type = owner.type_();

            let attribute = Attribute::new(edge.to());
            let attribute_type = attribute.type_();

            let owns = owner_type
                .get_owns_attribute(snapshot, self.type_manager(), attribute_type.clone())?
                .expect("encountered a has edge without a corresponding owns in the schema");

            if owns.is_unique(snapshot, self.type_manager())? {
                match write {
                    Write::Insert { .. } | Write::Put { .. } => (),
                    Write::Delete => (),
                }
            }

            if let Some(cardinality) = owns.get_cardinality(snapshot, self.type_manager())? {
                let count = owner.get_has_type(snapshot, self, attribute_type.clone())?.count();
                if !cardinality.is_valid(count as u64) {
                    errors.push(ConceptWriteError::KeyMissing {}) // FIXME
                }
            }
        }
        Ok(())
    }

    pub fn create_entity<'a>(
        &self,
        snapshot: &mut Snapshot,
        entity_type: EntityType<'static>,
    ) -> Result<Entity<'a>, ConceptWriteError> {
        Ok(Entity::new(self.vertex_generator.create_entity(entity_type.vertex().type_id_(), snapshot)))
    }

    pub fn create_relation<'a>(
        &self,
        snapshot: &mut Snapshot,
        relation_type: RelationType<'static>,
    ) -> Result<Relation<'a>, ConceptWriteError> {
        Ok(Relation::new(self.vertex_generator.create_relation(relation_type.vertex().type_id_(), snapshot)))
    }

    pub fn create_attribute<'a>(
        &self,
        snapshot: &mut Snapshot,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<Attribute<'a>, ConceptWriteError> {
        let value_type = attribute_type.get_value_type(snapshot, self.type_manager.as_ref())?;
        if Some(value.value_type()) == value_type {
            let vertex = match value {
                Value::Boolean(bool) => {
                    let encoded_boolean = BooleanBytes::build(bool);
                    self.vertex_generator.create_attribute_boolean(
                        attribute_type.vertex().type_id_(),
                        encoded_boolean,
                        snapshot,
                    )
                }
                Value::Long(long) => {
                    let encoded_long = LongBytes::build(long);
                    self.vertex_generator.create_attribute_long(
                        attribute_type.vertex().type_id_(),
                        encoded_long,
                        snapshot,
                    )
                }
                Value::Double(double) => {
                    let encoded_double = DoubleBytes::build(double);
                    self.vertex_generator.create_attribute_double(
                        attribute_type.vertex().type_id_(),
                        encoded_double,
                        snapshot,
                    )
                }
                Value::DateTime(date_time) => {
                    let encoded_date_time = DateTimeBytes::build(date_time);
                    self.vertex_generator.create_attribute_date_time(
                        attribute_type.vertex().type_id_(),
                        encoded_date_time,
                        snapshot,
                    )
                }
                Value::String(string) => {
                    let annotations =
                        self.type_manager.get_attribute_type_annotations(snapshot, attribute_type.clone())?;
                    for annotation in annotations.iter() {
                        match annotation {
                            AttributeTypeAnnotation::Abstract(_) => todo!("create abstract attribute"),
                            AttributeTypeAnnotation::Independent(_) => (),
                            AttributeTypeAnnotation::Regex(regex) => {
                                let regex = regex.regex();
                                if !Regex::new(regex).unwrap().is_match(&string) {
                                    return Err(ConceptWriteError::StringAttributeRegex {
                                        regex: regex.to_owned(),
                                        value: string.into_owned(),
                                    });
                                }
                            }
                        }
                    }
                    let encoded_string: StringBytes<'_, BUFFER_KEY_INLINE> = StringBytes::build_ref(&string);
                    self.vertex_generator
                        .create_attribute_string(attribute_type.vertex().type_id_(), encoded_string, snapshot)
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

    pub(crate) fn delete_attribute(
        &self,
        snapshot: &mut Snapshot,
        attribute: Attribute<'_>,
    ) -> Result<(), ConceptWriteError> {
        let key = attribute.into_vertex().into_storage_key().into_owned_array();
        snapshot.delete(key);
        Ok(())
    }

    pub(crate) fn unput_attribute(
        &self,
        snapshot: &mut Snapshot,
        mut attribute: Attribute<'_>,
    ) -> Result<(), ConceptWriteError> {
        let value = match attribute
            .get_value(snapshot, self)
            .map_err(|error| ConceptWriteError::ConceptRead { source: error })?
        {
            Value::String(string) => ByteArray::copy(string.as_bytes()),
            _ => ByteArray::empty(),
        };
        let key = attribute.into_vertex().into_storage_key().into_owned_array();
        snapshot.unput_val(key, value);
        Ok(())
    }

    pub(crate) fn set_has_unordered<'a>(
        &self,
        snapshot: &mut Snapshot,
        owner: &impl ObjectAPI<'a>,
        attribute: Attribute<'_>,
    ) {
        // TODO: handle duplicates
        // note: we always re-put the attribute. TODO: optimise knowing when the attribute pre-exists.
        snapshot.put(attribute.vertex().as_storage_key().into_owned_array());
        owner.set_modified(snapshot, self);
        let has = ThingEdgeHas::build(owner.vertex(), attribute.vertex());
        snapshot.put_val(has.into_storage_key().into_owned_array(), encode_value_u64(1));
        let has_reverse = ThingEdgeHasReverse::build(attribute.into_vertex(), owner.vertex());
        snapshot.put_val(has_reverse.into_storage_key().into_owned_array(), encode_value_u64(1));
    }

    pub(crate) fn unset_has<'a>(&self, snapshot: &mut Snapshot, owner: &impl ObjectAPI<'a>, attribute: Attribute<'_>) {
        owner.set_modified(snapshot, self);
        let has = ThingEdgeHas::build(owner.vertex(), attribute.vertex());
        snapshot.delete(has.into_storage_key().into_owned_array());
        let has_reverse = ThingEdgeHasReverse::build(attribute.into_vertex(), owner.vertex());
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
        snapshot.put_val(key.into_storage_key().into_owned_array(), value);
    }

    pub(crate) fn increment_has<'a>(
        &self,
        snapshot: &mut Snapshot,
        owner: impl ObjectAPI<'a>,
        attribute: Attribute<'_>,
    ) {
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

    pub(crate) fn unset_has_ordered<'a>(
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

        if self
            .type_manager
            .relation_index_available(snapshot, relation.type_())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?
        {
            self.relation_index_player_regenerate(snapshot, relation, Object::new(player.vertex()), role_type, 1)?;
        }
        Ok(())
    }

    ///
    /// Delete all counts of the specific role player in a given relation, and update indexes if required
    ///
    pub fn unset_role_player<'a>(
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

        if self
            .type_manager
            .relation_index_available(snapshot, relation.type_())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?
        {
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
        let role_player_reverse = ThingEdgeRolePlayer::build_role_player_reverse(
            player.vertex(),
            relation.vertex(),
            role_type.clone().into_vertex(),
        );

        let rp_count = snapshot.get_mapped(role_player.as_storage_key().as_reference(), decode_value_u64).unwrap();
        let rp_reverse_count =
            snapshot.get_mapped(role_player_reverse.as_storage_key().as_reference(), decode_value_u64).unwrap();
        debug_assert_eq!(&rp_count, &rp_reverse_count, "roleplayer count mismatch!");

        let count = rp_count.unwrap_or(0) + 1;
        let reverse_count = rp_reverse_count.unwrap_or(0) + 1;
        snapshot.put_val(role_player.as_storage_key().into_owned_array(), encode_value_u64(count));
        snapshot.put_val(role_player_reverse.as_storage_key().into_owned_array(), encode_value_u64(reverse_count));

        // must lock to fail concurrent transactions updating the same counters
        snapshot.exclusive_lock_add(role_player.into_storage_key().into_owned_array().into_byte_array());

        if self
            .type_manager
            .relation_index_available(snapshot, relation.type_())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?
        {
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
        let role_player_reverse = ThingEdgeRolePlayer::build_role_player_reverse(
            player.vertex(),
            relation.vertex(),
            role_type.clone().into_vertex(),
        );

        let rp_count = snapshot.get_mapped(role_player.as_storage_key().as_reference(), decode_value_u64).unwrap();
        let rp_reverse_count =
            snapshot.get_mapped(role_player_reverse.as_storage_key().as_reference(), decode_value_u64).unwrap();
        debug_assert_eq!(&rp_count, &rp_reverse_count, "roleplayer count mismatch!");

        let count = rp_count.unwrap() - decrement_count;
        let reverse_count = rp_reverse_count.unwrap() - decrement_count;
        if count == 0 {
            snapshot.delete(role_player.as_storage_key().into_owned_array());
            snapshot.delete(role_player_reverse.as_storage_key().into_owned_array());
        } else {
            snapshot.put_val(role_player.as_storage_key().into_owned_array(), encode_value_u64(count));
            snapshot.put_val(role_player_reverse.as_storage_key().into_owned_array(), encode_value_u64(reverse_count));
        }

        // must lock to fail concurrent transactions updating the same counters
        snapshot.exclusive_lock_add(role_player.into_storage_key().into_owned_array().into_byte_array());

        if self
            .type_manager
            .relation_index_available(snapshot, relation.type_())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?
        {
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
        let players = relation
            .get_players(snapshot, self)
            .collect_cloned_vec(|(roleplayer, _count)| (roleplayer.player().into_owned(), roleplayer.role_type()))
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
        let players = relation
            .get_players(snapshot, self)
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
