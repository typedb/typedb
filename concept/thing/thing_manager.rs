/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{Bound, HashMap, HashSet},
    iter::{once, Map},
    ops::RangeBounds,
    sync::Arc,
};

use bytes::{byte_array::ByteArray, util::increment, Bytes};
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeHasReverse, ThingEdgeIndexedRelation, ThingEdgeLinks},
            property::{build_object_vertex_property_has_order, build_object_vertex_property_links_order},
            vertex_attribute::{AttributeID, AttributeVertex},
            vertex_generator::ThingVertexGenerator,
            vertex_object::ObjectVertex,
            ThingVertex,
        },
        type_::{
            edge::TypeEdge,
            property::{TypeEdgeProperty, TypeVertexProperty, TypeVertexPropertyEncoding},
            vertex::{PrefixedTypeVertexEncoding, TypeID, TypeVertex, TypeVertexEncoding},
        },
        Typed,
    },
    layout::{infix::Infix, prefix::Prefix},
    value::{
        boolean_bytes::BooleanBytes,
        date_bytes::DateBytes,
        date_time_bytes::DateTimeBytes,
        date_time_tz_bytes::DateTimeTZBytes,
        decimal_bytes::DecimalBytes,
        double_bytes::DoubleBytes,
        duration_bytes::DurationBytes,
        integer_bytes::IntegerBytes,
        primitive_encoding::{decode_u64, encode_u64},
        string_bytes::StringBytes,
        struct_bytes::StructBytes,
        value::Value,
        value_struct::{StructIndexEntry, StructValue},
        value_type::{ValueType, ValueTypeCategory},
        ValueEncodable,
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};
use iterator::minmax_or;
use itertools::Itertools;
use lending_iterator::Peekable;
use primitive::either::Either;
use resource::{
    constants::{
        encoding::StructFieldIDUInt,
        snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
    },
    profile::StorageCounters,
};
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    snapshot::{lock::create_custom_lock_key, write::Write, ReadableSnapshot, WritableSnapshot},
};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    iterator::InstanceIterator,
    thing::{
        attribute::{Attribute, AttributeIterator},
        decode_attribute_ids, decode_role_players, encode_attribute_ids, encode_role_players,
        entity::Entity,
        has::Has,
        object::{HasIterator, HasReverseIterator, Object, ObjectAPI},
        r#struct::StructIndexForAttributeTypeIterator,
        relation::{IndexedRelationsIterator, LinksIterator, LinksReverseIterator, Relation, RolePlayer},
        statistics::Statistics,
        thing_manager::validation::{
            commit_time_validation::{collect_errors, CommitTimeValidation},
            operation_time_validation::OperationTimeValidation,
            DataValidationError,
        },
        ThingAPI,
    },
    type_::{
        annotation::{AnnotationCascade, AnnotationIndependent},
        attribute_type::AttributeType,
        constraint::{get_checked_constraints, Constraint},
        entity_type::EntityType,
        object_type::ObjectType,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::TypeManager,
        Capability, ObjectTypeAPI, OwnerAPI, PlayerAPI, TypeAPI,
    },
    ConceptStatus,
};

pub mod validation;

#[derive(Debug)]
pub struct ThingManager {
    vertex_generator: Arc<ThingVertexGenerator>,
    type_manager: Arc<TypeManager>,
    statistics: Arc<Statistics>,
}

impl ThingManager {
    pub fn new(
        vertex_generator: Arc<ThingVertexGenerator>,
        type_manager: Arc<TypeManager>,
        statistics: Arc<Statistics>,
    ) -> Self {
        ThingManager { vertex_generator, type_manager, statistics }
    }

    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }

    pub fn type_manager(&self) -> &TypeManager {
        &self.type_manager
    }

    /// Return simple iterator of all Concept(Vertex) found for a specific instantiable Type
    /// If this type is an Attribute type, this iterator will not hide the Dependent attributes that have no owners.
    fn get_instances_in<T: ThingAPI>(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_type: T::TypeAPI,
        keyspace: EncodingKeyspace,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<T> {
        if thing_type.is_abstract(snapshot, self.type_manager()).unwrap() {
            return InstanceIterator::empty();
        }

        let prefix = <T as ThingAPI>::prefix_for_type(thing_type);
        let storage_key_prefix =
            <T as ThingAPI>::Vertex::build_prefix_type(prefix, thing_type.vertex().type_id_(), keyspace);
        let snapshot_iterator = snapshot
            .iterate_range(&KeyRange::new_within(storage_key_prefix, prefix.fixed_width_keys()), storage_counters);
        InstanceIterator::new(snapshot_iterator)
    }

    fn get_instances<T: ThingAPI>(
        &self,
        keyspace: EncodingKeyspace,
        snapshot: &impl ReadableSnapshot,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<T> {
        let (prefix_start, prefix_end_exclusive) = T::PREFIX_RANGE_INCLUSIVE;
        let key_start = T::Vertex::build_prefix_prefix(prefix_start, keyspace);
        let key_end = T::Vertex::build_prefix_prefix(prefix_end_exclusive, keyspace);
        let snapshot_iterator = snapshot.iterate_range(
            &KeyRange::new_variable_width(RangeStart::Inclusive(key_start), RangeEnd::EndPrefixInclusive(key_end)),
            storage_counters,
        );
        InstanceIterator::new(snapshot_iterator)
    }

    fn get_instances_in_range<T: ThingAPI>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_range: &impl RangeBounds<T::TypeAPI>,
        keyspace: EncodingKeyspace,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<T> {
        let start = match Self::start_type_bound_to_range_start_included_type(type_range.start_bound()) {
            None => return InstanceIterator::empty(),
            Some(start_type_included) => <T as ThingAPI>::Vertex::build_prefix_type(
                <T as ThingAPI>::prefix_for_type(start_type_included),
                start_type_included.vertex().type_id_(),
                keyspace,
            ),
        };
        let end = match Self::end_type_bound_to_range_end_included_type(type_range.end_bound()) {
            None => return InstanceIterator::empty(),
            Some(end_type_included) => <T as ThingAPI>::Vertex::build_prefix_type(
                <T as ThingAPI>::prefix_for_type(end_type_included),
                end_type_included.vertex().type_id_(),
                keyspace,
            ),
        };
        let key_range = KeyRange::new(
            RangeStart::Inclusive(start),
            RangeEnd::EndPrefixInclusive(end),
            <T as ThingAPI>::Vertex::FIXED_WIDTH_ENCODING,
        );
        InstanceIterator::new(snapshot.iterate_range(&key_range, storage_counters))
    }

    pub fn get_instance<T: ThingAPI>(
        &self,
        snapshot: &impl ReadableSnapshot,
        iid: &Bytes<'_, BUFFER_KEY_INLINE>,
        storage_counters: StorageCounters,
    ) -> Result<Option<T>, Box<ConceptReadError>> {
        let Some(vertex) = T::Vertex::try_decode(iid) else {
            return Err(Box::new(ConceptReadError::IidRepresentsWrongInstanceKind {}));
        };
        let exists = snapshot
            .contains(StorageKeyReference::new(vertex.keyspace(), iid), storage_counters)
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?;
        Ok(exists.then_some(T::new(vertex)))
    }

    pub fn get_objects_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        object_type: ObjectType,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<Object> {
        self.get_instances_in(snapshot, object_type, <Object as ThingAPI>::Vertex::KEYSPACE, storage_counters)
    }

    pub fn get_objects_in_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        object_type_range: &impl RangeBounds<ObjectType>,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<Object> {
        self.get_instances_in_range(
            snapshot,
            object_type_range,
            <Object as ThingAPI>::Vertex::KEYSPACE,
            storage_counters,
        )
    }

    pub fn get_object(
        &self,
        snapshot: &impl ReadableSnapshot,
        iid: &Bytes<'_, BUFFER_KEY_INLINE>,
        storage_counters: StorageCounters,
    ) -> Result<Option<Object>, Box<ConceptReadError>> {
        self.get_instance::<Object>(snapshot, iid, storage_counters)
    }

    pub fn get_entities(
        &self,
        snapshot: &impl ReadableSnapshot,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<Entity> {
        self.get_instances::<Entity>(<Entity as ThingAPI>::Vertex::KEYSPACE, snapshot, storage_counters)
    }

    pub fn get_entities_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_: EntityType,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<Entity> {
        self.get_instances_in(snapshot, type_, <Entity as ThingAPI>::Vertex::KEYSPACE, storage_counters)
    }

    pub fn get_entities_in_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type_range: &impl RangeBounds<EntityType>,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<Entity> {
        self.get_instances_in_range(
            snapshot,
            entity_type_range,
            <Entity as ThingAPI>::Vertex::KEYSPACE,
            storage_counters,
        )
    }

    pub fn get_relations(
        &self,
        snapshot: &impl ReadableSnapshot,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<Relation> {
        self.get_instances::<Relation>(<Relation as ThingAPI>::Vertex::KEYSPACE, snapshot, storage_counters)
    }

    pub fn get_relations_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_: RelationType,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<Relation> {
        self.get_instances_in(snapshot, type_, <Relation as ThingAPI>::Vertex::KEYSPACE, storage_counters)
    }

    pub fn get_relations_in_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type_range: &impl RangeBounds<RelationType>,
        storage_counters: StorageCounters,
    ) -> InstanceIterator<Relation> {
        self.get_instances_in_range(
            snapshot,
            relation_type_range,
            <Relation as ThingAPI>::Vertex::KEYSPACE,
            storage_counters,
        )
    }

    pub fn get_attributes<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        storage_counters: StorageCounters,
    ) -> Result<impl Iterator<Item = Result<Attribute, Box<ConceptReadError>>>, Box<ConceptReadError>> {
        Ok(self
            .get_attributes_short(snapshot, storage_counters.clone())?
            .chain(self.get_attributes_long(snapshot, storage_counters)?))
    }

    pub fn get_attributes_short<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        storage_counters: StorageCounters,
    ) -> Result<AttributeIterator<InstanceIterator<Attribute>>, Box<ConceptReadError>> {
        let has_reverse_start = ThingEdgeHasReverse::prefix_from_prefix_short(Prefix::VertexAttribute);
        let range = KeyRange::new_within(has_reverse_start, Prefix::VertexAttribute.fixed_width_keys());
        let has_reverse_iterator = HasReverseIterator::new(snapshot.iterate_range(&range, storage_counters.clone()));
        Ok(AttributeIterator::new(
            self.get_instances::<Attribute>(AttributeVertex::keyspace_for_is_short(true), snapshot, storage_counters),
            has_reverse_iterator,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    pub fn get_attributes_long<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        storage_counters: StorageCounters,
    ) -> Result<AttributeIterator<InstanceIterator<Attribute>>, Box<ConceptReadError>> {
        let has_reverse_start = ThingEdgeHasReverse::prefix_from_prefix_long(Prefix::VertexAttribute);
        let range = KeyRange::new_within(has_reverse_start, Prefix::VertexAttribute.fixed_width_keys());
        let has_reverse_iterator = HasReverseIterator::new(snapshot.iterate_range(&range, storage_counters.clone()));
        Ok(AttributeIterator::new(
            self.get_instances::<Attribute>(AttributeVertex::keyspace_for_is_short(false), snapshot, storage_counters),
            has_reverse_iterator,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    pub fn get_attributes_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
        storage_counters: StorageCounters,
    ) -> Result<AttributeIterator<InstanceIterator<Attribute>>, Box<ConceptReadError>> {
        let attribute_value_type =
            attribute_type.get_value_type_without_source(snapshot, self.type_manager.as_ref())?;
        let Some(value_type) = attribute_value_type.as_ref() else {
            return Ok(AttributeIterator::new_empty());
        };

        let has_reverse_prefix =
            ThingEdgeHasReverse::prefix_from_attribute_type(value_type.category(), attribute_type.vertex().type_id_());
        let range = KeyRange::new_within(has_reverse_prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        let has_reverse_iterator = HasReverseIterator::new(snapshot.iterate_range(&range, storage_counters.clone()));
        Ok(AttributeIterator::new(
            self.get_instances_in(
                snapshot,
                attribute_type,
                AttributeVertex::keyspace_for_category(value_type.category()),
                storage_counters,
            ),
            has_reverse_iterator,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    pub fn get_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        iid: &Bytes<'_, BUFFER_KEY_INLINE>,
        storage_counters: StorageCounters,
    ) -> Result<Option<Attribute>, Box<ConceptReadError>> {
        self.get_instance::<Attribute>(snapshot, iid, storage_counters)
    }

    pub(crate) fn get_attribute_value(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: &Attribute,
        storage_counters: StorageCounters,
    ) -> Result<Value<'static>, Box<ConceptReadError>> {
        match attribute.vertex().attribute_id() {
            AttributeID::Boolean(id) => Ok(Value::Boolean(id.read().as_bool())),
            AttributeID::Integer(id) => Ok(Value::Integer(id.read().as_i64())),
            AttributeID::Double(id) => Ok(Value::Double(id.read().as_f64())),
            AttributeID::Decimal(id) => Ok(Value::Decimal(id.read().as_decimal())),
            AttributeID::Date(id) => Ok(Value::Date(id.read().as_naive_date())),
            AttributeID::DateTime(id) => Ok(Value::DateTime(id.read().as_naive_date_time())),
            AttributeID::DateTimeTZ(id) => Ok(Value::DateTimeTZ(id.read().as_date_time())),
            AttributeID::Duration(id) => Ok(Value::Duration(id.read().as_duration())),
            AttributeID::String(id) => {
                let string = if id.is_inline() {
                    String::from(id.get_inline_id_value().as_str())
                } else {
                    snapshot
                        .get_mapped(
                            attribute.vertex().into_storage_key().as_reference(),
                            |bytes| String::from(StringBytes::new(Bytes::<1>::Reference(bytes)).as_str()),
                            storage_counters,
                        )
                        .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))?
                        .ok_or(ConceptReadError::InternalMissingAttributeValue {})?
                };
                Ok(Value::String(Cow::Owned(string)))
            }
            AttributeID::Struct(_id) => {
                let struct_value = snapshot
                    .get_mapped(
                        attribute.vertex().into_storage_key().as_reference(),
                        |bytes| StructBytes::new(Bytes::<1>::Reference(bytes)).as_struct(),
                        storage_counters,
                    )
                    .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))?
                    .ok_or(ConceptReadError::InternalMissingAttributeValue {})?;
                Ok(Value::Struct(Cow::Owned(struct_value)))
            }
        }
    }

    pub fn get_attribute_with_value(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
        value: Value<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<Attribute>, Box<ConceptReadError>> {
        let value_type = value.value_type();
        let attribute_value_type = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?;
        if attribute_value_type.is_none() || attribute_value_type.as_ref().unwrap() != &value_type {
            return Ok(None);
        }

        let attribute = match value_type {
            | ValueType::Boolean
            | ValueType::Integer
            | ValueType::Double
            | ValueType::Decimal
            | ValueType::Date
            | ValueType::DateTime
            | ValueType::DateTimeTZ
            | ValueType::Duration => {
                debug_assert!(AttributeID::is_inlineable(value.as_reference()));
                match self.get_attribute_with_value_inline(snapshot, attribute_type, value, storage_counters) {
                    Ok(Some(attribute)) => attribute,
                    fail => return fail,
                }
            }
            ValueType::String => {
                if AttributeID::is_inlineable(value.as_reference()) {
                    match self.get_attribute_with_value_inline(snapshot, attribute_type, value, storage_counters) {
                        Ok(Some(attribute)) => attribute,
                        fail => return fail,
                    }
                } else {
                    match self.vertex_generator.find_attribute_id_string_noinline(
                        attribute_type.vertex().type_id_(),
                        value.encode_string::<256>(),
                        snapshot,
                    ) {
                        Ok(Some(id)) => Attribute::new(AttributeVertex::new(
                            attribute_type.vertex().type_id_(),
                            AttributeID::String(id),
                        )),
                        Ok(None) => return Ok(None),
                        Err(err) => return Err(Box::new(ConceptReadError::SnapshotIterate { source: err })),
                    }
                }
            }
            ValueType::Struct(_) => {
                match self.vertex_generator.find_attribute_id_struct(
                    attribute_type.vertex().type_id_(),
                    value.encode_struct::<256>(),
                    snapshot,
                ) {
                    Ok(Some(id)) => Attribute::new(AttributeVertex::new(
                        attribute_type.vertex().type_id_(),
                        AttributeID::Struct(id),
                    )),
                    Ok(None) => return Ok(None),
                    Err(err) => return Err(Box::new(ConceptReadError::SnapshotIterate { source: err })),
                }
            }
        };

        Ok(Some(attribute))
    }

    pub fn get_attributes_in_range<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
        value_range: &'a impl RangeBounds<Value<'a>>,
        storage_counters: StorageCounters,
    ) -> Result<AttributeIterator<InstanceIterator<Attribute>>, Box<ConceptReadError>> {
        if matches!(value_range.start_bound(), Bound::Unbounded) && matches!(value_range.end_bound(), Bound::Unbounded)
        {
            return self.get_attributes_in(snapshot, attribute_type, storage_counters);
        }
        let Some(attribute_value_type) = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?
        else {
            return Ok(AttributeIterator::new_empty());
        };

        let Some((value_lower_bound, value_upper_bound)) =
            Self::get_value_range(attribute_value_type.category(), value_range)
        else {
            return Ok(AttributeIterator::new_empty());
        };
        let start_attribute_vertex_bound = self.get_attribute_vertex_prefix_lower_bound(
            attribute_type.vertex().type_id_(),
            attribute_value_type.category(),
            value_lower_bound,
        );
        let end_attribute_vertex_bound = self.get_attribute_vertex_prefix_upper_bound(
            attribute_type.vertex().type_id_(),
            attribute_value_type.category(),
            value_upper_bound,
        );

        let has_reverse_start_prefix = start_attribute_vertex_bound.map(|start| {
            ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(attribute_value_type.category(), start.bytes())
        });
        let has_reverse_end_prefix = end_attribute_vertex_bound.map(|end| {
            ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(
                attribute_value_type.category(),
                end.as_reference().bytes(),
            )
        });

        let range = KeyRange::new_variable_width(start_attribute_vertex_bound, end_attribute_vertex_bound);
        let attributes_iterator = InstanceIterator::new(snapshot.iterate_range(&range, storage_counters.clone()));
        let has_reverse_range = KeyRange::new_variable_width(has_reverse_start_prefix, has_reverse_end_prefix);
        let has_reverse_iterator =
            HasReverseIterator::new(snapshot.iterate_range(&has_reverse_range, storage_counters));
        Ok(AttributeIterator::new(
            attributes_iterator,
            has_reverse_iterator,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    fn get_attribute_vertex_prefix_lower_bound(
        &self,
        attribute_type_id: TypeID,
        attribute_value_type_category: ValueTypeCategory,
        value_lower_bound: Bound<Value<'_>>,
    ) -> RangeStart<StorageKey<'static, BUFFER_KEY_INLINE>> {
        match value_lower_bound {
            Bound::Included(lower_value) => {
                let vertex_or_prefix = AttributeVertex::build_or_prefix_for_value(
                    attribute_type_id,
                    lower_value,
                    self.vertex_generator.hasher(),
                );
                let storage_key_prefix = match vertex_or_prefix {
                    Either::First(vertex) => vertex.into_storage_key(),
                    Either::Second(incomplete_attribute_prefix) => incomplete_attribute_prefix,
                };
                RangeStart::Inclusive(storage_key_prefix)
            }
            Bound::Excluded(lower_value) => {
                let vertex_or_prefix = AttributeVertex::build_or_prefix_for_value(
                    attribute_type_id,
                    lower_value,
                    self.vertex_generator.hasher(),
                );
                let storage_key_prefix = match vertex_or_prefix {
                    Either::First(vertex) => vertex.into_storage_key(),
                    Either::Second(incomplete_attribute_prefix) => incomplete_attribute_prefix,
                };
                RangeStart::ExcludePrefix(storage_key_prefix)
            }
            Bound::Unbounded => RangeStart::Inclusive(
                AttributeVertex::build_prefix_type(
                    AttributeVertex::PREFIX,
                    attribute_type_id,
                    AttributeVertex::keyspace_for_category(attribute_value_type_category),
                )
                .resize_to(),
            ),
        }
    }

    fn get_attribute_vertex_prefix_upper_bound(
        &self,
        attribute_type_id: TypeID,
        attribute_value_type_category: ValueTypeCategory,
        value_upper_bound: Bound<Value<'_>>,
    ) -> RangeEnd<StorageKey<'static, BUFFER_KEY_INLINE>> {
        match value_upper_bound {
            Bound::Included(upper_value) => {
                let vertex_or_prefix = AttributeVertex::build_or_prefix_for_value(
                    attribute_type_id,
                    upper_value,
                    self.vertex_generator.hasher(),
                );
                let storage_key_prefix = match vertex_or_prefix {
                    Either::First(vertex) => vertex.into_storage_key(),
                    Either::Second(incomplete_attribute_prefix) => incomplete_attribute_prefix,
                };
                RangeEnd::EndPrefixInclusive(storage_key_prefix)
            }
            Bound::Excluded(upper_value) => {
                let vertex_or_prefix = AttributeVertex::build_or_prefix_for_value(
                    attribute_type_id,
                    upper_value,
                    self.vertex_generator.hasher(),
                );
                let storage_key_prefix = match vertex_or_prefix {
                    Either::First(vertex) => vertex.into_storage_key(),
                    Either::Second(incomplete_attribute_prefix) => incomplete_attribute_prefix,
                };
                RangeEnd::EndPrefixExclusive(storage_key_prefix)
            }
            Bound::Unbounded => {
                let prefix = AttributeVertex::build_prefix_type(
                    AttributeVertex::PREFIX,
                    attribute_type_id,
                    AttributeVertex::keyspace_for_category(attribute_value_type_category),
                );
                let keyspace = prefix.keyspace_id();
                let mut array = prefix.into_bytes().into_array();
                array.increment().unwrap();
                let prefix_key = StorageKey::Array(StorageKeyArray::new_raw(keyspace, array));
                RangeEnd::EndPrefixExclusive(prefix_key.resize_to())
            }
        }
    }

    fn get_attribute_with_value_inline(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
        value: Value<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<Attribute>, Box<ConceptReadError>> {
        debug_assert!(AttributeID::is_inlineable(value.as_reference()));
        let attribute_value_type = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?;
        if attribute_value_type.is_none() || attribute_value_type.as_ref().unwrap() != &value.value_type() {
            return Ok(None);
        }
        let vertex = AttributeVertex::new(attribute_type.vertex().type_id_(), AttributeID::build_inline(value));
        snapshot
            .get_mapped(vertex.into_storage_key().as_reference(), |_| Attribute::new(vertex), storage_counters)
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))
    }

    pub(crate) fn get_player_relations_roles(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: impl ObjectAPI,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<(Relation, RoleType, u64), Box<ConceptReadError>>> + 'static {
        let prefix = ThingEdgeLinks::prefix_reverse_from_player(player.vertex());
        Iterator::map(
            LinksReverseIterator::new(snapshot.iterate_range(
                &KeyRange::new_within(prefix, ThingEdgeLinks::FIXED_WIDTH_ENCODING_REVERSE),
                storage_counters,
            )),
            |result| {
                result.map(|(links, count)| {
                    let (relation, role) = links.into_relation_role();
                    (relation, role, count)
                })
            },
        )
    }

    pub(crate) fn get_player_relations(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: impl ObjectAPI,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<Relation, Box<ConceptReadError>>> {
        self.get_player_relations_roles(snapshot, player, storage_counters).map::<Result<Relation, _>, _>(|res| {
            let (rel, _, _) = res?;
            Ok(rel)
        })
    }

    pub(crate) fn get_player_relations_using_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: impl ObjectAPI,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<(Relation, u64), Box<ConceptReadError>>> {
        self.get_player_relations_roles(snapshot, player, storage_counters).filter_map::<Result<(Relation, u64), _>, _>(
            move |item| match item {
                Ok((rel, role, count)) => (role == role_type).then_some(Ok((rel, count))),
                Err(error) => Some(Err(error)),
            },
        )
    }

    pub(crate) fn owner_has_attribute_with_value(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        value: Value<'_>,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        let value_type = value.value_type();
        OperationTimeValidation::validate_value_type_matches_attribute_type_for_read(
            snapshot,
            self,
            attribute_type,
            value_type.clone(),
        )?;

        let vertex = if AttributeID::is_inlineable(value.as_reference()) {
            // don't need to do an extra lookup to get the attribute vertex - if it exists, it will have this ID
            AttributeVertex::new(attribute_type.vertex().type_id_(), AttributeID::build_inline(value))
        } else {
            // non-inline attributes require an extra lookup before checking for the has edge existence
            let attribute = self.get_attribute_with_value(snapshot, attribute_type, value, storage_counters.clone())?;
            match attribute {
                Some(attribute) => attribute.vertex(),
                None => return Ok(false),
            }
        };

        let has = ThingEdgeHas::new(owner.vertex(), vertex);
        let has_exists = snapshot
            .get_mapped(has.into_storage_key().as_reference(), |_value| true, storage_counters)
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or(false);
        Ok(has_exists)
    }

    pub(crate) fn owner_has_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: impl ObjectAPI,
        attribute: &Attribute,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        let has = ThingEdgeHas::new(owner.vertex(), attribute.vertex());
        let has_exists = snapshot
            .get_mapped(has.into_storage_key().as_reference(), |_value| true, storage_counters)
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or(false);
        Ok(has_exists)
    }

    pub fn get_has_from_owner_type_range_unordered(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner_type_range: &impl RangeBounds<ObjectType>,
        storage_counters: StorageCounters,
    ) -> HasIterator {
        let start = match Self::start_type_bound_to_range_start_included_type(owner_type_range.start_bound()) {
            None => return HasIterator::new_empty(),
            Some(start_type_included) => ThingEdgeHas::prefix_from_type(start_type_included.vertex()),
        };
        let end = match Self::end_type_bound_to_range_end_included_type(owner_type_range.end_bound()) {
            None => return HasIterator::new_empty(),
            Some(end_type_included) => ThingEdgeHas::prefix_from_type(end_type_included.vertex()),
        };
        let key_range = KeyRange::new(
            RangeStart::Inclusive(start),
            RangeEnd::EndPrefixInclusive(end),
            ThingEdgeHas::FIXED_WIDTH_ENCODING,
        );
        HasIterator::new(snapshot.iterate_range(&key_range, storage_counters))
    }

    pub fn get_has_reverse(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
        storage_counters: StorageCounters,
    ) -> Result<HasReverseIterator, Box<ConceptReadError>> {
        let Some(value_type) = attribute_type.get_value_type_without_source(snapshot, self.type_manager())? else {
            return Ok(HasReverseIterator::new_empty());
        };
        let prefix =
            ThingEdgeHasReverse::prefix_from_attribute_type(value_type.category(), attribute_type.vertex().type_id_());
        let range = KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        Ok(HasReverseIterator::new(snapshot.iterate_range(&range, storage_counters)))
    }

    /// Given an attribute type, and a range of values, return an iterator of Has where the owners satisfy this range (best effort)
    /// For inlineable values, this range will be fully respected, and for large values, it is an approximation and should still be checked afterward
    /// The Owner types range hint is useful in particular when 1 Inlinable Value is provided, allowing constructing the exact
    /// range [att type][att vertex][start owner type] --> [att type][att vertex][end owner type]
    /// However, it is in general only a hint used to constrain the start prefix and end of the range. When multiple values are matched,
    /// the Has's returned will likely contain Owner types _not_ in the indicated range.
    pub fn get_has_reverse_in_range<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
        range: &'a impl RangeBounds<Value<'a>>,
        owner_types_range_hint: &impl RangeBounds<ObjectType>,
        storage_counters: StorageCounters,
    ) -> Result<HasReverseIterator, Box<ConceptReadError>> {
        if matches!(range.start_bound(), Bound::Unbounded) && matches!(range.end_bound(), Bound::Unbounded) {
            return self.get_has_reverse(snapshot, attribute_type, storage_counters);
        }
        let Some(attribute_value_type) = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?
        else {
            return Ok(HasReverseIterator::new_empty());
        };

        let Some((value_lower_bound, value_upper_bound)) =
            Self::get_value_range(attribute_value_type.category(), range)
        else {
            return Ok(HasReverseIterator::new_empty());
        };

        let has_range_start = match value_lower_bound {
            Bound::Included(lower_value) => {
                let vertex_or_prefix = AttributeVertex::build_or_prefix_for_value(
                    attribute_type.vertex().type_id_(),
                    lower_value,
                    self.vertex_generator.hasher(),
                );
                match vertex_or_prefix {
                    Either::First(vertex) => {
                        match owner_types_range_hint.start_bound() {
                            Bound::Included(start) => {
                                let start_type = start.vertex();
                                RangeStart::Inclusive(ThingEdgeHasReverse::prefix_from_attribute_to_type(
                                    vertex, start_type,
                                ))
                            }
                            Bound::Excluded(start) => {
                                // increment and treat as included
                                let mut bytes: [u8; TypeVertex::LENGTH] =
                                    start.vertex().to_bytes().as_ref().try_into().unwrap();
                                increment(&mut bytes).unwrap();
                                let start_type = TypeVertex::decode(Bytes::Reference(&bytes));
                                RangeStart::Inclusive(ThingEdgeHasReverse::prefix_from_attribute_to_type(
                                    vertex, start_type,
                                ))
                            }
                            Bound::Unbounded => {
                                RangeStart::Inclusive(ThingEdgeHasReverse::prefix_from_attribute(vertex).resize_to())
                            }
                        }
                    }
                    Either::Second(prefix) => {
                        // attribute vertex could not be built fully, probably due to not being an inline-valued attribute
                        RangeStart::Inclusive(
                            ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(
                                attribute_value_type.category(),
                                prefix.bytes(),
                            )
                            .resize_to(),
                        )
                    }
                }
            }
            Bound::Excluded(lower_value) => {
                let vertex_or_prefix = AttributeVertex::build_or_prefix_for_value(
                    attribute_type.vertex().type_id_(),
                    lower_value,
                    self.vertex_generator.hasher(),
                );
                match vertex_or_prefix {
                    Either::First(vertex) => {
                        // trick: increment the vertex, since it is complete, then concat the next type - this will help
                        // with hitting the bloom filters
                        let storage_key = vertex.into_storage_key();
                        let mut byte_array = storage_key.into_owned_array().into_byte_array();
                        byte_array.increment().unwrap();
                        let next_attribute = AttributeVertex::decode(&byte_array);
                        match owner_types_range_hint.start_bound() {
                            Bound::Included(start) => {
                                let start_type = start.vertex();
                                RangeStart::Inclusive(ThingEdgeHasReverse::prefix_from_attribute_to_type(
                                    next_attribute,
                                    start_type,
                                ))
                            }
                            Bound::Excluded(start) => {
                                // increment and treat as included
                                let mut bytes: [u8; TypeVertex::LENGTH] =
                                    start.vertex().to_bytes().as_ref().try_into().unwrap();
                                increment(&mut bytes).unwrap();
                                let start_type = TypeVertex::decode(Bytes::Reference(&bytes));
                                RangeStart::Inclusive(ThingEdgeHasReverse::prefix_from_attribute_to_type(
                                    next_attribute,
                                    start_type,
                                ))
                            }
                            Bound::Unbounded => RangeStart::Inclusive(
                                ThingEdgeHasReverse::prefix_from_attribute(next_attribute).resize_to(),
                            ),
                        }
                    }
                    Either::Second(prefix) => {
                        // since this is not a complete vertex, and only a prefix, we shouldn't make assumptions about incrementing
                        // to get to value + 1 in sort order
                        RangeStart::Inclusive(
                            ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(
                                attribute_value_type.category(),
                                prefix.bytes(),
                            )
                            .resize_to(),
                        )
                    }
                }
            }
            Bound::Unbounded => RangeStart::Inclusive(
                ThingEdgeHasReverse::prefix_from_attribute_type(
                    attribute_value_type.category(),
                    attribute_type.vertex().type_id_(),
                )
                .resize_to(),
            ),
        };

        let has_range_end = match value_upper_bound {
            Bound::Included(upper_value) => {
                let vertex_or_prefix = AttributeVertex::build_or_prefix_for_value(
                    attribute_type.vertex().type_id_(),
                    upper_value,
                    self.vertex_generator.hasher(),
                );
                match vertex_or_prefix {
                    Either::First(vertex) => match owner_types_range_hint.end_bound() {
                        Bound::Included(end) => {
                            let end_type = end.vertex();
                            RangeEnd::EndPrefixInclusive(ThingEdgeHasReverse::prefix_from_attribute_to_type(
                                vertex, end_type,
                            ))
                        }
                        Bound::Excluded(end) => {
                            let end_type = end.vertex();
                            RangeEnd::EndPrefixExclusive(ThingEdgeHasReverse::prefix_from_attribute_to_type(
                                vertex, end_type,
                            ))
                        }
                        Bound::Unbounded => {
                            RangeEnd::EndPrefixInclusive(ThingEdgeHasReverse::prefix_from_attribute(vertex).resize_to())
                        }
                    },
                    Either::Second(prefix) => RangeEnd::EndPrefixInclusive(
                        ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(
                            attribute_value_type.category(),
                            prefix.bytes(),
                        )
                        .resize_to(),
                    ),
                }
            }
            Bound::Excluded(upper_value) => {
                let vertex_or_prefix = AttributeVertex::build_or_prefix_for_value(
                    attribute_type.vertex().type_id_(),
                    upper_value,
                    self.vertex_generator.hasher(),
                );
                match vertex_or_prefix {
                    Either::First(vertex) => {
                        RangeEnd::EndPrefixExclusive(ThingEdgeHasReverse::prefix_from_attribute(vertex).resize_to())
                    }
                    Either::Second(prefix) => RangeEnd::EndPrefixExclusive(
                        ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(
                            attribute_value_type.category(),
                            prefix.bytes(),
                        )
                        .resize_to(),
                    ),
                }
            }
            Bound::Unbounded => RangeEnd::EndPrefixInclusive(
                ThingEdgeHasReverse::prefix_from_attribute_type(
                    attribute_value_type.category(),
                    attribute_type.vertex().type_id_(),
                )
                .resize_to(),
            ),
        };
        let key_range = KeyRange::new(has_range_start, has_range_end, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        Ok(HasReverseIterator::new(snapshot.iterate_range(&key_range, storage_counters)))
    }

    pub fn get_attributes_by_struct_field<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        attribute_type: AttributeType,
        path_to_field: Vec<StructFieldIDUInt>,
        value: Value<'_>,
        storage_counters: StorageCounters,
    ) -> Result<impl Iterator<Item = Result<Attribute, Box<ConceptReadError>>>, Box<ConceptReadError>> {
        debug_assert!({
            let value_type =
                attribute_type.get_value_type_without_source(snapshot, &self.type_manager).unwrap().unwrap();
            value_type.category() == ValueTypeCategory::Struct
        });

        let index_attribute_iterator = Peekable::new(StructIndexForAttributeTypeIterator::new(
            snapshot,
            &self.vertex_generator,
            attribute_type,
            &path_to_field,
            value.as_reference(),
        )?);
        let has_reverse_prefix = ThingEdgeHasReverse::prefix_from_attribute_type(
            ValueTypeCategory::Struct,
            attribute_type.vertex().type_id_(),
        );
        let range = KeyRange::new_within(has_reverse_prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        let has_reverse_iterator = HasReverseIterator::new(snapshot.iterate_range(&range, storage_counters));
        let iter = AttributeIterator::new(
            index_attribute_iterator,
            has_reverse_iterator,
            self.type_manager.get_independent_attribute_types(snapshot)?,
        );
        Ok(iter)
    }

    pub(crate) fn owner_get_has_unordered_all(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: impl ObjectAPI,
        storage_counters: StorageCounters,
    ) -> Result<HasIterator, Box<ConceptReadError>> {
        let prefix = ThingEdgeHas::prefix_from_object(owner.vertex());
        let key_range = KeyRange::new_within(prefix, ThingEdgeHas::FIXED_WIDTH_ENCODING);
        Ok(HasIterator::new(snapshot.iterate_range(&key_range, storage_counters)))
    }

    pub(crate) fn owner_get_has_unordered_in_value_type<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: impl ObjectAPI,
        attribute_type_range_hint: &impl RangeBounds<AttributeType>,
        value_type_categories: &[ValueTypeCategory],
        value_range: &'a impl RangeBounds<Value<'a>>,
        storage_counters: StorageCounters,
    ) -> Result<HasIterator, Box<ConceptReadError>> {
        let start_attribute_type =
            match Self::start_type_bound_to_range_start_included_type(attribute_type_range_hint.start_bound()) {
                None => return Ok(HasIterator::new_empty()),
                Some(start_type_included) => start_type_included,
            };
        let start_value_bound = match Self::get_value_lower_bound_across_types(value_type_categories, value_range) {
            None => return Ok(HasIterator::new_empty()),
            Some(lower_bound) => lower_bound,
        };
        let start = self.get_has_from_thing_to_type_unordered_start_bound(
            owner,
            start_attribute_type.vertex().type_id_(),
            start_value_bound,
        );

        let end_attribute_type =
            match Self::end_type_bound_to_range_end_included_type(attribute_type_range_hint.end_bound()) {
                None => return Ok(HasIterator::new_empty()),
                Some(end_type_included) => end_type_included,
            };
        let end_value_bound = match Self::get_value_upper_bound_across_types(value_type_categories, value_range) {
            None => return Ok(HasIterator::new_empty()),
            Some(upper_bound) => upper_bound,
        };
        let end = self.get_has_from_thing_to_type_unordered_end_bound(
            owner,
            end_attribute_type.vertex().type_id_(),
            end_value_bound,
        );
        let key_range = KeyRange::new(start, end, ThingEdgeHas::FIXED_WIDTH_ENCODING);
        Ok(HasIterator::new(snapshot.iterate_range(&key_range, storage_counters)))
    }

    pub(crate) fn get_has_from_thing_to_type_unordered<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        value_range: &'a impl RangeBounds<Value<'a>>,
        storage_counters: StorageCounters,
    ) -> Result<
        Map<
            HasIterator,
            fn(Result<(Has, u64), Box<ConceptReadError>>) -> Result<(Attribute, u64), Box<ConceptReadError>>,
        >,
        Box<ConceptReadError>,
    > {
        let attribute_value_type = match attribute_type.get_value_type_without_source(snapshot, self.type_manager())? {
            None => {
                return Ok(Iterator::map(
                    HasIterator::new_empty(),
                    |result: Result<(Has, u64), Box<ConceptReadError>>| {
                        result.map(|(has, value)| (has.attribute(), value))
                    },
                ));
            }
            Some(value_type) => value_type,
        };
        let Some((value_lower_bound, value_upper_bound)) =
            Self::get_value_range(attribute_value_type.category(), value_range)
        else {
            return Ok(Iterator::map(HasIterator::new_empty(), |result: Result<(Has, u64), Box<ConceptReadError>>| {
                result.map(|(has, value)| (has.attribute(), value))
            }));
        };
        let has_start_bound = self.get_has_from_thing_to_type_unordered_start_bound(
            owner,
            attribute_type.vertex().type_id_(),
            value_lower_bound,
        );
        let has_end_bound = self.get_has_from_thing_to_type_unordered_end_bound(
            owner,
            attribute_type.vertex().type_id_(),
            value_upper_bound,
        );
        let range = KeyRange::new(has_start_bound, has_end_bound, ThingEdgeHas::FIXED_WIDTH_ENCODING);
        Ok(Iterator::map(
            HasIterator::new(snapshot.iterate_range(&range, storage_counters)),
            |result: Result<(Has, u64), Box<ConceptReadError>>| result.map(|(has, value)| (has.attribute(), value)),
        ))
    }

    fn get_has_from_thing_to_type_unordered_start_bound(
        &self,
        owner: impl ObjectAPI,
        attribute_type_id: TypeID,
        value_lower_bound: Bound<Value<'_>>,
    ) -> RangeStart<StorageKey<'static, BUFFER_KEY_INLINE>> {
        let attribute_vertex_lower_bound = self.get_attribute_vertex_prefix_lower_bound(
            attribute_type_id,
            // ### DUMMY - IRRELEVANT - keyspace determined by Has edge later ###
            ValueTypeCategory::Boolean,
            // ### DUMMY - IRRELEVANT ###
            value_lower_bound,
        );
        attribute_vertex_lower_bound.map(|lower_bound| {
            ThingEdgeHas::prefix_from_object_to_type_with_attribute_prefix(owner.vertex(), lower_bound.bytes())
                .resize_to()
        })
    }

    fn get_has_from_thing_to_type_unordered_end_bound(
        &self,
        owner: impl ObjectAPI,
        attribute_type_id: TypeID,
        value_upper_bound: Bound<Value<'_>>,
    ) -> RangeEnd<StorageKey<'static, BUFFER_KEY_INLINE>> {
        let attribute_vertex_upper_bound = self.get_attribute_vertex_prefix_upper_bound(
            attribute_type_id,
            // ### DUMMY - IRRELEVANT - keyspace determined by Has edge later ###
            ValueTypeCategory::Boolean,
            // ### DUMMY - IRRELEVANT ###
            value_upper_bound,
        );
        attribute_vertex_upper_bound.map(|end| {
            ThingEdgeHas::prefix_from_object_to_type_with_attribute_prefix(owner.vertex(), end.bytes()).resize_to()
        })
    }

    pub(crate) fn get_has_from_thing_to_type_ordered(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        storage_counters: StorageCounters,
    ) -> Result<Vec<Attribute>, Box<ConceptReadError>> {
        let key = build_object_vertex_property_has_order(owner.vertex(), attribute_type.vertex());
        let attribute_value_type = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?;
        let value_type = match attribute_value_type.as_ref() {
            None => return Ok(Vec::new()),
            Some(value_type) => value_type,
        };
        let attributes = snapshot
            .get_mapped(
                key.into_storage_key().as_reference(),
                |bytes| {
                    decode_attribute_ids(value_type.category(), bytes)
                        .map(|id| Attribute::new(AttributeVertex::new(attribute_type.vertex().type_id_(), id)))
                        .collect()
                },
                storage_counters,
            )
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or_else(Vec::new);
        Ok(attributes)
    }

    pub(crate) fn get_owners(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: &Attribute,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<(Object, u64), Box<ConceptReadError>>> {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute.vertex());
        Iterator::map(
            HasReverseIterator::new(snapshot.iterate_range(
                &KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING),
                storage_counters,
            )),
            |result| result.map(|(has, count)| (has.owner(), count)),
        )
    }

    pub(crate) fn get_owners_by_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: &Attribute,
        owner_type: impl ObjectTypeAPI,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<(Object, u64), Box<ConceptReadError>>> {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute_to_type(attribute.vertex(), owner_type.vertex());
        Iterator::map(
            HasReverseIterator::new(snapshot.iterate_range(
                &KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING),
                storage_counters,
            )),
            |result| result.map(|(has, count)| (has.owner(), count)),
        )
    }

    pub fn get_has_reverse_by_attribute_and_owner_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: &Attribute,
        owner_type_range: &impl RangeBounds<ObjectType>,
        storage_counters: StorageCounters,
    ) -> HasReverseIterator {
        let range_start = match owner_type_range.start_bound() {
            Bound::Included(owner_start) => RangeStart::Inclusive(ThingEdgeHasReverse::prefix_from_attribute_to_type(
                attribute.vertex(),
                owner_start.vertex(),
            )),
            Bound::Excluded(owner_start) => RangeStart::ExcludePrefix(
                ThingEdgeHasReverse::prefix_from_attribute_to_type(attribute.vertex(), owner_start.vertex()),
            ),
            Bound::Unbounded => RangeStart::Inclusive(ThingEdgeHasReverse::prefix_from_attribute_to_type_parts(
                attribute.vertex(),
                Prefix::min_object_type_prefix(),
                TypeID::MIN,
            )),
        };
        let range_end = match owner_type_range.end_bound() {
            Bound::Included(owner_end) => RangeEnd::EndPrefixInclusive(
                ThingEdgeHasReverse::prefix_from_attribute_to_type(attribute.vertex(), owner_end.vertex()),
            ),
            Bound::Excluded(owner_end) => RangeEnd::EndPrefixExclusive(
                ThingEdgeHasReverse::prefix_from_attribute_to_type(attribute.vertex(), owner_end.vertex()),
            ),
            Bound::Unbounded => RangeEnd::EndPrefixInclusive(ThingEdgeHasReverse::prefix_from_attribute_to_type_parts(
                attribute.vertex(),
                Prefix::max_object_type_prefix(),
                TypeID::MAX,
            )),
        };
        let key_range = KeyRange::new(range_start, range_end, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        HasReverseIterator::new(snapshot.iterate_range(&key_range, storage_counters))
    }

    pub(crate) fn has_owners(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: &Attribute,
        buffered_only: bool,
    ) -> bool {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute.vertex());
        snapshot.any_in_range(&KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING), buffered_only)
    }

    pub(crate) fn has_links(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        buffered_only: bool, // FIXME use enums
    ) -> bool {
        let prefix = ThingEdgeLinks::prefix_from_relation(relation.vertex());
        snapshot.any_in_range(&KeyRange::new_within(prefix, ThingEdgeLinks::FIXED_WIDTH_ENCODING), buffered_only)
    }

    pub fn get_links_by_relation_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type_range: &impl RangeBounds<RelationType>,
        storage_counters: StorageCounters,
    ) -> LinksIterator {
        let start = match Self::start_type_bound_to_range_start_included_type(relation_type_range.start_bound()) {
            None => return LinksIterator::new_empty(),
            Some(start_type) => ThingEdgeLinks::prefix_from_relation_type(start_type.vertex().type_id_()),
        };
        let end = match Self::end_type_bound_to_range_end_included_type(relation_type_range.end_bound()) {
            None => return LinksIterator::new_empty(),
            Some(end_type) => ThingEdgeLinks::prefix_from_relation_type(end_type.vertex().type_id_()),
        };
        LinksIterator::new(snapshot.iterate_range(
            &KeyRange::new(
                RangeStart::Inclusive(start),
                RangeEnd::EndPrefixInclusive(end),
                ThingEdgeLinks::FIXED_WIDTH_ENCODING,
            ),
            storage_counters,
        ))
    }

    pub fn get_links_by_relation_and_player_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        player_type_range: &impl RangeBounds<ObjectType>,
        storage_counters: StorageCounters,
    ) -> LinksIterator {
        let start = match Self::start_type_bound_to_range_start_included_type(player_type_range.start_bound()) {
            None => return LinksIterator::new_empty(),
            Some(start_type) => {
                ThingEdgeLinks::prefix_from_relation_player_type(relation.vertex(), start_type.vertex())
            }
        };
        let end = match Self::end_type_bound_to_range_end_included_type(player_type_range.end_bound()) {
            None => return LinksIterator::new_empty(),
            Some(end_type) => ThingEdgeLinks::prefix_from_relation_player_type(relation.vertex(), end_type.vertex()),
        };
        let key_range = KeyRange::new(
            RangeStart::Inclusive(start),
            RangeEnd::EndPrefixInclusive(end),
            ThingEdgeLinks::FIXED_WIDTH_ENCODING,
        );
        LinksIterator::new(snapshot.iterate_range(&key_range, storage_counters))
    }

    pub fn get_links_by_relation_and_player(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        storage_counters: StorageCounters,
    ) -> LinksIterator {
        let prefix = ThingEdgeLinks::prefix_from_relation_player(relation.vertex(), player.vertex());
        LinksIterator::new(
            snapshot
                .iterate_range(&KeyRange::new_within(prefix, ThingEdgeLinks::FIXED_WIDTH_ENCODING), storage_counters),
        )
    }

    pub fn get_links_reverse_by_player_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        player_type_range: &impl RangeBounds<ObjectType>,
        storage_counters: StorageCounters,
    ) -> LinksReverseIterator {
        let range_start = match Self::start_type_bound_to_range_start_included_type(player_type_range.start_bound()) {
            None => return LinksReverseIterator::new_empty(),
            Some(start_type) => ThingEdgeLinks::prefix_reverse_from_player_type(
                start_type.vertex().prefix(),
                start_type.vertex().type_id_(),
            ),
        };
        let range_end = match Self::end_type_bound_to_range_end_included_type(player_type_range.end_bound()) {
            None => return LinksReverseIterator::new_empty(),
            Some(end_type) => ThingEdgeLinks::prefix_reverse_from_player_type(
                end_type.vertex().prefix(),
                end_type.vertex().type_id_(),
            ),
        };
        LinksReverseIterator::new(snapshot.iterate_range(
            &KeyRange::new(
                RangeStart::Inclusive(range_start),
                RangeEnd::EndPrefixInclusive(range_end),
                ThingEdgeLinks::FIXED_WIDTH_ENCODING,
            ),
            storage_counters,
        ))
    }

    pub fn get_links_reverse_by_player_and_relation_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: impl ObjectAPI,
        relation_type_range: &impl RangeBounds<RelationType>,
        storage_counters: StorageCounters,
    ) -> LinksReverseIterator {
        let range_start = match Self::start_type_bound_to_range_start_included_type(relation_type_range.start_bound()) {
            None => return LinksReverseIterator::new_empty(),
            Some(start_type) => ThingEdgeLinks::prefix_reverse_from_player_relation_type(
                player.vertex(),
                start_type.vertex().type_id_(),
            ),
        };
        let range_end = match Self::end_type_bound_to_range_end_included_type(relation_type_range.end_bound()) {
            None => return LinksReverseIterator::new_empty(),
            Some(end_type) => {
                ThingEdgeLinks::prefix_reverse_from_player_relation_type(player.vertex(), end_type.vertex().type_id_())
            }
        };
        LinksReverseIterator::new(snapshot.iterate_range(
            &KeyRange::new(
                RangeStart::Inclusive(range_start),
                RangeEnd::EndPrefixInclusive(range_end),
                ThingEdgeLinks::FIXED_WIDTH_ENCODING,
            ),
            storage_counters,
        ))
    }

    pub(crate) fn has_role_player(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        let links = ThingEdgeLinks::new(relation.vertex(), player.vertex(), role_type.vertex());
        let links_exists = snapshot
            .get_mapped(links.into_storage_key().as_reference(), |_| true, storage_counters)
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or(false);
        Ok(links_exists)
    }

    pub(crate) fn get_role_players(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<(RolePlayer, u64), Box<ConceptReadError>>> {
        let prefix = ThingEdgeLinks::prefix_from_relation(relation.vertex());
        Iterator::map(
            LinksIterator::new(
                snapshot.iterate_range(
                    &KeyRange::new_within(prefix, ThingEdgeLinks::FIXED_WIDTH_ENCODING),
                    storage_counters,
                ),
            ),
            |result| result.map(|(links, count)| (links.into_role_player(), count)),
        )
    }

    pub(crate) fn get_role_players_ordered(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> Result<Vec<Object>, Box<ConceptReadError>> {
        let key = build_object_vertex_property_links_order(relation.vertex(), role_type.into_vertex());
        let players = snapshot
            .get_mapped(
                key.into_storage_key().as_reference(),
                |bytes| decode_role_players(bytes).map(Object::new).collect(),
                storage_counters,
            )
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or_else(Vec::new);
        Ok(players)
    }

    pub(crate) fn get_role_players_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<(RolePlayer, u64), Box<ConceptReadError>>> {
        self.get_role_players(snapshot, relation, storage_counters).filter_map::<Result<(RolePlayer, u64), _>, _>(
            move |item| match item {
                Ok((role_player, count)) => (role_player.role_type() == role_type).then_some(Ok((role_player, count))),
                Err(error) => Some(Err(error)),
            },
        )
    }

    fn indexed_relation_prefix_in_relation_type(
        relation_type: RelationType,
        start_player_type: ObjectType,
    ) -> StorageKey<'static, { ThingEdgeIndexedRelation::LENGTH_PREFIX_REL_TYPE_ID_START_TYPE }> {
        ThingEdgeIndexedRelation::prefix_relation_type_start_type(
            relation_type.vertex().type_id_(),
            start_player_type.vertex(),
        )
    }

    pub fn get_indexed_relations_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
        start_player_type_range: impl RangeBounds<ObjectType>,
        storage_counters: StorageCounters,
    ) -> Result<IndexedRelationsIterator, Box<ConceptReadError>> {
        let start = match Self::start_type_bound_to_range_start_included_type(start_player_type_range.start_bound()) {
            None => return Ok(IndexedRelationsIterator::new_empty()),
            Some(start_player_type) => Self::indexed_relation_prefix_in_relation_type(relation_type, start_player_type),
        };
        let end = match Self::end_type_bound_to_range_end_included_type(start_player_type_range.end_bound()) {
            None => return Ok(IndexedRelationsIterator::new_empty()),
            Some(end_player_type) => Self::indexed_relation_prefix_in_relation_type(relation_type, end_player_type),
        };
        let range = &KeyRange::new(
            RangeStart::Inclusive(start),
            RangeEnd::EndPrefixInclusive(end),
            ThingEdgeIndexedRelation::FIXED_WIDTH_ENCODING,
        );
        self.iterate_indexed_relations(snapshot, range, relation_type, storage_counters)
    }

    pub(crate) fn has_indexed_relation_player(
        &self,
        snapshot: &impl ReadableSnapshot,
        start_player: impl ObjectAPI,
        end_player: Object,
        relation: Relation,
        start_role: RoleType,
        end_role: RoleType,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        if !self.type_manager.relation_index_available(snapshot, relation.type_())? {
            Err(ConceptReadError::RelationIndexNotAvailable {
                relation_label: relation.type_().get_label(snapshot, self.type_manager())?.to_owned(),
            })?;
        }
        let edge = ThingEdgeIndexedRelation::new(
            start_player.vertex(),
            end_player.vertex(),
            relation.vertex(),
            start_role.vertex().type_id_(),
            end_role.vertex().type_id_(),
        );
        snapshot
            .get::<BUFFER_KEY_INLINE>(edge.into_storage_key().as_reference(), storage_counters)
            .map(|option| option.is_some())
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))
    }

    pub(crate) fn get_indexed_relation_players_from(
        &self,
        snapshot: &impl ReadableSnapshot,
        start: impl ObjectAPI,
        relation_type: RelationType,
        storage_counters: StorageCounters,
    ) -> Result<IndexedRelationsIterator, Box<ConceptReadError>> {
        let prefix = ThingEdgeIndexedRelation::prefix_start(relation_type.vertex().type_id_(), start.vertex());
        self.iterate_indexed_relations(
            snapshot,
            &KeyRange::new_within(prefix, ThingEdgeIndexedRelation::FIXED_WIDTH_ENCODING),
            relation_type,
            storage_counters,
        )
    }

    pub(crate) fn get_indexed_relations_between(
        &self,
        snapshot: &impl ReadableSnapshot,
        start: impl ObjectAPI,
        end: impl ObjectAPI,
        relation_type: RelationType,
        storage_counters: StorageCounters,
    ) -> Result<IndexedRelationsIterator, Box<ConceptReadError>> {
        let prefix =
            ThingEdgeIndexedRelation::prefix_start_end(relation_type.vertex().type_id_(), start.vertex(), end.vertex());
        self.iterate_indexed_relations(
            snapshot,
            &KeyRange::new_within(prefix, ThingEdgeIndexedRelation::FIXED_WIDTH_ENCODING),
            relation_type,
            storage_counters,
        )
    }

    pub(crate) fn get_indexed_relation_roles(
        &self,
        snapshot: &impl ReadableSnapshot,
        start: impl ObjectAPI,
        end: impl ObjectAPI,
        relation: Relation,
        storage_counters: StorageCounters,
    ) -> Result<IndexedRelationsIterator, Box<ConceptReadError>> {
        let prefix =
            ThingEdgeIndexedRelation::prefix_start_end_relation(start.vertex(), end.vertex(), relation.vertex());
        self.iterate_indexed_relations(
            snapshot,
            &KeyRange::new_within(prefix, ThingEdgeIndexedRelation::FIXED_WIDTH_ENCODING),
            relation.type_(),
            storage_counters,
        )
    }

    pub(crate) fn get_indexed_relation_end_roles(
        &self,
        snapshot: &impl ReadableSnapshot,
        start: impl ObjectAPI,
        end: impl ObjectAPI,
        relation: Relation,
        start_role: RoleType,
        storage_counters: StorageCounters,
    ) -> Result<IndexedRelationsIterator, Box<ConceptReadError>> {
        let prefix = ThingEdgeIndexedRelation::prefix_start_end_relation_startrole(
            start.vertex(),
            end.vertex(),
            relation.vertex(),
            start_role.vertex(),
        );
        self.iterate_indexed_relations(
            snapshot,
            &KeyRange::new_within(prefix, ThingEdgeIndexedRelation::FIXED_WIDTH_ENCODING),
            relation.type_(),
            storage_counters,
        )
    }

    fn iterate_indexed_relations<const INLINE_SIZE: usize>(
        &self,
        snapshot: &impl ReadableSnapshot,
        range: &KeyRange<StorageKey<'_, INLINE_SIZE>>,
        relation_type: RelationType,
        storage_counters: StorageCounters,
    ) -> Result<IndexedRelationsIterator, Box<ConceptReadError>> {
        if !self.type_manager().relation_index_available(snapshot, relation_type)? {
            Err(ConceptReadError::RelationIndexNotAvailable {
                relation_label: relation_type.get_label(snapshot, self.type_manager())?.to_owned(),
            })?;
        }
        Ok(IndexedRelationsIterator::new(snapshot.iterate_range(range, storage_counters)))
    }

    pub(crate) fn get_status(
        &self,
        snapshot: &impl ReadableSnapshot,
        key: StorageKey<'_, BUFFER_KEY_INLINE>,
        storage_counters: StorageCounters,
    ) -> ConceptStatus {
        snapshot
            .get_write(key.as_reference())
            .map(|write| match write {
                Write::Insert { .. } => ConceptStatus::Inserted,
                Write::Put { .. } => ConceptStatus::Put,
                Write::Delete => ConceptStatus::Deleted,
            })
            .unwrap_or_else(|| {
                debug_assert!(snapshot
                    .get_last_existing::<BUFFER_VALUE_INLINE>(key.as_reference(), storage_counters)
                    .is_ok_and(|option| option.is_some()));
                ConceptStatus::Persisted
            })
    }

    pub fn instance_exists(
        &self,
        snapshot: &impl ReadableSnapshot,
        instance: &impl ThingAPI,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        snapshot
            .contains(instance.vertex().into_storage_key().as_reference(), storage_counters)
            .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))
    }

    pub fn type_exists(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeAPI,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        snapshot
            .contains(type_.vertex().into_storage_key().as_reference(), storage_counters)
            .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))
    }

    pub fn start_type_bound_to_range_start_included_type<T: TypeAPI>(start_bound: Bound<&T>) -> Option<T> {
        let bound_inclusive = match start_bound {
            Bound::Included(type_) => *type_,
            Bound::Excluded(type_) => type_.previous_possible()?,
            Bound::Unbounded => T::MIN,
        };
        Some(bound_inclusive)
    }

    fn end_type_bound_to_range_end_included_type<T: TypeAPI>(start_bound: Bound<&T>) -> Option<T> {
        let bound_inclusive = match start_bound {
            Bound::Included(type_) => *type_,
            Bound::Excluded(type_) => type_.previous_possible()?,
            Bound::Unbounded => T::MAX,
        };
        Some(bound_inclusive)
    }

    fn get_value_range<'a>(
        expected_value_type_category: ValueTypeCategory,
        range: &'a impl RangeBounds<Value<'a>>,
    ) -> Option<(Bound<Value<'a>>, Bound<Value<'a>>)> {
        let lower_bound = Self::get_value_lower_bound(expected_value_type_category, range)?;
        let upper_bound = Self::get_value_upper_bound(expected_value_type_category, range)?;
        Some((lower_bound, upper_bound))
    }

    fn get_value_lower_bound_across_types<'a>(
        ordered_possible_value_types: &[ValueTypeCategory],
        range: &'a impl RangeBounds<Value<'a>>,
    ) -> Option<Bound<Value<'a>>> {
        debug_assert!(Self::value_types_in_order(ordered_possible_value_types));
        ordered_possible_value_types
            .iter()
            .filter_map(|value_type_category| Self::get_value_lower_bound(*value_type_category, range))
            .next()
    }

    fn value_types_in_order(value_types: &[ValueTypeCategory]) -> bool {
        if value_types.is_empty() {
            return true;
        }
        let mut current = &value_types[0];
        for other in &value_types[1..] {
            if current.to_bytes() > other.to_bytes() {
                return false;
            }
            current = other;
        }
        true
    }

    fn get_value_upper_bound_across_types<'a>(
        ordered_possible_value_types: &[ValueTypeCategory],
        range: &'a impl RangeBounds<Value<'a>>,
    ) -> Option<Bound<Value<'a>>> {
        debug_assert!(Self::value_types_in_order(ordered_possible_value_types));
        ordered_possible_value_types
            .iter()
            .rev()
            .filter_map(|value_type_category| Self::get_value_upper_bound(*value_type_category, range))
            .next()
    }

    fn get_value_lower_bound<'a>(
        expected_value_type_category: ValueTypeCategory,
        range: &'a impl RangeBounds<Value<'a>>,
    ) -> Option<Bound<Value<'a>>> {
        let start_value_type = match range.start_bound() {
            Bound::Included(value) | Bound::Excluded(value) => value.value_type(),
            Bound::Unbounded => return Some(Bound::Unbounded),
        };
        if !start_value_type.is_approximately_castable_to(expected_value_type_category) {
            return None;
        }
        Some(range.start_bound().map(|value| {
            if expected_value_type_category != start_value_type.category() {
                value
                    .as_reference()
                    .approximate_cast_lower_bound(expected_value_type_category)
                    .expect("Failed to do a lower-bound conversion between value types.")
            } else {
                value.as_reference()
            }
        }))
    }

    fn get_value_upper_bound<'a>(
        expected_value_type_category: ValueTypeCategory,
        range: &'a impl RangeBounds<Value<'a>>,
    ) -> Option<Bound<Value<'a>>> {
        let end_value_type = match range.end_bound() {
            Bound::Included(value) | Bound::Excluded(value) => value.value_type(),
            Bound::Unbounded => return Some(Bound::Unbounded),
        };
        if !end_value_type.is_approximately_castable_to(expected_value_type_category) {
            return None;
        }
        Some(range.end_bound().map(|value| {
            if expected_value_type_category != end_value_type.category() {
                value
                    .as_reference()
                    .approximate_cast_upper_bound(expected_value_type_category)
                    .expect("Failed to do an upper-bound conversion between value types.")
            } else {
                value.as_reference()
            }
        }))
    }
}

impl ThingManager {
    pub(crate) fn lock_existing_object(&self, snapshot: &mut impl WritableSnapshot, object: impl ObjectAPI) {
        snapshot.unmodifiable_lock_add(object.vertex().into_storage_key().into_owned_array())
    }

    pub(crate) fn lock_existing_attribute(&self, snapshot: &mut impl WritableSnapshot, attribute: &Attribute) {
        snapshot.unmodifiable_lock_add(attribute.vertex().into_storage_key().into_owned_array())
    }

    pub fn finalise(
        &self,
        snapshot: &mut impl WritableSnapshot,
        storage_counters: StorageCounters,
    ) -> Result<(), Vec<ConceptWriteError>> {
        self.validate(snapshot, storage_counters.clone())?;

        self.cleanup_relations(snapshot, storage_counters.clone()).map_err(|err| vec![*err])?;
        self.cleanup_attributes(snapshot, storage_counters.clone()).map_err(|err| vec![*err])?;

        match self.create_commit_locks(snapshot, storage_counters) {
            Ok(_) => Ok(()),
            Err(error) => Err(vec![ConceptWriteError::ConceptRead { typedb_source: error }]),
        }
    }

    fn create_commit_locks(
        &self,
        snapshot: &mut impl WritableSnapshot,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        // TODO: Should not collect here (iterate_writes() already copies)
        for (key, _write) in snapshot.iterate_writes().collect_vec() {
            if ThingEdgeHas::is_has(&key) {
                let has = ThingEdgeHas::decode(Bytes::Reference(key.bytes()));
                let object = Object::new(has.from());
                let attribute = Attribute::new(has.to());
                let attribute_type = attribute.type_();

                self.add_exclusive_lock_for_unique_constraint(snapshot, &object, attribute, storage_counters.clone())?;
                self.add_exclusive_lock_for_owns_cardinality_constraint(snapshot, &object, attribute_type)?;
            } else if ThingEdgeLinks::is_links(&key) {
                let role_player = ThingEdgeLinks::decode(Bytes::Reference(key.bytes()));
                let relation = Relation::new(role_player.relation());
                let player = Object::new(role_player.player());
                let role_type = RoleType::build_from_type_id(role_player.role_id());

                self.add_exclusive_lock_for_plays_cardinality_constraint(snapshot, &player, role_type)?;
                self.add_exclusive_lock_for_relates_cardinality_constraint(snapshot, &relation, role_type)?;
            }
        }

        Ok(())
    }

    fn add_exclusive_lock_for_unique_constraint(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: &Object,
        attribute: Attribute,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let unique_constraint_opt = owner.type_().get_owned_attribute_type_constraint_unique(
            snapshot,
            self.type_manager(),
            attribute.type_(),
        )?;
        if let Some(unique_constraint) = unique_constraint_opt {
            let attribute_key = attribute.vertex();
            let attribute_value = snapshot
                .get_last_existing::<BUFFER_VALUE_INLINE>(
                    attribute_key.into_storage_key().as_reference(),
                    storage_counters,
                )
                .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))?
                .ok_or(ConceptReadError::InternalMissingAttributeValue {})?;

            let lock_key = create_custom_lock_key(
                [
                    &Infix::PropertyAnnotationUnique.infix_id().bytes(),
                    &*unique_constraint.source().attribute().vertex().to_bytes(),
                    attribute_key.attribute_id().bytes(),
                    &attribute_value,
                    &*owner.vertex().to_bytes(),
                    &*unique_constraint.source().owner().vertex().to_bytes(),
                ]
                .into_iter(),
            );
            snapshot.exclusive_lock_add(lock_key);
        }
        Ok(())
    }

    fn add_exclusive_lock_for_owns_cardinality_constraint(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: &Object,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinality_constraints =
            get_checked_constraints(owner.type_().get_owned_attribute_type_constraints_cardinality(
                snapshot,
                self.type_manager(),
                attribute_type,
            )?);
        if cardinality_constraints.is_empty() {
            return Ok(());
        }

        for constraint in cardinality_constraints {
            let lock_key = create_custom_lock_key(
                [
                    &Infix::PropertyAnnotationCardinality.infix_id().bytes(),
                    &*owner.vertex().to_bytes(),
                    &Prefix::EdgeOwns.prefix_id().to_bytes(),
                    &*constraint.source().interface().vertex().to_bytes(),
                ]
                .into_iter(),
            );
            snapshot.exclusive_lock_add(lock_key);
        }
        Ok(())
    }

    fn add_exclusive_lock_for_plays_cardinality_constraint(
        &self,
        snapshot: &mut impl WritableSnapshot,
        player: &Object,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinality_constraints = get_checked_constraints(
            player.type_().get_played_role_type_constraints_cardinality(snapshot, self.type_manager(), role_type)?,
        );
        if cardinality_constraints.is_empty() {
            return Ok(());
        }

        for constraint in cardinality_constraints {
            let lock_key = create_custom_lock_key(
                [
                    &Infix::PropertyAnnotationCardinality.infix_id().bytes(),
                    &*player.vertex().to_bytes(),
                    &Prefix::EdgePlays.prefix_id().to_bytes(),
                    &*constraint.source().interface().vertex().to_bytes(),
                ]
                .into_iter(),
            );
            snapshot.exclusive_lock_add(lock_key);
        }
        Ok(())
    }

    fn add_exclusive_lock_for_relates_cardinality_constraint(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: &Relation,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinality_constraints = get_checked_constraints(
            relation.type_().get_related_role_type_constraints_cardinality(snapshot, self.type_manager(), role_type)?,
        );
        if cardinality_constraints.is_empty() {
            return Ok(());
        }

        for constraint in cardinality_constraints {
            let lock_key = create_custom_lock_key(
                [
                    &Infix::PropertyAnnotationCardinality.infix_id().bytes(),
                    &*relation.vertex().to_bytes(),
                    &Prefix::EdgeRelates.prefix_id().to_bytes(),
                    &*constraint.source().interface().vertex().to_bytes(),
                ]
                .into_iter(),
            );
            snapshot.exclusive_lock_add(lock_key);
        }
        Ok(())
    }

    fn cleanup_relations(
        &self,
        snapshot: &mut impl WritableSnapshot,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let mut any_deleted = true;
        while any_deleted {
            any_deleted = false;
            for (key, _) in snapshot
                .iterate_writes_range(&KeyRange::new_within(
                    ThingEdgeLinks::prefix(),
                    ThingEdgeLinks::FIXED_WIDTH_ENCODING,
                ))
                .filter(|(_, write)| matches!(write, Write::Delete))
            {
                let edge = ThingEdgeLinks::decode(Bytes::Reference(key.byte_array().as_ref()));
                let relation = Relation::new(edge.from());
                if relation.get_status(snapshot, self, storage_counters.clone()) == ConceptStatus::Deleted {
                    continue;
                }
                if !relation.has_players(snapshot, self, storage_counters.clone()) {
                    if !self.type_manager().get_is_relation_type_independent(snapshot, relation.type_())? {
                        relation.delete(snapshot, self, storage_counters.clone())?;
                        any_deleted = true;
                    }
                }
            }
        }

        let mut any_deleted = true;
        while any_deleted {
            any_deleted = false;
            for key in snapshot
                .iterate_writes_range(&KeyRange::new_within(
                    ObjectVertex::build_prefix_prefix(Prefix::VertexRelation, ObjectVertex::KEYSPACE),
                    ObjectVertex::FIXED_WIDTH_ENCODING,
                ))
                .filter_map(|(key, write)| (!matches!(write, Write::Delete)).then_some(key))
            {
                let relation = Relation::new(ObjectVertex::decode(key.bytes()));
                if !relation.has_players(snapshot, self, storage_counters.clone()) {
                    relation.delete(snapshot, self, storage_counters.clone())?;
                    any_deleted = true;
                }
            }
        }

        let mut any_deleted = true;
        while any_deleted {
            any_deleted = false;
            for relation_type in snapshot
                .iterate_writes_range(&KeyRange::new_within(
                    TypeVertexProperty::build_prefix(),
                    TypeVertexProperty::FIXED_WIDTH_ENCODING,
                ))
                .filter_map(|(key, write)| match write {
                    Write::Put { .. } | Write::Insert { .. } => {
                        let bytes = Bytes::reference(key.bytes());
                        if <AnnotationCascade as TypeVertexPropertyEncoding>::is_decodable_from(bytes.clone()) {
                            let decoded = TypeVertexProperty::decode(bytes);
                            RelationType::from_vertex(decoded.type_vertex()).ok()
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
            {
                if !self.type_exists(snapshot, relation_type, storage_counters.clone())? {
                    continue;
                }
                let subtypes = relation_type.get_subtypes_transitive(snapshot, self.type_manager())?;
                once(&relation_type).chain(subtypes.into_iter()).try_for_each(|type_| {
                    let is_cascade = true; // TODO: Always consider cascade now, can be changed later.
                    if is_cascade {
                        let mut relations: InstanceIterator<Relation> = self.get_instances_in(
                            snapshot,
                            *type_,
                            <Relation as ThingAPI>::Vertex::KEYSPACE,
                            storage_counters.clone(),
                        );
                        while let Some(relation) = Iterator::next(&mut relations).transpose()? {
                            if !relation.has_players(snapshot, self, storage_counters.clone()) {
                                relation.delete(snapshot, self, storage_counters.clone())?;
                                any_deleted = true;
                            }
                        }
                    }
                    Ok::<(), Box<ConceptWriteError>>(())
                })?;
            }
        }

        Ok(())
    }

    fn cleanup_attributes(
        &self,
        snapshot: &mut impl WritableSnapshot,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        for (key, _write) in snapshot
            .iterate_writes_range(&KeyRange::new_within(ThingEdgeHas::prefix(), ThingEdgeHas::FIXED_WIDTH_ENCODING))
            .filter(|(_, write)| matches!(write, Write::Delete))
        {
            let edge = ThingEdgeHas::decode(Bytes::Reference(key.byte_array()));
            let attribute = Attribute::new(edge.to());
            let is_independent = attribute.type_().is_independent(snapshot, self.type_manager())?;
            if attribute.get_status(snapshot, self, storage_counters.clone()) == ConceptStatus::Deleted {
                continue;
            }
            if !is_independent && !attribute.has_owners(snapshot, self, storage_counters.clone()) {
                attribute.delete(snapshot, self, storage_counters.clone())?;
            }
        }

        // link together long and short attributes
        for (key, _value) in snapshot
            .iterate_writes_range(&KeyRange::new_within(
                StorageKey::new(
                    AttributeVertex::keyspace_for_is_short(true),
                    Bytes::inline(Prefix::VertexAttribute.prefix_id().to_bytes(), 1),
                ),
                Prefix::VertexAttribute.fixed_width_keys(),
            ))
            .chain(snapshot.iterate_writes_range(&KeyRange::new_within(
                StorageKey::new(
                    AttributeVertex::keyspace_for_is_short(false),
                    Bytes::inline(Prefix::VertexAttribute.prefix_id().to_bytes(), 1),
                ),
                Prefix::VertexAttribute.fixed_width_keys(),
            )))
            .filter_map(|(key, write)| match write {
                Write::Put { value, .. } => Some((key, value)),
                _ => None,
            })
        {
            let attribute = Attribute::new(AttributeVertex::decode(key.bytes()));
            let is_independent = attribute.type_().is_independent(snapshot, self.type_manager())?;
            if !is_independent && !attribute.has_owners(snapshot, self, storage_counters.clone()) {
                self.unput_attribute(snapshot, &attribute, storage_counters.clone())?;
            }
        }

        for attribute_type in snapshot
            .iterate_writes_range(&KeyRange::new_within(
                TypeVertexProperty::build_prefix(),
                TypeVertexProperty::FIXED_WIDTH_ENCODING,
            ))
            .filter_map(|(key, write)| match write {
                Write::Delete => {
                    let bytes = Bytes::reference(key.bytes());
                    if <AnnotationIndependent as TypeVertexPropertyEncoding>::is_decodable_from(bytes.clone()) {
                        let decoded = TypeVertexProperty::decode(bytes);
                        AttributeType::from_vertex(decoded.type_vertex()).ok()
                    } else {
                        None
                    }
                }
                _ => None,
            })
        {
            if !self.type_exists(snapshot, attribute_type, storage_counters.clone())? {
                continue;
            }
            let subtypes = attribute_type.get_subtypes_transitive(snapshot, self.type_manager())?;
            once(&attribute_type).chain(subtypes.into_iter()).try_for_each(|type_| {
                let is_independent = type_.is_independent(snapshot, self.type_manager())?;
                if let Some(value_type) = type_.get_value_type_without_source(snapshot, self.type_manager())? {
                    if !is_independent {
                        let mut attributes: InstanceIterator<Attribute> = self.get_instances_in(
                            snapshot,
                            *type_,
                            AttributeVertex::keyspace_for_category(value_type.category()),
                            storage_counters.clone(),
                        );
                        while let Some(attribute) = Iterator::next(&mut attributes).transpose()? {
                            if !attribute.has_owners(snapshot, self, storage_counters.clone()) {
                                attribute.delete(snapshot, self, storage_counters.clone())?;
                            }
                        }
                    }
                }
                Ok::<(), Box<ConceptWriteError>>(())
            })?;
        }

        Ok(())
    }

    fn validate(
        &self,
        snapshot: &mut impl WritableSnapshot,
        storage_counters: StorageCounters,
    ) -> Result<(), Vec<ConceptWriteError>> {
        /*
        The cardinalities validation flow is the following:
        1. Collect instances affected by cardinalities changes (separately for 3 capabilities: owns, plays, relates)
        2. Validate only the affected instances to avoid rescanning the whole system (see validate_capability_cardinality_constraint). For each object,
          2a. Count every capability instance it has (every has, every played role, every roleplayer)
          2b. Collect cardinality constraints (declared and inherited) of all marked capabilities without duplications (if a subtype and its supertype are affected, the supertype's constraint is checked once)
          2c. Validate each constraint separately using the counts prepared in 2a. To validate a constraint, take its source type (where this constraint is declared), and count all instances of the source type and its subtypes.

        Let's consider the following example:
          entity person,
            owns name @card(1..),
            owns surname, # sub name
            owns changed-surname @card(1..2); # sub surname

        A query is being run:
          define person owns surname @card(1..10);

        It will be processed like:
        1. All instances of persons will be collected, the only surname attribute type saved as modified.
        2. For each instance of persons:
          2a. All names, surnames, and changed-surnames are counted (based on instances' explicit types).
          2b. surname's constraints will be taken: @card(1..) from name and @card(1..10) from surname.
          2c. For each constraint:
            @card(1..): combines counts of names, surnames, and changed-surnames. If it's at least 1, it's good.
            @card(1..10): combines counts of surnames and changed-surnames (without names). If it's from 1 to 10, it's good.

        This way, the validation on step 2 always goes up the sub hierarchy to collect current constraints, and then goes down the hierarchy to consider all the suitable instances.

        However, it won't work if stage 1 is incomplete. For example:
          undefine owns surname from person

        If we mark only surnames as affected attribute types, we will get 0 constraints on the validation stage (as person does not have any constraints for it anymore, it does not own it).
        Thus, we will not check the cardinality of names, although it might be violated as it does not now count surnames!
        We could potentially use the old version of storage (ignoring the snapshot), but it would make the reasoning even more complicated.
        Please keep these complexities in mind when modifying the collection stage in the following methods.
         */
        let mut errors = Vec::new();

        let mut modified_objects_attribute_types = HashMap::new();
        let mut modified_objects_role_types = HashMap::new();
        let mut modified_relations_role_types = HashMap::new();

        let mut res = self.collect_new_objects(
            snapshot,
            &mut modified_objects_attribute_types,
            &mut modified_objects_role_types,
            &mut modified_relations_role_types,
        );
        collect_errors!(errors, res, |typedb_source| DataValidationError::ConceptRead { typedb_source });

        res = self.collect_modified_has_objects(
            snapshot,
            &mut modified_objects_attribute_types,
            storage_counters.clone(),
        );
        collect_errors!(errors, res, |typedb_source| DataValidationError::ConceptRead { typedb_source });

        res = self.collect_modified_links_objects(
            snapshot,
            &mut modified_relations_role_types,
            &mut modified_objects_role_types,
            storage_counters.clone(),
        );
        collect_errors!(errors, res, |typedb_source| DataValidationError::ConceptRead { typedb_source });

        res = self.collect_modified_schema_capability_cardinalities_objects(
            snapshot,
            &mut modified_objects_attribute_types,
            &mut modified_objects_role_types,
            &mut modified_relations_role_types,
            storage_counters.clone(),
        );
        collect_errors!(errors, res, |typedb_source| DataValidationError::ConceptRead { typedb_source });

        for (object, modified_owns) in modified_objects_attribute_types {
            res = CommitTimeValidation::validate_object_has(
                snapshot,
                self,
                object,
                modified_owns,
                &mut errors,
                storage_counters.clone(),
            );
            collect_errors!(errors, res, |typedb_source| DataValidationError::ConceptRead { typedb_source });
        }

        for (object, modified_plays) in modified_objects_role_types {
            res = CommitTimeValidation::validate_object_links(
                snapshot,
                self,
                object,
                modified_plays,
                &mut errors,
                storage_counters.clone(),
            );
            collect_errors!(errors, res, |typedb_source| DataValidationError::ConceptRead { typedb_source });
        }

        for (relation, modified_relates) in modified_relations_role_types {
            res = CommitTimeValidation::validate_relation_links(
                snapshot,
                self,
                relation,
                modified_relates,
                &mut errors,
                storage_counters.clone(),
            );
            collect_errors!(errors, res, |typedb_source| DataValidationError::ConceptRead { typedb_source });
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors
                .into_iter()
                .map(|typedb_source| ConceptWriteError::DataValidation { typedb_source: Box::new(typedb_source) })
                .collect())
        }
    }

    fn collect_new_objects(
        &self,
        snapshot: &impl WritableSnapshot,
        out_object_attribute_types: &mut HashMap<Object, HashSet<AttributeType>>,
        out_object_role_types: &mut HashMap<Object, HashSet<RoleType>>,
        out_relation_role_types: &mut HashMap<Relation, HashSet<RoleType>>,
    ) -> Result<(), Box<ConceptReadError>> {
        for key in snapshot
            .iterate_writes_range(&KeyRange::new_variable_width(
                RangeStart::Inclusive(StorageKey::new(
                    ObjectVertex::KEYSPACE,
                    Bytes::<0>::reference(
                        ObjectVertex::build_prefix_prefix(Prefix::VertexEntity, ObjectVertex::KEYSPACE).bytes(),
                    ),
                )),
                RangeEnd::EndPrefixInclusive(StorageKey::new(
                    ObjectVertex::KEYSPACE,
                    Bytes::<0>::reference(
                        ObjectVertex::build_prefix_prefix(Prefix::VertexRelation, ObjectVertex::KEYSPACE).bytes(),
                    ),
                )),
            ))
            .filter_map(|(key, write)| match write {
                Write::Insert { .. } => Some(key),
                Write::Delete => None,
                Write::Put { .. } => unreachable!("Encountered a Put for an entity"),
            })
        {
            let object = Object::new(ObjectVertex::decode(key.bytes()));
            match &object {
                Object::Entity(_) => {}
                Object::Relation(relation) => {
                    let updated_role_types = out_relation_role_types.entry(*relation).or_default();
                    for relates in relation.type_().get_relates(snapshot, self.type_manager())?.into_iter() {
                        updated_role_types.insert(relates.role());
                    }
                }
            }

            let updated_attribute_types = out_object_attribute_types.entry(object).or_default();
            for owns in object.type_().get_owns(snapshot, self.type_manager())?.into_iter() {
                updated_attribute_types.insert(owns.attribute());
            }

            let updated_role_types = out_object_role_types.entry(object).or_default();
            for plays in object.type_().get_plays(snapshot, self.type_manager())?.into_iter() {
                updated_role_types.insert(plays.role());
            }
        }
        Ok(())
    }

    fn collect_modified_has_objects(
        &self,
        snapshot: &impl WritableSnapshot,
        out_object_attribute_types: &mut HashMap<Object, HashSet<AttributeType>>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        for (key, _) in snapshot
            .iterate_writes_range(&KeyRange::new_within(ThingEdgeHas::prefix(), ThingEdgeHas::FIXED_WIDTH_ENCODING))
        {
            let edge = ThingEdgeHas::decode(Bytes::Reference(key.byte_array()));
            let owner = Object::new(edge.from());
            let attribute = Attribute::new(edge.to());
            if self.instance_exists(snapshot, &owner, storage_counters.clone())? {
                let updated_attribute_types = out_object_attribute_types.entry(owner).or_default();
                updated_attribute_types.insert(attribute.type_());
            }
        }

        Ok(())
    }

    fn collect_modified_links_objects(
        &self,
        snapshot: &impl WritableSnapshot,
        out_relation_role_types: &mut HashMap<Relation, HashSet<RoleType>>,
        out_object_role_types: &mut HashMap<Object, HashSet<RoleType>>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        for (key, _) in snapshot
            .iterate_writes_range(&KeyRange::new_within(ThingEdgeLinks::prefix(), ThingEdgeLinks::FIXED_WIDTH_ENCODING))
        {
            let edge = ThingEdgeLinks::decode(Bytes::reference(key.bytes()));
            let relation = Relation::new(edge.relation());
            let player = Object::new(edge.player());
            let role_type = RoleType::build_from_type_id(edge.role_id());

            if self.instance_exists(snapshot, &relation, storage_counters.clone())? {
                let updated_role_types = out_relation_role_types.entry(relation).or_default();
                updated_role_types.insert(role_type);
            }

            if self.instance_exists(snapshot, &player, storage_counters.clone())? {
                let updated_role_types = out_object_role_types.entry(player).or_default();
                updated_role_types.insert(role_type);
            }
        }

        Ok(())
    }

    fn collect_modified_schema_capability_cardinalities_objects(
        &self,
        snapshot: &impl WritableSnapshot,
        out_object_attribute_types: &mut HashMap<Object, HashSet<AttributeType>>,
        out_object_role_types: &mut HashMap<Object, HashSet<RoleType>>,
        out_relation_role_types: &mut HashMap<Relation, HashSet<RoleType>>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let mut modified_owns = HashMap::new();
        let mut modified_plays = HashMap::new();
        let mut modified_relates = HashMap::new();
        self.collect_modified_schema_capability_cardinalities(
            snapshot,
            &mut modified_owns,
            &mut modified_plays,
            &mut modified_relates,
            storage_counters.clone(),
        )?;

        for (relation_type, role_types) in modified_relates {
            let (min, max) = minmax_or!(
                TypeAPI::chain_types(
                    relation_type,
                    relation_type.get_subtypes_transitive(snapshot, self.type_manager(),)?.into_iter().cloned()
                ),
                unreachable!("Expected at least one object type")
            );
            let mut it = self.get_relations_in_range(
                snapshot,
                &(Bound::Included(min), Bound::Included(max)),
                storage_counters.clone(),
            );
            while let Some(relation) = Iterator::next(&mut it).transpose()? {
                let updated_role_types = out_relation_role_types.entry(relation).or_default();
                updated_role_types.extend(role_types.iter());
            }
        }

        for (object_type, role_types) in modified_plays {
            let (min, max) = minmax_or!(
                TypeAPI::chain_types(
                    object_type,
                    object_type.get_subtypes_transitive(snapshot, self.type_manager(),)?.into_iter().cloned()
                ),
                unreachable!("Expected at least one object type")
            );
            let mut it = self.get_objects_in_range(
                snapshot,
                &(Bound::Included(min), Bound::Included(max)),
                storage_counters.clone(),
            );
            while let Some(object) = Iterator::next(&mut it).transpose()? {
                let updated_role_types = out_object_role_types.entry(object).or_default();
                updated_role_types.extend(role_types.iter());
            }
        }

        for (object_type, attribute_types) in modified_owns {
            let (min, max) = minmax_or!(
                TypeAPI::chain_types(
                    object_type,
                    object_type.get_subtypes_transitive(snapshot, self.type_manager(),)?.into_iter().cloned()
                ),
                unreachable!("Expected at least one object type")
            );
            let mut it = self.get_objects_in_range(
                snapshot,
                &(Bound::Included(min), Bound::Included(max)),
                storage_counters.clone(),
            );
            while let Some(object) = Iterator::next(&mut it).transpose()? {
                let updated_attribute_types = out_object_attribute_types.entry(object).or_default();
                updated_attribute_types.extend(attribute_types.iter());
            }
        }

        Ok(())
    }

    fn collect_modified_schema_capability_cardinalities(
        &self,
        snapshot: &impl WritableSnapshot,
        modified_owns: &mut HashMap<ObjectType, HashSet<AttributeType>>,
        modified_plays: &mut HashMap<ObjectType, HashSet<RoleType>>,
        modified_relates: &mut HashMap<RelationType, HashSet<RoleType>>,
        _storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        // New / deleted capabilities

        for (key, write) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgeOwns),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let edge = TypeEdge::decode(Bytes::reference(key.bytes()));
            let attribute_type = AttributeType::new(edge.to());
            let updated_attribute_types = modified_owns.entry(ObjectType::new(edge.from())).or_default();
            match write {
                Write::Insert { .. } | Write::Put { .. } => {
                    updated_attribute_types.insert(attribute_type);
                }
                Write::Delete => {
                    updated_attribute_types.extend(TypeAPI::chain_types(
                        attribute_type,
                        attribute_type.get_supertypes_transitive(snapshot, self.type_manager())?.into_iter().cloned(),
                    ));
                }
            }
        }

        for (key, write) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgePlays),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let edge = TypeEdge::decode(Bytes::reference(key.bytes()));
            let role_type = RoleType::new(edge.to());
            let updated_role_types = modified_plays.entry(ObjectType::new(edge.from())).or_default();
            match write {
                Write::Insert { .. } | Write::Put { .. } => {
                    updated_role_types.insert(role_type);
                }
                Write::Delete => {
                    updated_role_types.extend(TypeAPI::chain_types(
                        role_type,
                        role_type.get_supertypes_transitive(snapshot, self.type_manager())?.into_iter().cloned(),
                    ));
                }
            }
        }

        for (key, write) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgeRelates),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let edge = TypeEdge::decode(Bytes::reference(key.bytes()));
            let role_type = RoleType::new(edge.to());
            let updated_role_types = modified_relates.entry(RelationType::new(edge.from())).or_default();
            match write {
                Write::Insert { .. } | Write::Put { .. } => {
                    updated_role_types.insert(role_type);
                }
                Write::Delete => {
                    updated_role_types.extend(TypeAPI::chain_types(
                        role_type,
                        role_type.get_supertypes_transitive(snapshot, self.type_manager())?.into_iter().cloned(),
                    ));
                }
            }
        }

        // New / deleted subs between objects and interfaces

        for (key, write) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgeSub),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let edge = TypeEdge::decode(Bytes::reference(key.bytes()));
            let subtype = edge.from();
            let supertype = edge.to();
            let prefix = supertype.prefix();
            match prefix {
                // Interfaces: owns
                Prefix::VertexAttributeType => match write {
                    Write::Insert { .. } | Write::Put { .. } => {
                        let attribute_subtype = AttributeType::new(subtype);
                        for &object_type in attribute_subtype.get_owner_types(snapshot, self.type_manager())?.keys() {
                            let updated_attribute_types = modified_owns.entry(object_type).or_default();
                            updated_attribute_types.insert(attribute_subtype);
                        }
                    }
                    Write::Delete => {
                        let attribute_supertype = AttributeType::new(supertype);
                        for attribute_type in TypeAPI::chain_types(
                            attribute_supertype,
                            attribute_supertype
                                .get_supertypes_transitive(snapshot, self.type_manager())?
                                .into_iter()
                                .cloned(),
                        ) {
                            for &object_type in attribute_type.get_owner_types(snapshot, self.type_manager())?.keys() {
                                let updated_attribute_types = modified_owns.entry(object_type).or_default();
                                updated_attribute_types.insert(attribute_type);
                            }
                        }
                    }
                },
                // Interfaces: plays and relates
                Prefix::VertexRoleType => match write {
                    Write::Insert { .. } | Write::Put { .. } => {
                        let role_subtype = RoleType::new(subtype);
                        for &object_type in role_subtype.get_player_types(snapshot, self.type_manager())?.keys() {
                            let updated_role_types = modified_plays.entry(object_type).or_default();
                            updated_role_types.insert(role_subtype);
                        }
                        for &relation_type in role_subtype.get_relation_types(snapshot, self.type_manager())?.keys() {
                            let updated_role_types = modified_relates.entry(relation_type).or_default();
                            updated_role_types.insert(role_subtype);
                        }
                    }
                    Write::Delete => {
                        let role_supertype = RoleType::new(supertype);
                        for role_type in TypeAPI::chain_types(
                            role_supertype,
                            role_supertype
                                .get_supertypes_transitive(snapshot, self.type_manager())?
                                .into_iter()
                                .cloned(),
                        ) {
                            for &object_type in role_type.get_player_types(snapshot, self.type_manager())?.keys() {
                                let updated_role_types = modified_plays.entry(object_type).or_default();
                                updated_role_types.insert(role_type);
                            }
                            for &relation_type in role_type.get_relation_types(snapshot, self.type_manager())?.keys() {
                                let updated_role_types = modified_relates.entry(relation_type).or_default();
                                updated_role_types.insert(role_type);
                            }
                        }
                    }
                },
                // Objects and Relations: owns, plays, and relates
                Prefix::VertexEntityType | Prefix::VertexRelationType => match write {
                    Write::Insert { .. } | Write::Put { .. } => {
                        let object_subtype = ObjectType::new(subtype);
                        let object_supertype = ObjectType::new(supertype);

                        let supertype_owned_attribute_types =
                            object_supertype.get_owned_attribute_types(snapshot, self.type_manager())?;
                        for attribute_type in object_subtype.get_owned_attribute_types(snapshot, self.type_manager())? {
                            for &supertype_attribute_type in &supertype_owned_attribute_types {
                                if supertype_attribute_type.is_supertype_transitive_of_or_same(
                                    snapshot,
                                    self.type_manager(),
                                    attribute_type,
                                )? {
                                    let updated_attribute_types = modified_owns.entry(object_subtype).or_default();
                                    updated_attribute_types.insert(attribute_type);
                                    break;
                                }
                            }
                        }

                        let supertype_played_role_types =
                            object_supertype.get_played_role_types(snapshot, self.type_manager())?;
                        for role_type in object_subtype.get_played_role_types(snapshot, self.type_manager())? {
                            for &supertype_role_type in &supertype_played_role_types {
                                if supertype_role_type.is_supertype_transitive_of_or_same(
                                    snapshot,
                                    self.type_manager(),
                                    role_type,
                                )? {
                                    let updated_role_types = modified_plays.entry(object_subtype).or_default();
                                    updated_role_types.insert(role_type);
                                    break;
                                }
                            }
                        }

                        if prefix == Prefix::VertexRelationType {
                            let relation_subtype = RelationType::new(subtype);
                            let relation_supertype = RelationType::new(supertype);

                            let supertype_related_role_types =
                                relation_supertype.get_related_role_types(snapshot, self.type_manager())?;
                            for role_type in relation_subtype.get_related_role_types(snapshot, self.type_manager())? {
                                for &supertype_role_type in &supertype_related_role_types {
                                    if supertype_role_type.is_supertype_transitive_of_or_same(
                                        snapshot,
                                        self.type_manager(),
                                        role_type,
                                    )? {
                                        let updated_role_types = modified_relates.entry(relation_subtype).or_default();
                                        updated_role_types.insert(role_type);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        // New / deleted annotations

        for (key, _) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::PropertyTypeEdge),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let property = TypeEdgeProperty::decode(Bytes::reference(key.bytes()));
            match property.infix() {
                Infix::PropertyAnnotationKey | Infix::PropertyAnnotationCardinality => {
                    let edge = property.type_edge();
                    match edge.prefix() {
                        Prefix::EdgeOwns => {
                            let updated_attribute_types =
                                modified_owns.entry(ObjectType::new(edge.from())).or_default();
                            updated_attribute_types.insert(AttributeType::new(edge.to()));
                        }
                        Prefix::EdgeOwnsReverse => debug_assert!(false, "Unexpected property on reverse owns"),
                        Prefix::EdgePlays => {
                            let updated_role_types = modified_plays.entry(ObjectType::new(edge.from())).or_default();
                            updated_role_types.insert(RoleType::new(edge.to()));
                        }
                        Prefix::EdgePlaysReverse => debug_assert!(false, "Unexpected property on reverse plays"),
                        Prefix::EdgeRelates => {
                            let updated_role_types =
                                modified_relates.entry(RelationType::new(edge.from())).or_default();
                            updated_role_types.insert(RoleType::new(edge.to()));
                        }
                        Prefix::EdgeRelatesReverse => debug_assert!(false, "Unexpected property on reverse relates"),
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub fn create_entity(
        &self,
        snapshot: &mut impl WritableSnapshot,
        entity_type: EntityType,
    ) -> Result<Entity, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_entity_type_is_not_abstract(snapshot, self, entity_type)
            .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        Ok(Entity::new(self.vertex_generator.create_entity(entity_type.vertex().type_id_(), snapshot)))
    }

    pub fn create_relation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation_type: RelationType,
    ) -> Result<Relation, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_relation_type_is_not_abstract(snapshot, self, relation_type)
            .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        Ok(Relation::new(self.vertex_generator.create_relation(relation_type.vertex().type_id_(), snapshot)))
    }

    pub fn create_attribute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<Attribute, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_attribute_type_is_not_abstract(snapshot, self, attribute_type)
            .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_attribute_regex_constraints(
            snapshot,
            self,
            attribute_type,
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_attribute_range_constraints(
            snapshot,
            self,
            attribute_type,
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_attribute_values_constraints(
            snapshot,
            self,
            attribute_type,
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        self.put_attribute(snapshot, attribute_type, value)
    }

    pub(crate) fn put_attribute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<Attribute, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_value_type_matches_attribute_type_for_write(
            snapshot,
            self,
            attribute_type,
            value.value_type(),
            value.as_reference(),
        )?;

        let vertex = match value
            .cast(attribute_type.get_value_type_without_source(snapshot, self.type_manager())?.unwrap().category())
            .expect("value type compatibility should have been verified by this point")
        {
            Value::Boolean(bool) => {
                let encoded_boolean = BooleanBytes::build(bool);
                self.vertex_generator.create_attribute_boolean(
                    attribute_type.vertex().type_id_(),
                    encoded_boolean,
                    snapshot,
                )
            }
            Value::Integer(integer) => {
                let encoded_integer = IntegerBytes::build(integer);
                self.vertex_generator.create_attribute_integer(
                    attribute_type.vertex().type_id_(),
                    encoded_integer,
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
            Value::Decimal(decimal) => {
                let encoded_decimal = DecimalBytes::build(decimal);
                self.vertex_generator.create_attribute_decimal(
                    attribute_type.vertex().type_id_(),
                    encoded_decimal,
                    snapshot,
                )
            }
            Value::Date(date) => {
                let encoded_date = DateBytes::build(date);
                self.vertex_generator.create_attribute_date(attribute_type.vertex().type_id_(), encoded_date, snapshot)
            }
            Value::DateTime(date_time) => {
                let encoded_date_time = DateTimeBytes::build(date_time);
                self.vertex_generator.create_attribute_date_time(
                    attribute_type.vertex().type_id_(),
                    encoded_date_time,
                    snapshot,
                )
            }
            Value::DateTimeTZ(date_time_tz) => {
                let encoded_date_time_tz = DateTimeTZBytes::build(date_time_tz);
                self.vertex_generator.create_attribute_date_time_tz(
                    attribute_type.vertex().type_id_(),
                    encoded_date_time_tz,
                    snapshot,
                )
            }
            Value::Duration(duration) => {
                let encoded_duration = DurationBytes::build(duration);
                self.vertex_generator.create_attribute_duration(
                    attribute_type.vertex().type_id_(),
                    encoded_duration,
                    snapshot,
                )
            }
            Value::String(string) => {
                let encoded_string: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(&string);
                self.vertex_generator
                    .create_attribute_string(attribute_type.vertex().type_id_(), encoded_string, snapshot)
                    .map_err(|err| ConceptWriteError::SnapshotIterate { source: err })?
            }
            Value::Struct(struct_) => {
                let encoded_struct: StructBytes<'static, BUFFER_KEY_INLINE> = StructBytes::build(&struct_);
                let struct_attribute = self
                    .vertex_generator
                    .create_attribute_struct(attribute_type.vertex().type_id_(), encoded_struct, snapshot)
                    .map_err(|err| ConceptWriteError::SnapshotIterate { source: err })?;
                self.index_struct_fields(snapshot, &struct_attribute, &struct_)?;
                struct_attribute
            }
        };
        Ok(Attribute::new(vertex))
    }

    fn index_struct_fields(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_vertex: &AttributeVertex,
        struct_value: &StructValue<'_>,
    ) -> Result<(), Box<ConceptWriteError>> {
        let index_entries = struct_value
            .create_index_entries(snapshot, &self.vertex_generator, attribute_vertex)
            .map_err(|err| ConceptWriteError::SnapshotIterate { source: err })?;
        for entry in index_entries {
            let StructIndexEntry { key, value } = entry;
            match value {
                None => snapshot.put(key.into_storage_key().into_owned_array()),
                Some(value) => snapshot.put_val(key.into_storage_key().into_owned_array(), value.into_array()),
            }
        }
        Ok(())
    }

    pub(crate) fn delete_entity(
        &self,
        snapshot: &mut impl WritableSnapshot,
        entity: Entity,
        storage_counters: StorageCounters,
    ) {
        self.delete_object(snapshot, entity.vertex().into_storage_key(), storage_counters);
    }

    pub(crate) fn delete_relation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        storage_counters: StorageCounters,
    ) {
        self.delete_object(snapshot, relation.vertex().into_storage_key(), storage_counters);
    }

    fn delete_object(
        &self,
        snapshot: &mut impl WritableSnapshot,
        key: StorageKey<'_, BUFFER_KEY_INLINE>,
        storage_counters: StorageCounters,
    ) {
        let status = self.get_status(snapshot, StorageKey::Reference(key.as_reference()), storage_counters);
        let key = key.into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        match status {
            ConceptStatus::Inserted => snapshot.uninsert(key),
            ConceptStatus::Persisted => snapshot.delete(key),
            ConceptStatus::Deleted => (),
            ConceptStatus::Put => unreachable!("Encountered a `put` object"),
        }
    }

    pub(crate) fn delete_attribute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute: Attribute,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let key = attribute.vertex().into_storage_key();
        match self.get_status(snapshot, StorageKey::Reference(key.as_reference()), storage_counters.clone()) {
            ConceptStatus::Put => {
                self.unput_attribute(snapshot, &attribute, storage_counters.clone())?;
                if self.instance_exists(snapshot, &attribute, storage_counters)? {
                    snapshot.delete(key.into_owned_array());
                }
            }
            ConceptStatus::Persisted => snapshot.delete(key.into_owned_array()),
            ConceptStatus::Deleted => (),
            ConceptStatus::Inserted => unreachable!("Encountered an `insert`ed attribute"),
        }
        Ok(())
    }

    fn unput_attribute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute: &Attribute,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let value = match attribute
            .get_value(snapshot, self, storage_counters)
            .map_err(|error| ConceptWriteError::ConceptRead { typedb_source: error })?
        {
            Value::String(string) => ByteArray::copy(string.as_bytes()),
            _ => ByteArray::empty(),
        };
        let key = attribute.vertex().into_storage_key().into_owned_array();
        snapshot.unput_val(key, value);
        Ok(())
    }

    pub(crate) fn set_has_unordered(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: impl ObjectAPI,
        attribute: &Attribute,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.set_has_count(snapshot, owner, attribute, 1, storage_counters)
    }

    pub(crate) fn set_has_count(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: impl ObjectAPI,
        attribute: &Attribute,
        count: u64,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let attribute_type = attribute.type_();
        let value = attribute.get_value(snapshot, self, storage_counters.clone())?.into_owned();

        OperationTimeValidation::validate_value_type_matches_attribute_type_for_write(
            snapshot,
            self,
            attribute_type,
            value.value_type(),
            value.as_reference(),
        )?;

        OperationTimeValidation::validate_has_unique_constraint(
            snapshot,
            self,
            owner,
            attribute.type_(),
            value.as_reference(),
            storage_counters.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_has_regex_constraints(
            snapshot,
            self,
            owner,
            attribute_type,
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_has_range_constraints(
            snapshot,
            self,
            owner,
            attribute_type,
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_has_values_constraints(
            snapshot,
            self,
            owner,
            attribute_type,
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        if count == 0 {
            self.unset_has(snapshot, owner, attribute, storage_counters.clone())
        } else {
            let has = ThingEdgeHas::new(owner.vertex(), attribute.vertex());
            let has_reverse = ThingEdgeHasReverse::new(attribute.vertex(), owner.vertex());

            owner.set_required(snapshot, self, storage_counters.clone());
            attribute.set_required(snapshot, self, storage_counters.clone());
            snapshot.put_val(has.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));
            snapshot.put_val(has_reverse.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));
            Ok(())
        }
    }

    pub(crate) fn unset_has(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: impl ObjectAPI,
        attribute: &Attribute,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let owner_status = owner.get_status(snapshot, self, storage_counters.clone());
        let has = ThingEdgeHas::new(owner.vertex(), attribute.vertex()).into_storage_key();
        let has_array = has.clone().into_owned_array();
        let has_reverse_array =
            ThingEdgeHasReverse::new(attribute.vertex(), owner.vertex()).into_storage_key().into_owned_array();

        let snapshot_value_opt = Self::get_snapshot_put_value(snapshot, has.as_reference());
        if let Some(snapshot_value) = snapshot_value_opt {
            snapshot.unput_val(has_array.clone(), snapshot_value.clone());
            snapshot.unput_val(has_reverse_array.clone(), snapshot_value);
        }

        if owner_status != ConceptStatus::Inserted {
            if self
                .owner_has_attribute(snapshot, owner, attribute, storage_counters)
                .map_err(|typedb_source| ConceptWriteError::ConceptRead { typedb_source })?
            {
                snapshot.delete(has_array);
                snapshot.delete(has_reverse_array);
            }
        }

        Ok(())
    }

    pub(crate) fn set_has_ordered(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        attributes: Vec<Attribute>,
        _storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let attribute_value_type = attribute_type
            .get_value_type_without_source(snapshot, self.type_manager())?
            .expect("Value type validation should be implemented in the callers of this method!");
        let key = build_object_vertex_property_has_order(owner.vertex(), attribute_type.vertex());
        let storage_key = key.into_storage_key().into_owned_array();
        let value = encode_attribute_ids(
            attribute_value_type.category(),
            attributes.iter().map(|attr| attr.vertex().attribute_id()),
        );
        snapshot.put_val(storage_key.clone(), value);

        // must lock to fail concurrent transactions updating the same counters
        snapshot.exclusive_lock_add(storage_key.into_byte_array());
        Ok(())
    }

    pub(crate) fn unset_has_ordered(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        _storage_counters: StorageCounters,
    ) {
        let order_property = build_object_vertex_property_has_order(owner.vertex(), attribute_type.vertex());
        snapshot.delete(order_property.into_storage_key().into_owned_array())
    }

    pub(crate) fn put_links_unordered(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let count: u64 = 1;
        relation.set_required(snapshot, self, storage_counters.clone());
        player.set_required(snapshot, self, storage_counters.clone());

        // must be idempotent, so no lock required -- cannot fail
        let links = ThingEdgeLinks::new(relation.vertex(), player.vertex(), role_type.vertex());
        snapshot.put_val(links.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));

        let links_reverse =
            ThingEdgeLinks::new_reverse(player.clone().vertex(), relation.clone().vertex(), role_type.vertex());
        snapshot.put_val(links_reverse.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));

        if self.type_manager.relation_index_available(snapshot, relation.type_())? {
            self.relation_index_player_regenerate(
                snapshot,
                relation,
                Object::new(player.vertex()),
                role_type,
                1,
                storage_counters,
            )?;
        }
        Ok(())
    }

    pub(crate) fn set_links_ordered(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        role_type: RoleType,
        players: Vec<Object>,
    ) -> Result<(), Box<ConceptWriteError>> {
        let key = build_object_vertex_property_links_order(relation.vertex(), role_type.into_vertex());
        let storage_key = key.into_storage_key().into_owned_array();
        let value = encode_role_players(players.iter().map(|player| player.vertex()));
        snapshot.put_val(storage_key.clone(), value);

        // must lock to fail concurrent transactions updating the same counters
        snapshot.exclusive_lock_add(storage_key.into_byte_array());

        Ok(())
    }

    pub(crate) fn set_links_count(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
        count: u64,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        if count == 0 {
            self.unset_links(snapshot, relation, player, role_type, storage_counters)
        } else {
            let links = ThingEdgeLinks::new(relation.vertex(), player.vertex(), role_type.vertex());
            let links_reverse = ThingEdgeLinks::new_reverse(player.vertex(), relation.vertex(), role_type.vertex());

            relation.set_required(snapshot, self, storage_counters.clone());
            player.set_required(snapshot, self, storage_counters.clone());

            snapshot.put_val(links.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));
            snapshot.put_val(links_reverse.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));

            if self.type_manager.relation_index_available(snapshot, relation.type_())? {
                let player = Object::new(player.vertex());
                self.relation_index_player_regenerate(snapshot, relation, player, role_type, count, storage_counters)?
            }
            Ok(())
        }
    }

    /// Delete all counts of the specific role player in a given relation, and update indexes if required
    pub fn unset_links(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let relation_status = relation.get_status(&*snapshot, self, storage_counters.clone());
        let links = ThingEdgeLinks::new(relation.vertex(), player.vertex(), role_type.vertex()).into_storage_key();
        let links_array = links.clone().into_owned_array();
        let links_reverse_array = ThingEdgeLinks::new_reverse(player.vertex(), relation.vertex(), role_type.vertex())
            .into_storage_key()
            .into_owned_array();

        let snapshot_value_opt = Self::get_snapshot_put_value(snapshot, links.as_reference());
        if let Some(snapshot_value) = snapshot_value_opt {
            snapshot.unput_val(links_array.clone(), snapshot_value.clone());
            snapshot.unput_val(links_reverse_array.clone(), snapshot_value);
        }

        if relation_status != ConceptStatus::Inserted {
            if self
                .has_role_player(snapshot, relation, player, role_type, storage_counters.clone())
                .map_err(|typedb_source| ConceptWriteError::ConceptRead { typedb_source })?
            {
                snapshot.delete(links_array);
                snapshot.delete(links_reverse_array);
            }
        }

        if self
            .type_manager
            .relation_index_available(snapshot, relation.type_())
            .map_err(|error| ConceptWriteError::ConceptRead { typedb_source: error })?
        {
            self.relation_index_player_links_unset(snapshot, relation, player, role_type, storage_counters)?;
        }
        Ok(())
    }

    /// Add a player to a relation that supports duplicates
    /// Caller must provide a lock that prevents race conditions on the player counts on the relation
    pub(crate) fn increment_links_count(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let links = ThingEdgeLinks::new(relation.vertex(), player.vertex(), role_type.vertex());
        let count = snapshot
            .get_mapped(
                links.into_storage_key().as_reference(),
                |arr| decode_u64(arr.try_into().unwrap()),
                storage_counters.clone(),
            )
            .map_err(|snapshot_err| Box::new(ConceptReadError::SnapshotGet { source: snapshot_err }))?;

        #[cfg(debug_assertions)]
        {
            let links_reverse = ThingEdgeLinks::new_reverse(player.vertex(), relation.vertex(), role_type.vertex());
            let reverse_count = snapshot
                .get_mapped(
                    links_reverse.into_storage_key().as_reference(),
                    |arr| decode_u64(arr.try_into().unwrap()),
                    storage_counters.clone(),
                )
                .unwrap();
            debug_assert_eq!(&count, &reverse_count, "canonical and reverse links edge count mismatch!");
        }

        self.set_links_count(snapshot, relation, player, role_type, count.unwrap_or(0) + 1, storage_counters)
    }

    /// Remove a player from a relation that supports duplicates
    /// Caller must provide a lock that prevents race conditions on the player counts on the relation
    pub(crate) fn decrement_links_count(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
        decrement_count: u64,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let links = ThingEdgeLinks::new(relation.vertex(), player.vertex(), role_type.vertex());
        let count = snapshot
            .get_mapped(
                links.into_storage_key().as_reference(),
                |arr| decode_u64(arr.try_into().unwrap()),
                storage_counters.clone(),
            )
            .map_err(|snapshot_err| Box::new(ConceptReadError::SnapshotGet { source: snapshot_err }))?;

        #[cfg(debug_assertions)]
        {
            let links_reverse = ThingEdgeLinks::new_reverse(player.vertex(), relation.vertex(), role_type.vertex());
            let reverse_count = snapshot
                .get_mapped(
                    links_reverse.into_storage_key().as_reference(),
                    |arr| decode_u64(arr.try_into().unwrap()),
                    storage_counters.clone(),
                )
                .unwrap();
            debug_assert_eq!(&count, &reverse_count, "canonical and reverse links edge count mismatch!");
        }

        OperationTimeValidation::validate_links_count_to_remove_players(
            snapshot,
            self,
            relation,
            player,
            role_type,
            count,
            decrement_count,
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        debug_assert!(*count.as_ref().unwrap() >= decrement_count);
        self.set_links_count(snapshot, relation, player, role_type, count.unwrap() - decrement_count, storage_counters)
    }

    // TODO:
    // * Call index regenerations when cardinality changes in schema
    //   (create role type, set cardinality annotation, unset cardinality annotation, ...)
    // * Clean up all parts of a relation index to do with a specific role player
    //   after the player has been deleted.
    pub(crate) fn relation_index_player_links_unset(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        let players = relation
            .get_players(snapshot, self, storage_counters.clone())
            .map_ok(|(roleplayer, _count)| (roleplayer.player(), roleplayer.role_type()));
        for rp in players {
            let (rp_player, rp_role_type) = rp?;

            let index = ThingEdgeIndexedRelation::new(
                player.vertex(),
                rp_player.vertex(),
                relation.vertex(),
                role_type.vertex().type_id_(),
                rp_role_type.vertex().type_id_(),
            )
            .into_storage_key();
            let snapshot_index_value_opt = Self::get_snapshot_put_value(snapshot, index.as_reference());
            let index_array = index.clone().into_owned_array();
            if let Some(snapshot_index_value) = snapshot_index_value_opt {
                snapshot.unput_val(index_array.clone(), snapshot_index_value);
            }

            let index_reverse = ThingEdgeIndexedRelation::new(
                rp_player.vertex(),
                player.vertex(),
                relation.vertex(),
                rp_role_type.vertex().type_id_(),
                role_type.vertex().type_id_(),
            )
            .into_storage_key();
            let snapshot_index_reverse_value_opt = Self::get_snapshot_put_value(snapshot, index_reverse.as_reference());
            let index_reverse_array = index_reverse.into_owned_array();
            if let Some(snapshot_index_reverse_value) = snapshot_index_reverse_value_opt {
                snapshot.unput_val(index_reverse_array.clone(), snapshot_index_reverse_value);
            }

            let is_persisted = snapshot
                .get_mapped(index.as_reference(), |_| true, storage_counters.clone())
                .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
                .unwrap_or(false);

            if is_persisted {
                let is_same_rp = rp_player == Object::new(player.vertex()) && rp_role_type == role_type;
                snapshot.delete(index_array);
                if !is_same_rp {
                    snapshot.delete(index_reverse_array);
                }
            }
        }
        Ok(())
    }

    /// For N duplicate role players, the self-edges are available N-1 times.
    /// For N duplicate player 1, and M duplicate player 2 - from N to M has M index repetitions,
    /// while M to N has N index repetitions.
    pub(crate) fn relation_index_player_regenerate(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        player: Object,
        role_type: RoleType,
        count_for_player: u64,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        debug_assert_ne!(count_for_player, 0);
        let players = relation
            .get_players(snapshot, self, storage_counters)
            .map_ok(|(roleplayer, count)| (roleplayer.player(), roleplayer.role_type(), count));
        for rp in players {
            let (rp_player, rp_role_type, rp_count) = rp?;
            let is_same_rp = rp_player == player && rp_role_type == role_type;
            if is_same_rp {
                let repetitions = count_for_player - 1;
                if repetitions > 0 {
                    let index = ThingEdgeIndexedRelation::new(
                        player.vertex(),
                        player.vertex(),
                        relation.vertex(),
                        role_type.vertex().type_id_(),
                        role_type.vertex().type_id_(),
                    );
                    snapshot.put_val(
                        index.into_storage_key().into_owned_array(),
                        ByteArray::copy(&encode_u64(repetitions)),
                    );
                }
            } else {
                let rp_repetitions = rp_count;
                let index = ThingEdgeIndexedRelation::new(
                    player.vertex(),
                    rp_player.vertex(),
                    relation.vertex(),
                    role_type.vertex().type_id_(),
                    rp_role_type.vertex().type_id_(),
                );
                snapshot
                    .put_val(index.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(rp_repetitions)));
                let player_repetitions = count_for_player;
                let index_reverse = ThingEdgeIndexedRelation::new(
                    rp_player.vertex(),
                    player.vertex(),
                    relation.vertex(),
                    rp_role_type.vertex().type_id_(),
                    role_type.vertex().type_id_(),
                );
                snapshot.put_val(
                    index_reverse.into_storage_key().into_owned_array(),
                    ByteArray::copy(&encode_u64(player_repetitions)),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn get_snapshot_put_value(
        snapshot: &mut impl WritableSnapshot,
        key: StorageKeyReference<'_>,
    ) -> Option<ByteArray<BUFFER_VALUE_INLINE>> {
        match snapshot.get_write(key).cloned()? {
            Write::Put { value, .. } => Some(value),
            Write::Delete => None,
            Write::Insert { .. } => {
                unreachable!("Encountered an `insert` while a `put` was expected in the snapshot.")
            }
        }
    }
}
