/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{Bound, HashMap, HashSet},
    iter::once,
    ops::RangeBounds,
    sync::Arc,
};

use bytes::{byte_array::ByteArray, Bytes};
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeHasReverse, ThingEdgeLinks, ThingEdgeRolePlayerIndex},
            property::{build_object_vertex_property_has_order, build_object_vertex_property_links_order},
            vertex_attribute::{AttributeID, AttributeVertex},
            vertex_generator::ThingVertexGenerator,
            vertex_object::ObjectVertex,
            ThingVertex,
        },
        type_::{
            property::{TypeVertexProperty, TypeVertexPropertyEncoding},
            vertex::{PrefixedTypeVertexEncoding, TypeVertexEncoding},
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
        long_bytes::LongBytes,
        primitive_encoding::{decode_u64, encode_u64},
        string_bytes::StringBytes,
        struct_bytes::StructBytes,
        value::Value,
        value_struct::{StructIndexEntry, StructIndexEntryKey, StructValue},
        value_type::{ValueType, ValueTypeCategory},
        ValueEncodable,
    },
    AsBytes, Keyable,
};
use itertools::{Itertools, MinMaxResult};
use lending_iterator::{AsHkt, LendingIterator};
use resource::constants::{
    encoding::StructFieldIDUInt,
    snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
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
        attribute::{Attribute, AttributeIterator, AttributeOwnerIterator},
        decode_attribute_ids, decode_role_players, encode_attribute_ids, encode_role_players,
        entity::Entity,
        object::{HasAttributeIterator, HasIterator, HasReverseIterator, Object, ObjectAPI},
        relation::{
            IndexedPlayersIterator, LinksIterator, Relation, RelationRoleIterator, RolePlayer, RolePlayerIterator,
        },
        statistics::Statistics,
        thing_manager::validation::{
            commit_time_validation::{collect_errors, CommitTimeValidation},
            operation_time_validation::OperationTimeValidation,
            DataValidationError,
        },
        HKInstance, ThingAPI,
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
    fn get_instances_in<'a, T: HKInstance>(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_type: <T::HktSelf<'a> as ThingAPI<'a>>::TypeAPI<'a>,
    ) -> InstanceIterator<T> {
        if thing_type.is_abstract(snapshot, self.type_manager()).unwrap() {
            return InstanceIterator::empty();
        }

        let prefix = <T::HktSelf<'_> as ThingAPI>::prefix_for_type(thing_type.clone());
        let storage_key_prefix =
            <T::HktSelf<'_> as ThingAPI<'_>>::Vertex::build_prefix_type(prefix, thing_type.vertex().type_id_());
        let snapshot_iterator = snapshot
            .iterate_range(KeyRange::new_within(RangeStart::Inclusive(storage_key_prefix), prefix.fixed_width_keys()));
        InstanceIterator::new(snapshot_iterator)
    }

    fn get_instances<T: HKInstance>(&self, snapshot: &impl ReadableSnapshot) -> InstanceIterator<T> {
        let (prefix_start, prefix_end_exclusive) = <T::HktSelf<'_> as ThingAPI<'_>>::PREFIX_RANGE_INCLUSIVE;
        let key_start = <T::HktSelf<'_> as ThingAPI<'_>>::Vertex::build_prefix_prefix(prefix_start);
        let key_end = <T::HktSelf<'_> as ThingAPI<'_>>::Vertex::build_prefix_prefix(prefix_end_exclusive);
        let snapshot_iterator = snapshot.iterate_range(KeyRange::new_variable_width(
            RangeStart::Inclusive(key_start),
            RangeEnd::EndPrefixInclusive(key_end),
        ));
        InstanceIterator::new(snapshot_iterator)
    }

    pub fn get_entities(&self, snapshot: &impl ReadableSnapshot) -> InstanceIterator<AsHkt![Entity<'_>]> {
        self.get_instances::<Entity<'static>>(snapshot)
    }

    pub fn get_relations(&self, snapshot: &impl ReadableSnapshot) -> InstanceIterator<AsHkt![Relation<'_>]> {
        self.get_instances::<Relation<'static>>(snapshot)
    }

    pub fn get_entities_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_: EntityType,
    ) -> InstanceIterator<AsHkt![Entity<'_>]> {
        self.get_instances_in(snapshot, type_)
    }

    pub fn get_relations_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_: RelationType,
    ) -> InstanceIterator<AsHkt![Relation<'_>]> {
        self.get_instances_in(snapshot, type_)
    }

    pub fn get_objects_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        object_type: ObjectType,
    ) -> InstanceIterator<AsHkt![Object<'_>]> {
        self.get_instances_in(snapshot, object_type)
    }

    pub fn instance_exists<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        instance: impl ThingAPI<'a>,
    ) -> Result<bool, Box<ConceptReadError>> {
        let storage_key = instance.into_vertex().into_storage_key();
        snapshot
            .get::<BUFFER_KEY_INLINE>(storage_key.as_reference())
            .map(|value| value.is_some())
            .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))
    }

    pub(crate) fn get_relations_roles<'o>(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: &impl ObjectAPI<'o>,
    ) -> RelationRoleIterator {
        let prefix = ThingEdgeLinks::prefix_reverse_from_player(player.vertex());
        RelationRoleIterator::new(snapshot.iterate_range(KeyRange::new_within(
            RangeStart::Inclusive(prefix),
            ThingEdgeLinks::FIXED_WIDTH_ENCODING_REVERSE,
        )))
    }

    pub(crate) fn get_relations_player<'o>(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: &'o impl ObjectAPI<'o>,
    ) -> impl for<'a> LendingIterator<Item<'a> = Result<Relation<'a>, Box<ConceptReadError>>> {
        self.get_relations_roles(snapshot, player).map::<Result<Relation<'_>, _>, _>(|res| {
            let (rel, _, _) = res?;
            Ok(rel)
        })
    }

    pub(crate) fn get_relations_player_role<'o>(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: &'o impl ObjectAPI<'o>,
        role_type: RoleType,
    ) -> impl for<'a> LendingIterator<Item<'a> = Result<(Relation<'a>, u64), Box<ConceptReadError>>> {
        self.get_relations_roles(snapshot, player).filter_map::<Result<(Relation<'_>, u64), _>, _>(move |item| {
            match item {
                Ok((rel, role, count)) => (role == role_type).then_some(Ok((rel, count))),
                Err(error) => Some(Err(error)),
            }
        })
    }

    pub fn get_attributes<'this, Snapshot: ReadableSnapshot>(
        &'this self,
        snapshot: &'this Snapshot,
    ) -> Result<AttributeIterator<InstanceIterator<AsHkt![Attribute<'_>]>>, Box<ConceptReadError>> {
        let has_reverse_start = ThingEdgeHasReverse::prefix_from_prefix(Prefix::VertexAttribute);
        let range =
            KeyRange::new_within(RangeStart::Inclusive(has_reverse_start), Prefix::VertexAttribute.fixed_width_keys());
        let has_reverse_iterator_buffer = snapshot.iterate_writes_range(range.clone());
        let has_reverse_iterator_storage = snapshot.iterate_storage_range(range);
        Ok(AttributeIterator::new(
            self.get_instances::<Attribute<'_>>(snapshot),
            has_reverse_iterator_buffer,
            has_reverse_iterator_storage,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    pub fn get_attributes_in<'this>(
        &'this self,
        snapshot: &'this impl ReadableSnapshot,
        attribute_type: AttributeType,
    ) -> Result<AttributeIterator<InstanceIterator<AsHkt![Attribute<'_>]>>, Box<ConceptReadError>> {
        let attribute_value_type =
            attribute_type.get_value_type_without_source(snapshot, self.type_manager.as_ref())?;
        let Some(_value_type) = attribute_value_type.as_ref() else {
            return Ok(AttributeIterator::new_empty());
        };

        let has_reverse_prefix = ThingEdgeHasReverse::prefix_from_attribute_type(attribute_type.vertex().type_id_());
        let range =
            KeyRange::new_within(RangeStart::Inclusive(has_reverse_prefix), ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        let has_reverse_iterator_buffer = snapshot.iterate_writes_range(range.clone());
        let has_reverse_iterator_storage = snapshot.iterate_storage_range(range);

        Ok(AttributeIterator::new(
            self.get_instances_in(snapshot, attribute_type.into_owned()),
            has_reverse_iterator_buffer,
            has_reverse_iterator_storage,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    pub(crate) fn get_attribute_value<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: &'a Attribute<'a>,
    ) -> Result<Value<'static>, Box<ConceptReadError>> {
        match attribute.vertex().attribute_id() {
            AttributeID::Boolean(id) => Ok(Value::Boolean(id.read().as_bool())),
            AttributeID::Long(id) => Ok(Value::Long(id.read().as_i64())),
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
                        .get_mapped(attribute.vertex().into_storage_key().as_reference(), |bytes| {
                            String::from(StringBytes::new(Bytes::<1>::Reference(bytes)).as_str())
                        })
                        .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))?
                        .ok_or(ConceptReadError::CorruptMissingMandatoryAttributeValue)?
                };
                Ok(Value::String(Cow::Owned(string)))
            }
            AttributeID::Struct(_id) => {
                let struct_value = snapshot
                    .get_mapped(attribute.vertex().into_storage_key().as_reference(), |bytes| {
                        StructBytes::new(Bytes::<1>::Reference(bytes)).as_struct()
                    })
                    .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))?
                    .ok_or(ConceptReadError::CorruptMissingMandatoryAttributeValue)?;
                Ok(Value::Struct(Cow::Owned(struct_value)))
            }
        }
    }

    pub fn get_attribute_with_value(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<Option<Attribute<'static>>, Box<ConceptReadError>> {
        let value_type = value.value_type();
        let attribute_value_type = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?;
        if attribute_value_type.is_none() || attribute_value_type.as_ref().unwrap() != &value_type {
            return Ok(None);
        }

        let attribute = match value_type {
            | ValueType::Boolean
            | ValueType::Long
            | ValueType::Double
            | ValueType::Decimal
            | ValueType::Date
            | ValueType::DateTime
            | ValueType::DateTimeTZ
            | ValueType::Duration => {
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
                        Ok(Some(id)) => Attribute::new(AttributeVertex::build(
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
                    Ok(Some(id)) => {
                        let attribute = Attribute::new(AttributeVertex::build(
                            attribute_type.vertex().type_id_(),
                            AttributeID::Struct(id),
                        ));
                        attribute
                    }
                    Ok(None) => return Ok(None),
                    Err(err) => return Err(Box::new(ConceptReadError::SnapshotIterate { source: err })),
                }
            }
        };

        Ok(Some(attribute))
    }

    pub fn get_attributes_in_range<'this>(
        &'this self,
        snapshot: &'this impl ReadableSnapshot,
        attribute_type: AttributeType,
        range: &impl RangeBounds<Value<'this>>,
    ) -> Result<AttributeIterator<InstanceIterator<AsHkt![Attribute<'_>]>>, Box<ConceptReadError>> {
        if matches!(range.start_bound(), Bound::Unbounded) && matches!(range.end_bound(), Bound::Unbounded) {
            return self.get_attributes_in(snapshot, attribute_type);
        }
        let Some(attribute_value_type) = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?
        else {
            return Ok(AttributeIterator::new_empty());
        };

        let Some((range_start, range_end)) = Self::get_value_range(attribute_value_type, range, |value| {
            AttributeVertex::build_prefix_for_value(
                attribute_type.vertex().type_id_(),
                value,
                self.vertex_generator.hasher(),
            )
        })?
        else {
            return Ok(AttributeIterator::new_empty());
        };
        let key_range_start = match range_start {
            Bound::Included(start) => RangeStart::Inclusive(start),
            Bound::Excluded(start) => RangeStart::Exclusive(start),
            Bound::Unbounded => RangeStart::Inclusive(
                AttributeVertex::build_prefix_type(AttributeVertex::PREFIX, attribute_type.vertex().type_id_())
                    .resize_to::<BUFFER_KEY_INLINE>(),
            ),
        };
        let key_range_end = match range_end {
            Bound::Included(end) => RangeEnd::EndPrefixInclusive(end),
            Bound::Excluded(end) => RangeEnd::EndPrefixExclusive(end),
            Bound::Unbounded => {
                let prefix =
                    AttributeVertex::build_prefix_type(AttributeVertex::PREFIX, attribute_type.vertex().type_id_());
                let keyspace = prefix.keyspace_id();
                let mut array = prefix.into_bytes().into_array();
                array.increment().unwrap();
                let prefix_key = StorageKey::Array(StorageKeyArray::new_raw(keyspace, array));
                RangeEnd::EndPrefixExclusive(prefix_key.resize_to::<BUFFER_KEY_INLINE>())
            }
        };

        let has_reverse_start_prefix = ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(
            key_range_start.get_value().as_reference().bytes(),
        );
        let has_reverse_end_prefix = match &key_range_end {
            RangeEnd::WithinStartAsPrefix => unreachable!(),
            RangeEnd::EndPrefixInclusive(end) => RangeEnd::EndPrefixInclusive(
                ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(end.as_reference().bytes()),
            ),
            RangeEnd::EndPrefixExclusive(end) => RangeEnd::EndPrefixExclusive(
                ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(end.as_reference().bytes()),
            ),
            RangeEnd::Unbounded => {
                // we don't have to bound this since it will only be consumed while the Attributes are read
                RangeEnd::Unbounded
            }
        };
        let has_reverse_range =
            KeyRange::new_variable_width(RangeStart::Inclusive(has_reverse_start_prefix), has_reverse_end_prefix);
        let has_reverse_iterator_buffer = snapshot.iterate_writes_range(has_reverse_range.clone());
        let has_reverse_iterator_storage = snapshot.iterate_storage_range(has_reverse_range);

        let range = KeyRange::new_variable_width(key_range_start, key_range_end);
        let snapshot_iterator = snapshot.iterate_range(range);
        let attributes_iterator = InstanceIterator::new(snapshot_iterator);
        Ok(AttributeIterator::new(
            attributes_iterator,
            has_reverse_iterator_buffer,
            has_reverse_iterator_storage,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    fn get_value_range<'a, PrefixFn, Key>(
        expected_value_type: ValueType,
        range: &impl RangeBounds<Value<'a>>,
        prefix_constructor: PrefixFn,
    ) -> Result<Option<(Bound<Key>, Bound<Key>)>, Box<ConceptReadError>>
    where
        PrefixFn: for<'b> Fn(Value<'b>) -> Key,
    {
        fn get_value_type(bound: Bound<&Value<'_>>) -> Option<ValueType> {
            match bound {
                Bound::Included(value) | Bound::Excluded(value) => Some(value.value_type()),
                Bound::Unbounded => None,
            }
        }
        let start_value_type = get_value_type(range.start_bound());
        let end_value_type = get_value_type(range.end_bound());
        debug_assert!(start_value_type == end_value_type || start_value_type.is_none() || end_value_type.is_none());
        let range_value_type = start_value_type.unwrap_or_else(|| end_value_type.unwrap());
        if !range_value_type.is_approximately_castable_to(&expected_value_type) {
            return Ok(None);
        }
        let value_type = expected_value_type;

        let range_start = range.start_bound().map(|value| {
            let value = if value_type != range_value_type {
                value.as_reference().approximate_cast_lower_bound(&value_type).unwrap()
            } else {
                value.as_reference()
            };
            prefix_constructor(value)
        });
        let range_end = range.end_bound().map(|value| {
            let value = if value_type != range_value_type {
                value.as_reference().approximate_cast_upper_bound(&value_type).unwrap()
            } else {
                value.as_reference()
            };
            prefix_constructor(value)
        });
        Ok(Some((range_start, range_end)))
    }

    fn get_attribute_with_value_inline(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<Option<Attribute<'static>>, Box<ConceptReadError>> {
        debug_assert!(AttributeID::is_inlineable(value.as_reference()));
        let attribute_value_type = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?;
        if attribute_value_type.is_none() || attribute_value_type.as_ref().unwrap() != &value.value_type() {
            return Ok(None);
        }
        let vertex = AttributeVertex::build(attribute_type.vertex().type_id_(), AttributeID::build_inline(value));
        snapshot
            .get_mapped(vertex.clone().into_storage_key().as_reference(), |_| Attribute::new(vertex.clone()))
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))
    }

    pub(crate) fn has_attribute_with_value<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<bool, Box<ConceptReadError>> {
        let value_type = value.value_type();
        OperationTimeValidation::validate_value_type_matches_attribute_type_for_read(
            snapshot,
            self,
            attribute_type.clone(),
            value_type.clone(),
        )?;

        let vertex = if AttributeID::is_inlineable(value.as_reference()) {
            // don't need to do an extra lookup to get the attribute vertex - if it exists, it will have this ID
            AttributeVertex::build(attribute_type.vertex().type_id_(), AttributeID::build_inline(value))
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
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or(false);
        Ok(has_exists)
    }

    pub(crate) fn has_attribute<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: &impl ObjectAPI<'a>,
        attribute: Attribute<'_>,
    ) -> Result<bool, Box<ConceptReadError>> {
        let has = ThingEdgeHas::build(owner.vertex(), attribute.vertex());
        let has_exists = snapshot
            .get_mapped(has.into_storage_key().as_reference(), |_value| true)
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or(false);
        Ok(has_exists)
    }

    pub fn get_has_from_owner_type_range_unordered(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner_type_range: KeyRange<ObjectType>,
    ) -> HasIterator {
        let range = owner_type_range
            .map(|type_| ThingEdgeHas::prefix_from_type(type_.into_vertex()), |_| ThingEdgeHas::FIXED_WIDTH_ENCODING);
        HasIterator::new(snapshot.iterate_range(range))
    }

    pub fn get_has_reverse(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
    ) -> Result<HasReverseIterator, Box<ConceptReadError>> {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute_type(attribute_type.vertex().type_id_());
        let range = KeyRange::new_within(RangeStart::Inclusive(prefix), ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        Ok(HasReverseIterator::new(snapshot.iterate_range(range)))
    }

    pub fn get_has_reverse_in_range<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
        range: &impl RangeBounds<Value<'a>>,
    ) -> Result<HasReverseIterator, Box<ConceptReadError>> {
        if matches!(range.start_bound(), Bound::Unbounded) && matches!(range.end_bound(), Bound::Unbounded) {
            return self.get_has_reverse(snapshot, attribute_type);
        }
        let Some(attribute_value_type) = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?
        else {
            return Ok(HasReverseIterator::new_empty());
        };

        let Some((range_start, range_end)) = Self::get_value_range(attribute_value_type, range, |value| {
            let attribute_vertex_prefix = AttributeVertex::build_prefix_for_value(
                attribute_type.vertex().type_id_(),
                value,
                self.vertex_generator.hasher(),
            );
            ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(attribute_vertex_prefix.as_reference().bytes())
        })?
        else {
            return Ok(HasReverseIterator::new_empty());
        };

        let key_range_start = match range_start {
            Bound::Included(start) => RangeStart::Inclusive(start),
            Bound::Excluded(start) => RangeStart::Exclusive(start),
            Bound::Unbounded => RangeStart::Inclusive(
                ThingEdgeHasReverse::prefix_from_attribute_type(attribute_type.vertex().type_id_())
                    .resize_to::<{ ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }>(),
            ),
        };
        let key_range_end = match range_end {
            Bound::Included(end) => RangeEnd::EndPrefixInclusive(end),
            Bound::Excluded(end) => RangeEnd::EndPrefixExclusive(end),
            Bound::Unbounded => {
                let prefix = ThingEdgeHasReverse::prefix_from_attribute_type(attribute_type.vertex().type_id_());
                let keyspace = prefix.keyspace_id();
                let mut array = prefix.into_bytes().into_array();
                array.increment().unwrap();
                let prefix_key = StorageKey::Array(StorageKeyArray::new_raw(keyspace, array));
                RangeEnd::EndPrefixExclusive(
                    prefix_key.resize_to::<{ ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }>(),
                )
            }
        };
        let key_range = if ThingEdgeHasReverse::FIXED_WIDTH_ENCODING {
            KeyRange::new_fixed_width(key_range_start, key_range_end)
        } else {
            KeyRange::new_variable_width(key_range_start, key_range_end)
        };
        Ok(HasReverseIterator::new(snapshot.iterate_range(key_range)))
    }

    pub fn get_attributes_by_struct_field<'this, Snapshot: ReadableSnapshot>(
        &'this self,
        snapshot: &'this Snapshot,
        attribute_type: AttributeType,
        path_to_field: Vec<StructFieldIDUInt>,
        value: Value<'_>,
    ) -> Result<
        impl for<'a> LendingIterator<Item<'a> = Result<Attribute<'a>, Box<ConceptReadError>>>,
        Box<ConceptReadError>,
    > {
        debug_assert!({
            let value_type =
                attribute_type.get_value_type_without_source(snapshot, &self.type_manager).unwrap().unwrap();
            value_type.category() == ValueTypeCategory::Struct
        });

        let prefix = StructIndexEntry::build_prefix_typeid_path_value(
            snapshot,
            &self.vertex_generator,
            &path_to_field,
            &value,
            &attribute_type.vertex(),
        )
        .map_err(|source| Box::new(ConceptReadError::SnapshotIterate { source }))?;
        let index_attribute_iterator = snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(prefix),
                Prefix::IndexValueToStruct.fixed_width_keys(),
            ))
            .map::<Result<Attribute<'_>, _>, _>(|result| {
                result
                    .map(|(key, _)| {
                        Attribute::new(
                            StructIndexEntry::new(StructIndexEntryKey::new(key.into_bytes()), None).attribute_vertex(),
                        )
                    })
                    .map_err(|err| Box::new(ConceptReadError::SnapshotIterate { source: err }))
            });

        let has_reverse_prefix = ThingEdgeHasReverse::prefix_from_attribute_type(attribute_type.vertex().type_id_());
        let range =
            KeyRange::new_within(RangeStart::Inclusive(has_reverse_prefix), ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        let has_reverse_iterator_buffer = snapshot.iterate_writes_range(range.clone());
        let has_reverse_iterator_storage = snapshot.iterate_storage_range(range);

        let iter = AttributeIterator::new(
            index_attribute_iterator,
            has_reverse_iterator_buffer,
            has_reverse_iterator_storage,
            self.type_manager.get_independent_attribute_types(snapshot)?,
        );
        Ok(iter)
    }

    pub(crate) fn get_has_from_thing_unordered<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: &impl ObjectAPI<'a>,
    ) -> HasAttributeIterator {
        let prefix = ThingEdgeHas::prefix_from_object(owner.vertex());
        HasAttributeIterator::new(
            snapshot
                .iterate_range(KeyRange::new_within(RangeStart::Inclusive(prefix), ThingEdgeHas::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn get_has_from_thing_to_type_unordered<'this, 'a>(
        &'this self,
        snapshot: &'this impl ReadableSnapshot,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType,
    ) -> HasAttributeIterator {
        let prefix = ThingEdgeHas::prefix_from_object_to_type(owner.vertex(), attribute_type.into_vertex().type_id_());
        HasAttributeIterator::new(
            snapshot
                .iterate_range(KeyRange::new_within(RangeStart::Inclusive(prefix), ThingEdgeHas::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn get_has_from_thing_to_type_ordered<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType,
    ) -> Result<Vec<Attribute<'static>>, Box<ConceptReadError>> {
        let key = build_object_vertex_property_has_order(owner.vertex(), attribute_type.vertex());
        let attribute_value_type = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?;
        let value_type = match attribute_value_type.as_ref() {
            None => return Ok(Vec::new()),
            Some(value_type) => value_type,
        };
        let attributes = snapshot
            .get_mapped(key.into_storage_key().as_reference(), |bytes| {
                decode_attribute_ids(value_type.category(), bytes)
                    .map(|id| Attribute::new(AttributeVertex::build(attribute_type.vertex().type_id_(), id)))
                    .collect()
            })
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or_else(Vec::new);
        Ok(attributes)
    }

    pub(crate) fn get_has_from_thing_to_type_range_unordered<'this, 'a>(
        &'this self,
        snapshot: &'this impl ReadableSnapshot,
        owner: &impl ObjectAPI<'a>,
        attribute_types: impl Iterator<Item = AttributeType>,
    ) -> Result<HasIterator, Box<ConceptReadError>> {
        let (min_type_id, max_type_id) = match attribute_types.minmax() {
            MinMaxResult::NoElements => unreachable!(),
            MinMaxResult::OneElement(type_) => (type_.vertex().type_id_(), type_.vertex().type_id_()),
            MinMaxResult::MinMax(min, max) => (min.vertex().type_id_(), max.vertex().type_id_()),
        };
        let min_edge_prefix = ThingEdgeHas::prefix_from_object_to_type(owner.vertex(), min_type_id);
        let max_edge_prefix = ThingEdgeHas::prefix_from_object_to_type(owner.vertex(), max_type_id);
        let range = if min_edge_prefix != max_edge_prefix {
            KeyRange::new_variable_width(
                RangeStart::Inclusive(min_edge_prefix),
                RangeEnd::EndPrefixInclusive(max_edge_prefix),
            )
        } else {
            KeyRange::new_within(RangeStart::Inclusive(min_edge_prefix), ThingEdgeHas::FIXED_WIDTH_ENCODING)
        };
        Ok(HasIterator::new(snapshot.iterate_range(range)))
    }

    pub(crate) fn get_owners(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: Attribute<'_>,
    ) -> AttributeOwnerIterator {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute.into_vertex());
        AttributeOwnerIterator::new(snapshot.iterate_range(KeyRange::new_within(
            RangeStart::Inclusive(prefix),
            ThingEdgeHasReverse::FIXED_WIDTH_ENCODING,
        )))
    }

    pub(crate) fn get_owners_by_type<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: Attribute<'_>,
        owner_type: impl ObjectTypeAPI<'a>,
    ) -> AttributeOwnerIterator {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute_to_type(attribute.into_vertex(), owner_type.vertex());
        AttributeOwnerIterator::new(snapshot.iterate_range(KeyRange::new_within(
            RangeStart::Inclusive(prefix),
            ThingEdgeHasReverse::FIXED_WIDTH_ENCODING,
        )))
    }

    pub fn get_has_reverse_by_attribute_and_owner_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: Attribute<'_>,
        owner_type_range: KeyRange<ObjectType>,
    ) -> HasReverseIterator {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute_to_type_range(
            attribute.into_vertex(),
            owner_type_range.start().get_value().vertex(),
            owner_type_range.end().clone().map(|object_type| object_type.into_vertex()),
        );
        HasReverseIterator::new(snapshot.iterate_range(prefix))
    }

    pub(crate) fn has_owners(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: Attribute<'_>,
        buffered_only: bool,
    ) -> bool {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute.into_vertex());
        snapshot.any_in_range(
            KeyRange::new_within(RangeStart::Inclusive(prefix), ThingEdgeHasReverse::FIXED_WIDTH_ENCODING),
            buffered_only,
        )
    }

    pub(crate) fn has_links(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation<'_>,
        buffered_only: bool, // FIXME use enums
    ) -> bool {
        let prefix = ThingEdgeLinks::prefix_from_relation(relation.into_vertex());
        snapshot.any_in_range(
            KeyRange::new_within(RangeStart::Inclusive(prefix), ThingEdgeLinks::FIXED_WIDTH_ENCODING),
            buffered_only,
        )
    }

    pub fn get_links_by_relation_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type_range: KeyRange<RelationType>,
    ) -> LinksIterator {
        let range = relation_type_range.map(
            |type_| ThingEdgeLinks::prefix_from_relation_type(type_.into_vertex()),
            |_| ThingEdgeLinks::FIXED_WIDTH_ENCODING,
        );
        LinksIterator::new(snapshot.iterate_range(range))
    }

    pub fn get_links_by_relation_and_player_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation<'_>,
        player_type_range: KeyRange<ObjectType>,
    ) -> LinksIterator {
        let range = player_type_range.map(
            |type_| ThingEdgeLinks::prefix_from_relation_player_type(relation.vertex(), type_.into_vertex()),
            |_| ThingEdgeLinks::FIXED_WIDTH_ENCODING,
        );
        LinksIterator::new(snapshot.iterate_range(range))
    }

    pub fn get_links_by_relation_and_player<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
    ) -> LinksIterator {
        let prefix = ThingEdgeLinks::prefix_from_relation_player(relation.into_vertex(), player.into_vertex());
        LinksIterator::new(
            snapshot.iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(prefix),
                ThingEdgeLinks::FIXED_WIDTH_ENCODING,
            )),
        )
    }

    pub fn get_links_reverse_by_player_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        player_type_range: KeyRange<ObjectType>,
    ) -> LinksIterator {
        let range = player_type_range.map(
            |type_| ThingEdgeLinks::prefix_reverse_from_player_type(type_.into_vertex()),
            |_| ThingEdgeLinks::FIXED_WIDTH_ENCODING,
        );
        LinksIterator::new(snapshot.iterate_range(range))
    }

    pub fn get_links_reverse_by_player_and_relation_type_range<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: impl ObjectAPI<'a>,
        relation_type_range: KeyRange<RelationType>,
    ) -> LinksIterator {
        let range = relation_type_range.map(
            |type_| ThingEdgeLinks::prefix_reverse_from_player_relation_type(player.vertex(), type_.into_vertex()),
            |_| ThingEdgeLinks::FIXED_WIDTH_ENCODING,
        );
        LinksIterator::new(snapshot.iterate_range(range))
    }

    pub(crate) fn has_role_player<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation<'_>,
        player: &impl ObjectAPI<'a>,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.vertex());
        let links_exists = snapshot
            .get_mapped(links.into_storage_key().as_reference(), |_| true)
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or(false);
        Ok(links_exists)
    }

    pub(crate) fn get_role_players(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation<'_>,
    ) -> RolePlayerIterator {
        let prefix = ThingEdgeLinks::prefix_from_relation(relation.into_vertex());
        RolePlayerIterator::new(
            snapshot.iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(prefix),
                ThingEdgeLinks::FIXED_WIDTH_ENCODING,
            )),
        )
    }

    pub(crate) fn get_role_players_ordered(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation<'_>,
        role_type: RoleType,
    ) -> Result<Vec<Object<'static>>, Box<ConceptReadError>> {
        let key = build_object_vertex_property_links_order(relation.into_vertex(), role_type.into_vertex());
        let players = snapshot
            .get_mapped(key.into_storage_key().as_reference(), |bytes| {
                decode_role_players(bytes).map(|vertex| Object::new(vertex).into_owned()).collect()
            })
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or_else(Vec::new);
        Ok(players)
    }

    pub(crate) fn get_role_players_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation<'_>,
        role_type: RoleType,
    ) -> impl for<'x> LendingIterator<Item<'x> = Result<(RolePlayer<'x>, u64), Box<ConceptReadError>>> {
        self.get_role_players(snapshot, relation).filter_map::<Result<(RolePlayer<'_>, u64), _>, _>(move |item| {
            match item {
                Ok((role_player, count)) => (role_player.role_type() == role_type).then_some(Ok((role_player, count))),
                Err(error) => Some(Err(error)),
            }
        })
    }

    pub(crate) fn get_indexed_players<'a>(
        &'a self,
        snapshot: &'a impl ReadableSnapshot,
        from: Object<'_>,
    ) -> IndexedPlayersIterator {
        let prefix = ThingEdgeRolePlayerIndex::prefix_from(from.vertex());
        IndexedPlayersIterator::new(snapshot.iterate_range(KeyRange::new_within(
            RangeStart::Inclusive(prefix),
            ThingEdgeRolePlayerIndex::FIXED_WIDTH_ENCODING,
        )))
    }

    pub(crate) fn get_status(
        &self,
        snapshot: &impl ReadableSnapshot,
        key: StorageKey<'_, BUFFER_KEY_INLINE>,
    ) -> ConceptStatus {
        snapshot
            .get_write(key.as_reference())
            .map(|write| match write {
                Write::Insert { .. } => ConceptStatus::Inserted,
                Write::Put { .. } => ConceptStatus::Put,
                Write::Delete => ConceptStatus::Deleted,
            })
            .unwrap_or_else(|| ConceptStatus::Persisted)
    }

    pub(crate) fn object_exists<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        object: &impl ObjectAPI<'a>,
    ) -> Result<bool, Box<ConceptReadError>> {
        snapshot
            .contains(object.vertex().into_storage_key().as_reference())
            .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))
    }

    pub(crate) fn type_exists<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeAPI<'a>,
    ) -> Result<bool, Box<ConceptReadError>> {
        snapshot
            .contains(type_.vertex().into_storage_key().as_reference())
            .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))
    }
}

impl ThingManager {
    pub(crate) fn lock_existing_object<'a>(&self, snapshot: &mut impl WritableSnapshot, object: impl ObjectAPI<'a>) {
        snapshot.unmodifiable_lock_add(object.into_vertex().into_storage_key().into_owned_array())
    }

    pub(crate) fn lock_existing_attribute(&self, snapshot: &mut impl WritableSnapshot, attribute: Attribute<'_>) {
        snapshot.unmodifiable_lock_add(attribute.into_vertex().into_storage_key().into_owned_array())
    }

    pub fn finalise(&self, snapshot: &mut impl WritableSnapshot) -> Result<(), Vec<Box<ConceptWriteError>>> {
        self.validate(snapshot)?;

        self.cleanup_relations(snapshot).map_err(|err| vec![err])?;
        self.cleanup_attributes(snapshot).map_err(|err| vec![err])?;

        match self.create_commit_locks(snapshot) {
            Ok(_) => Ok(()),
            Err(error) => Err(vec![Box::new(ConceptWriteError::ConceptRead { source: error })]),
        }
    }

    fn create_commit_locks(&self, snapshot: &mut impl WritableSnapshot) -> Result<(), Box<ConceptReadError>> {
        // TODO: Should not collect here (iterate_writes() already copies)
        for (key, _write) in snapshot.iterate_writes().collect_vec() {
            let key_reference = StorageKeyReference::from(&key);
            if ThingEdgeHas::is_has(key_reference) {
                let has = ThingEdgeHas::new(Bytes::Reference(key_reference.bytes()));
                let object = Object::new(has.from()).into_owned();
                let attribute = Attribute::new(has.to()).into_owned();
                let attribute_type = attribute.type_();

                self.add_exclusive_lock_for_unique_constraint(snapshot, &object, attribute)?;
                self.add_exclusive_lock_for_owns_cardinality_constraint(snapshot, &object, attribute_type)?;
            } else if ThingEdgeLinks::is_links(key_reference) {
                let role_player = ThingEdgeLinks::new(Bytes::Reference(key_reference.bytes()));
                let relation = Relation::new(role_player.relation()).into_owned();
                let player = Object::new(role_player.player()).into_owned();
                let role_type = RoleType::build_from_type_id(role_player.role_id()).into_owned();

                self.add_exclusive_lock_for_plays_cardinality_constraint(snapshot, &player, role_type.clone())?;
                self.add_exclusive_lock_for_relates_cardinality_constraint(snapshot, &relation, role_type)?;
            }
        }

        Ok(())
    }

    fn add_exclusive_lock_for_unique_constraint<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: &Object<'a>,
        attribute: Attribute<'a>,
    ) -> Result<(), Box<ConceptReadError>> {
        let unique_constraint_opt = owner.type_().get_owned_attribute_type_constraint_unique(
            snapshot,
            self.type_manager(),
            attribute.type_(),
        )?;
        if let Some(unique_constraint) = unique_constraint_opt {
            let attribute_key = attribute.vertex();
            let attribute_value = snapshot
                .get_last_existing::<BUFFER_VALUE_INLINE>(attribute_key.clone().into_storage_key().as_reference())
                .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))?
                .ok_or(ConceptReadError::CorruptMissingMandatoryAttributeValue)?;

            let lock_key = create_custom_lock_key(
                [
                    &Infix::PropertyAnnotationUnique.infix_id().bytes(),
                    &*unique_constraint.source().attribute().vertex().into_bytes(),
                    attribute_key.attribute_id().bytes(),
                    &attribute_value,
                    &*owner.vertex().into_bytes(),
                    &*unique_constraint.source().owner().vertex().into_bytes(),
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
        owner: &Object<'_>,
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
                    &*owner.vertex().clone().into_bytes(),
                    &Prefix::EdgeOwns.prefix_id().bytes(),
                    &*constraint.source().interface().vertex().clone().into_bytes(),
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
        player: &Object<'_>,
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
                    &*player.vertex().clone().into_bytes(),
                    &Prefix::EdgePlays.prefix_id().bytes(),
                    &*constraint.source().interface().vertex().clone().into_bytes(),
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
        relation: &Relation<'_>,
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
                    &*relation.vertex().clone().into_bytes(),
                    &Prefix::EdgeRelates.prefix_id().bytes(),
                    &*constraint.source().interface().vertex().clone().into_bytes(),
                ]
                .into_iter(),
            );
            snapshot.exclusive_lock_add(lock_key);
        }
        Ok(())
    }

    fn cleanup_relations(&self, snapshot: &mut impl WritableSnapshot) -> Result<(), Box<ConceptWriteError>> {
        let mut any_deleted = true;
        while any_deleted {
            any_deleted = false;
            for (key, _) in snapshot
                .iterate_writes_range(KeyRange::new_within(
                    RangeStart::Inclusive(ThingEdgeLinks::prefix()),
                    ThingEdgeLinks::FIXED_WIDTH_ENCODING,
                ))
                .filter(|(_, write)| matches!(write, Write::Delete))
            {
                let edge = ThingEdgeLinks::new(Bytes::Reference(key.byte_array().as_ref()));
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

        let mut any_deleted = true;
        while any_deleted {
            any_deleted = false;
            for key in snapshot
                .iterate_writes_range(KeyRange::new_within(
                    RangeStart::Inclusive(ObjectVertex::build_prefix_prefix(Prefix::VertexRelation)),
                    ObjectVertex::FIXED_WIDTH_ENCODING,
                ))
                .filter_map(|(key, write)| (!matches!(write, Write::Delete)).then_some(key))
            {
                let relation = Relation::new(ObjectVertex::new(Bytes::reference(key.bytes())));
                if !relation.has_players(snapshot, self) {
                    relation.delete(snapshot, self)?;
                    any_deleted = true;
                }
            }
        }

        let mut any_deleted = true;
        while any_deleted {
            any_deleted = false;
            for relation_type in snapshot
                .iterate_writes_range(KeyRange::new_within(
                    RangeStart::Inclusive(TypeVertexProperty::build_prefix()),
                    TypeVertexProperty::FIXED_WIDTH_ENCODING,
                ))
                .filter_map(|(key, write)| match write {
                    Write::Put { .. } | Write::Insert { .. } => {
                        let bytes = Bytes::reference(key.bytes());
                        if <AnnotationCascade as TypeVertexPropertyEncoding>::is_decodable_from(bytes.clone()) {
                            let decoded = TypeVertexProperty::new(bytes);
                            RelationType::from_vertex(decoded.type_vertex()).ok().map(|type_| type_.into_owned())
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
            {
                if !self.type_exists(snapshot, relation_type.clone())? {
                    continue;
                }
                let subtypes = relation_type.get_subtypes_transitive(snapshot, self.type_manager())?;
                once(&relation_type).chain(subtypes.into_iter()).try_for_each(|type_| {
                    let is_cascade = true; // TODO: Always consider cascade now, can be changed later.
                    if is_cascade {
                        let mut relations: InstanceIterator<Relation<'_>> =
                            self.get_instances_in(snapshot, type_.clone());
                        while let Some(relation) = relations.next().transpose()? {
                            if !relation.has_players(snapshot, self) {
                                relation.delete(snapshot, self)?;
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

    fn cleanup_attributes(&self, snapshot: &mut impl WritableSnapshot) -> Result<(), Box<ConceptWriteError>> {
        for (key, _write) in snapshot
            .iterate_writes_range(KeyRange::new_within(
                RangeStart::Inclusive(ThingEdgeHas::prefix()),
                ThingEdgeHas::FIXED_WIDTH_ENCODING,
            ))
            .filter(|(_, write)| matches!(write, Write::Delete))
        {
            let edge = ThingEdgeHas::new(Bytes::Reference(key.byte_array()));
            let attribute = Attribute::new(edge.to());
            let is_independent = attribute.type_().is_independent(snapshot, self.type_manager())?;
            if attribute.get_status(snapshot, self) == ConceptStatus::Deleted {
                continue;
            }
            if !is_independent && !attribute.has_owners(snapshot, self) {
                attribute.delete(snapshot, self)?;
            }
        }

        for (key, _value) in snapshot
            .iterate_writes_range(KeyRange::new_within(
                RangeStart::Inclusive(StorageKey::new(
                    AttributeVertex::KEYSPACE,
                    Bytes::inline(Prefix::VertexAttribute.prefix_id().bytes(), 1),
                )),
                Prefix::VertexAttribute.fixed_width_keys(),
            ))
            .filter_map(|(key, write)| match write {
                Write::Put { value, .. } => Some((key, value)),
                _ => None,
            })
        {
            let attribute = Attribute::new(AttributeVertex::new(Bytes::reference(key.bytes())));
            let is_independent = attribute.type_().is_independent(snapshot, self.type_manager())?;
            if !is_independent && !attribute.has_owners(snapshot, self) {
                self.unput_attribute(snapshot, attribute)?;
            }
        }

        for attribute_type in snapshot
            .iterate_writes_range(KeyRange::new_within(
                RangeStart::Inclusive(TypeVertexProperty::build_prefix()),
                TypeVertexProperty::FIXED_WIDTH_ENCODING,
            ))
            .filter_map(|(key, write)| match write {
                Write::Delete => {
                    let bytes = Bytes::reference(key.bytes());
                    if <AnnotationIndependent as TypeVertexPropertyEncoding>::is_decodable_from(bytes.clone()) {
                        let decoded = TypeVertexProperty::new(bytes);
                        AttributeType::from_vertex(decoded.type_vertex()).ok().map(|type_| type_.into_owned())
                    } else {
                        None
                    }
                }
                _ => None,
            })
        {
            if !self.type_exists(snapshot, attribute_type.clone())? {
                continue;
            }
            let subtypes = attribute_type.get_subtypes_transitive(snapshot, self.type_manager())?;
            once(&attribute_type).chain(subtypes.into_iter()).try_for_each(|type_| {
                let is_independent = type_.is_independent(snapshot, self.type_manager())?;
                if !is_independent {
                    let mut attributes: InstanceIterator<Attribute<'_>> =
                        self.get_instances_in(snapshot, type_.clone());
                    while let Some(attribute) = attributes.next().transpose()? {
                        if !attribute.has_owners(snapshot, self) {
                            attribute.delete(snapshot, self)?;
                        }
                    }
                }
                Ok::<(), Box<ConceptWriteError>>(())
            })?;
        }

        Ok(())
    }

    fn validate(&self, snapshot: &mut impl WritableSnapshot) -> Result<(), Vec<Box<ConceptWriteError>>> {
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
        collect_errors!(errors, res, |source| Box::new(DataValidationError::ConceptRead { source }));
        res = self.collect_modified_has(snapshot, &mut modified_objects_attribute_types);
        collect_errors!(errors, res, |source| Box::new(DataValidationError::ConceptRead { source }));
        res =
            self.collect_modified_links(snapshot, &mut modified_relations_role_types, &mut modified_objects_role_types);
        collect_errors!(errors, res, |source| Box::new(DataValidationError::ConceptRead { source }));

        for (object, modified_owns) in modified_objects_attribute_types {
            res = CommitTimeValidation::validate_object_has(snapshot, self, object, modified_owns, &mut errors);
            collect_errors!(errors, res, |source| Box::new(DataValidationError::ConceptRead { source }));
        }

        for (object, modified_plays) in modified_objects_role_types {
            res = CommitTimeValidation::validate_object_links(snapshot, self, object, modified_plays, &mut errors);
            collect_errors!(errors, res, |source| Box::new(DataValidationError::ConceptRead { source }));
        }

        for (relation, modified_relates) in modified_relations_role_types {
            res =
                CommitTimeValidation::validate_relation_links(snapshot, self, relation, modified_relates, &mut errors);
            collect_errors!(errors, res, |source| Box::new(DataValidationError::ConceptRead { source }));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors
                .into_iter()
                .map(|typedb_source| Box::new(ConceptWriteError::DataValidation { typedb_source }))
                .collect())
        }
    }

    fn collect_new_objects(
        &self,
        snapshot: &impl WritableSnapshot,
        out_object_attribute_types: &mut HashMap<Object<'static>, HashSet<AttributeType>>,
        out_object_role_types: &mut HashMap<Object<'static>, HashSet<RoleType>>,
        out_relation_role_types: &mut HashMap<Relation<'static>, HashSet<RoleType>>,
    ) -> Result<(), Box<ConceptReadError>> {
        for key in snapshot
            .iterate_writes_range(KeyRange::new_variable_width(
                RangeStart::Inclusive(StorageKey::new(
                    ObjectVertex::KEYSPACE,
                    Bytes::<0>::reference(ObjectVertex::build_prefix_prefix(Prefix::VertexEntity).bytes()),
                )),
                RangeEnd::EndPrefixInclusive(StorageKey::new(
                    ObjectVertex::KEYSPACE,
                    Bytes::<0>::reference(ObjectVertex::build_prefix_prefix(Prefix::VertexRelation).bytes()),
                )),
            ))
            .filter_map(|(key, write)| match write {
                Write::Insert { .. } => Some(key),
                Write::Delete => None,
                Write::Put { .. } => unreachable!("Encountered a Put for an entity"),
            })
        {
            let object = Object::new(ObjectVertex::new(Bytes::reference(key.bytes()))).into_owned();
            match &object {
                Object::Entity(_) => {}
                Object::Relation(relation) => {
                    let updated_role_types = out_relation_role_types.entry(relation.clone()).or_default();
                    for relates in relation.type_().get_relates(snapshot, self.type_manager())?.into_iter() {
                        updated_role_types.insert(relates.role());
                    }
                }
            }

            let updated_attribute_types = out_object_attribute_types.entry(object.clone()).or_default();
            for owns in object.type_().get_owns(snapshot, self.type_manager())?.into_iter() {
                updated_attribute_types.insert(owns.attribute());
            }

            let updated_role_types = out_object_role_types.entry(object.clone()).or_default();
            for plays in object.type_().get_plays(snapshot, self.type_manager())?.into_iter() {
                updated_role_types.insert(plays.role());
            }
        }
        Ok(())
    }

    fn collect_modified_has(
        &self,
        snapshot: &impl WritableSnapshot,
        out_object_attribute_types: &mut HashMap<Object<'static>, HashSet<AttributeType>>,
    ) -> Result<(), Box<ConceptReadError>> {
        for (key, _) in snapshot.iterate_writes_range(KeyRange::new_within(
            RangeStart::Inclusive(ThingEdgeHas::prefix()),
            ThingEdgeHas::FIXED_WIDTH_ENCODING,
        )) {
            let edge = ThingEdgeHas::new(Bytes::Reference(key.byte_array()));
            let owner = Object::new(edge.from());
            let attribute = Attribute::new(edge.to());
            if self.object_exists(snapshot, &owner)? {
                let updated_attribute_types = out_object_attribute_types.entry(owner.into_owned()).or_default();
                updated_attribute_types.insert(attribute.type_());
            }
        }

        Ok(())
    }

    fn collect_modified_links(
        &self,
        snapshot: &impl WritableSnapshot,
        out_relation_role_types: &mut HashMap<Relation<'static>, HashSet<RoleType>>,
        out_object_role_types: &mut HashMap<Object<'static>, HashSet<RoleType>>,
    ) -> Result<(), Box<ConceptReadError>> {
        for (key, _) in snapshot.iterate_writes_range(KeyRange::new_within(
            RangeStart::Inclusive(ThingEdgeLinks::prefix()),
            ThingEdgeLinks::FIXED_WIDTH_ENCODING,
        )) {
            let edge = ThingEdgeLinks::new(Bytes::reference(key.bytes()));
            let relation = Relation::new(edge.relation());
            let player = Object::new(edge.player());
            let role_type = RoleType::build_from_type_id(edge.role_id());

            if self.object_exists(snapshot, &relation)? {
                let updated_role_types = out_relation_role_types.entry(relation.into_owned()).or_default();
                updated_role_types.insert(role_type.clone());
            }

            if self.object_exists(snapshot, &player)? {
                let updated_role_types = out_object_role_types.entry(player.into_owned()).or_default();
                updated_role_types.insert(role_type.clone());
            }
        }

        Ok(())
    }

    pub fn create_entity<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        entity_type: EntityType,
    ) -> Result<Entity<'a>, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_entity_type_is_not_abstract(snapshot, self, entity_type.clone())
            .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        Ok(Entity::new(self.vertex_generator.create_entity(entity_type.vertex().type_id_(), snapshot)))
    }

    pub fn create_relation<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation_type: RelationType,
    ) -> Result<Relation<'a>, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_relation_type_is_not_abstract(snapshot, self, relation_type.clone())
            .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        Ok(Relation::new(self.vertex_generator.create_relation(relation_type.vertex().type_id_(), snapshot)))
    }

    pub fn create_attribute<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<Attribute<'a>, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_attribute_type_is_not_abstract(snapshot, self, attribute_type.clone())
            .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_value_type_matches_attribute_type_for_write(
            snapshot,
            self,
            attribute_type.clone(),
            value.value_type(),
            value.as_reference(),
        )?;

        OperationTimeValidation::validate_attribute_regex_constraints(
            snapshot,
            self,
            attribute_type.clone(),
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_attribute_range_constraints(
            snapshot,
            self,
            attribute_type.clone(),
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_attribute_values_constraints(
            snapshot,
            self,
            attribute_type.clone(),
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        self.put_attribute(snapshot, attribute_type, value)
    }

    pub(crate) fn put_attribute<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<Attribute<'a>, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_value_type_matches_attribute_type_for_write(
            snapshot,
            self,
            attribute_type.clone(),
            value.value_type(),
            value.as_reference(),
        )?;

        let vertex = match value
            .cast(&attribute_type.get_value_type_without_source(snapshot, self.type_manager())?.unwrap())
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
            Value::Long(long) => {
                let encoded_long = LongBytes::build(long);
                self.vertex_generator.create_attribute_long(attribute_type.vertex().type_id_(), encoded_long, snapshot)
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
                let encoded_string: StringBytes<'_, BUFFER_KEY_INLINE> = StringBytes::build_ref(&string);
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
        attribute_vertex: &AttributeVertex<'static>,
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

    pub(crate) fn delete_entity(&self, snapshot: &mut impl WritableSnapshot, entity: Entity<'_>) {
        let key = entity.into_vertex().into_storage_key().into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        snapshot.delete(key)
    }

    pub(crate) fn delete_relation(&self, snapshot: &mut impl WritableSnapshot, relation: Relation<'_>) {
        let key = relation.into_vertex().into_storage_key().into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        snapshot.delete(key)
    }

    pub(crate) fn delete_attribute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute: Attribute<'_>,
    ) -> Result<(), Box<ConceptWriteError>> {
        let key = attribute.into_vertex().into_storage_key().into_owned_array();
        snapshot.delete(key);
        Ok(())
    }

    pub(crate) fn uninsert_entity(&self, snapshot: &mut impl WritableSnapshot, entity: Entity<'_>) {
        let key = entity.into_vertex().into_storage_key().into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        snapshot.uninsert(key)
    }

    pub(crate) fn uninsert_relation(&self, snapshot: &mut impl WritableSnapshot, relation: Relation<'_>) {
        let key = relation.into_vertex().into_storage_key().into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        snapshot.uninsert(key)
    }

    pub(crate) fn unput_attribute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute: Attribute<'_>,
    ) -> Result<(), Box<ConceptWriteError>> {
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
        snapshot: &mut impl WritableSnapshot,
        owner: &impl ObjectAPI<'a>,
        attribute: Attribute<'_>,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.set_has_count(snapshot, owner, attribute, 1)
    }

    pub(crate) fn set_has_count<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: &impl ObjectAPI<'a>,
        attribute: Attribute<'_>,
        count: u64,
    ) -> Result<(), Box<ConceptWriteError>> {
        let attribute_type = attribute.type_();
        let value = attribute.get_value(snapshot, self)?.into_owned();

        OperationTimeValidation::validate_value_type_matches_attribute_type_for_write(
            snapshot,
            self,
            attribute_type.clone(),
            value.value_type(),
            value.as_reference(),
        )?;

        OperationTimeValidation::validate_has_unique_constraint(
            snapshot,
            self,
            owner,
            attribute.type_(),
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_has_regex_constraints(
            snapshot,
            self,
            owner,
            attribute_type.clone(),
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_has_range_constraints(
            snapshot,
            self,
            owner,
            attribute_type.clone(),
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        OperationTimeValidation::validate_has_values_constraints(
            snapshot,
            self,
            owner,
            attribute_type.clone(),
            value.as_reference(),
        )
        .map_err(|typedb_source| ConceptWriteError::DataValidation { typedb_source })?;

        let has = ThingEdgeHas::build(owner.vertex(), attribute.vertex());
        let has_reverse = ThingEdgeHasReverse::build(attribute.vertex(), owner.vertex());

        if count == 0 {
            snapshot.delete(has.into_storage_key().into_owned_array());
            snapshot.delete(has_reverse.into_storage_key().into_owned_array());
        } else {
            owner.set_required(snapshot, self)?;
            attribute.set_required(snapshot, self)?;

            snapshot.put_val(has.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));
            snapshot.put_val(has_reverse.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));
        }

        Ok(())
    }

    pub(crate) fn unset_has<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: &impl ObjectAPI<'a>,
        attribute: Attribute<'_>,
    ) {
        let owner_status = owner.get_status(snapshot, self);
        let has = ThingEdgeHas::build(owner.vertex(), attribute.vertex()).into_storage_key().into_owned_array();
        let has_reverse =
            ThingEdgeHasReverse::build(attribute.vertex(), owner.vertex()).into_storage_key().into_owned_array();
        match owner_status {
            ConceptStatus::Inserted => {
                let count = 1;
                snapshot.unput_val(has, ByteArray::copy(&encode_u64(count)));
                snapshot.unput_val(has_reverse, ByteArray::copy(&encode_u64(count)));
            }
            ConceptStatus::Persisted => {
                snapshot.delete(has);
                snapshot.delete(has_reverse);
            }
            ConceptStatus::Put => unreachable!("Encountered a `put` attribute owner: {owner:?}."),
            ConceptStatus::Deleted => unreachable!("Attempting to unset attribute ownership on a deleted owner."),
        }
    }

    pub(crate) fn set_has_ordered<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType,
        attributes: Vec<Attribute<'_>>,
    ) -> Result<(), Box<ConceptWriteError>> {
        let attribute_value_type = attribute_type
            .get_value_type_without_source(snapshot, self.type_manager())?
            .expect("Value type validation should be implemented in the callers of this method!");
        let key = build_object_vertex_property_has_order(owner.vertex(), attribute_type.into_vertex());
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

    pub(crate) fn unset_has_ordered<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType,
    ) {
        let order_property = build_object_vertex_property_has_order(owner.vertex(), attribute_type.into_vertex());
        snapshot.delete(order_property.into_storage_key().into_owned_array())
    }

    pub(crate) fn put_links_unordered<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let count: u64 = 1;
        // must be idempotent, so no lock required -- cannot fail

        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.clone().into_vertex());
        snapshot.put_val(links.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));

        let links_reverse = ThingEdgeLinks::build_links_reverse(
            player.clone().into_vertex(),
            relation.clone().into_vertex(),
            role_type.clone().into_vertex(),
        );
        snapshot.put_val(links_reverse.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));

        if self.type_manager.relation_index_available(snapshot, relation.type_())? {
            self.relation_index_player_regenerate(snapshot, relation, Object::new(player.vertex()), role_type, 1)?;
        }
        Ok(())
    }

    pub(crate) fn set_links_ordered(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation<'_>,
        role_type: RoleType,
        players: Vec<Object<'_>>,
    ) -> Result<(), Box<ConceptWriteError>> {
        let key =
            build_object_vertex_property_links_order(relation.as_reference().into_vertex(), role_type.into_vertex());
        let storage_key = key.into_storage_key().into_owned_array();
        let value = encode_role_players(players.iter().map(|player| player.vertex()));
        snapshot.put_val(storage_key.clone(), value);

        // must lock to fail concurrent transactions updating the same counters
        snapshot.exclusive_lock_add(storage_key.into_byte_array());

        Ok(())
    }

    pub(crate) fn set_links_count<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType,
        count: u64,
    ) -> Result<(), Box<ConceptWriteError>> {
        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.clone().into_vertex());
        let links_reverse =
            ThingEdgeLinks::build_links_reverse(player.vertex(), relation.vertex(), role_type.clone().into_vertex());

        if count == 0 {
            snapshot.delete(links.into_storage_key().into_owned_array());
            snapshot.delete(links_reverse.into_storage_key().into_owned_array());
        } else {
            relation.set_required(snapshot, self)?;
            player.set_required(snapshot, self)?;

            snapshot.put_val(links.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));
            snapshot.put_val(links_reverse.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));

            if self.type_manager.relation_index_available(snapshot, relation.type_())? {
                let player = Object::new(player.vertex());
                self.relation_index_player_regenerate(snapshot, relation, player, role_type, count)?
            }
        }

        Ok(())
    }

    /// Delete all counts of the specific role player in a given relation, and update indexes if required
    pub fn unset_links<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.clone().into_vertex())
            .into_storage_key()
            .into_owned_array();

        let links_reverse = ThingEdgeLinks::build_links_reverse(player.vertex(), relation.vertex(), role_type.vertex())
            .into_storage_key()
            .into_owned_array();

        let owner_status = relation.get_status(&*snapshot, self);

        match owner_status {
            ConceptStatus::Inserted => {
                let count = 1;
                snapshot.unput_val(links, ByteArray::copy(&encode_u64(count)));
                snapshot.unput_val(links_reverse, ByteArray::copy(&encode_u64(count)));
            }
            ConceptStatus::Persisted => {
                snapshot.delete(links);
                snapshot.delete(links_reverse);
            }
            ConceptStatus::Put => unreachable!("Encountered a `put` relation: {relation:?}."),
            ConceptStatus::Deleted => unreachable!("Attempting to unset attribute ownership on a deleted owner."),
        }

        if self
            .type_manager
            .relation_index_available(snapshot, relation.type_())
            .map_err(|error| ConceptWriteError::ConceptRead { source: error })?
        {
            self.relation_index_player_deleted(snapshot, relation, player, role_type)?;
        }
        Ok(())
    }

    /// Add a player to a relation that supports duplicates
    /// Caller must provide a lock that prevents race conditions on the player counts on the relation
    pub(crate) fn increment_links_count<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.clone().into_vertex());
        let count = snapshot
            .get_mapped(links.into_storage_key().as_reference(), |arr| decode_u64(arr.try_into().unwrap()))
            .map_err(|snapshot_err| Box::new(ConceptReadError::SnapshotGet { source: snapshot_err }))?;

        #[cfg(debug_assertions)]
        {
            let links_reverse = ThingEdgeLinks::build_links_reverse(
                player.vertex(),
                relation.vertex(),
                role_type.clone().into_vertex(),
            );
            let reverse_count = snapshot
                .get_mapped(links_reverse.into_storage_key().as_reference(), |arr| decode_u64(arr.try_into().unwrap()))
                .unwrap();
            debug_assert_eq!(&count, &reverse_count, "canonical and reverse links edge count mismatch!");
        }

        self.set_links_count(snapshot, relation, player, role_type, count.unwrap_or(0) + 1)
    }

    /// Remove a player from a relation that supports duplicates
    /// Caller must provide a lock that prevents race conditions on the player counts on the relation
    pub(crate) fn decrement_links_count<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType,
        decrement_count: u64,
    ) -> Result<(), Box<ConceptWriteError>> {
        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.clone().into_vertex());
        let count = snapshot
            .get_mapped(links.into_storage_key().as_reference(), |arr| decode_u64(arr.try_into().unwrap()))
            .map_err(|snapshot_err| Box::new(ConceptReadError::SnapshotGet { source: snapshot_err }))?;

        #[cfg(debug_assertions)]
        {
            let links_reverse = ThingEdgeLinks::build_links_reverse(
                player.vertex(),
                relation.vertex(),
                role_type.clone().into_vertex(),
            );
            let reverse_count = snapshot
                .get_mapped(links_reverse.into_storage_key().as_reference(), |arr| decode_u64(arr.try_into().unwrap()))
                .unwrap();
            debug_assert_eq!(&count, &reverse_count, "canonical and reverse links edge count mismatch!");
        }

        debug_assert!(*count.as_ref().unwrap() >= decrement_count);
        self.set_links_count(snapshot, relation, player, role_type, count.unwrap() - decrement_count)
    }

    ///
    /// TODO:
    /// Call index regenerations when cardinality changes in schema
    /// (create role type, set cardinality annotation, unset cardinality annotation, ...)
    ///

    /// Clean up all parts of a relation index to do with a specific role player
    /// after the player has been deleted.
    pub(crate) fn relation_index_player_deleted<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation<'_>,
        player: impl ObjectAPI<'a>,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let players = relation
            .get_players(snapshot, self)
            .map_static(|item| {
                let (roleplayer, _count) = item?;
                Ok((roleplayer.player().into_owned(), roleplayer.role_type()))
            })
            .try_collect::<Vec<_>, Box<ConceptReadError>>()?;
        for (rp_player, rp_role_type) in players {
            debug_assert!(!(rp_player == Object::new(player.vertex()) && role_type == rp_role_type));
            let index = ThingEdgeRolePlayerIndex::build(
                player.vertex(),
                rp_player.vertex(),
                relation.vertex(),
                role_type.vertex().type_id_(),
                rp_role_type.vertex().type_id_(),
            );
            snapshot.delete(index.into_storage_key().into_owned_array());
            let index_reverse = ThingEdgeRolePlayerIndex::build(
                rp_player.vertex(),
                player.vertex(),
                relation.vertex(),
                rp_role_type.vertex().type_id_(),
                role_type.vertex().type_id_(),
            );
            snapshot.delete(index_reverse.into_storage_key().into_owned_array());
        }
        Ok(())
    }

    /// For N duplicate role players, the self-edges are available N-1 times.
    /// For N duplicate player 1, and M duplicate player 2 - from N to M has M index repetitions,
    /// while M to N has N index repetitions.
    pub(crate) fn relation_index_player_regenerate(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation<'_>,
        player: Object<'_>,
        role_type: RoleType,
        count_for_player: u64,
    ) -> Result<(), Box<ConceptWriteError>> {
        debug_assert_ne!(count_for_player, 0);
        let players = relation
            .get_players(snapshot, self)
            .map_static(|item| {
                let (roleplayer, count) = item?;
                Ok((roleplayer.player().into_owned(), roleplayer.role_type(), count))
            })
            .try_collect::<Vec<_>, Box<ConceptReadError>>()?;
        for (rp_player, rp_role_type, rp_count) in players {
            let is_same_rp = rp_player == player && rp_role_type == role_type;
            if is_same_rp {
                let repetitions = count_for_player - 1;
                if repetitions > 0 {
                    let index = ThingEdgeRolePlayerIndex::build(
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
                let index = ThingEdgeRolePlayerIndex::build(
                    player.vertex(),
                    rp_player.vertex(),
                    relation.vertex(),
                    role_type.vertex().type_id_(),
                    rp_role_type.vertex().type_id_(),
                );
                snapshot
                    .put_val(index.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(rp_repetitions)));
                let player_repetitions = count_for_player;
                let index_reverse = ThingEdgeRolePlayerIndex::build(
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
}
