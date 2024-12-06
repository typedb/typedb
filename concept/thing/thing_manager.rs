/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{Bound, HashMap, HashSet},
    io::Read,
    iter::once,
    ops::RangeBounds,
    sync::Arc,
};

use bytes::{byte_array::ByteArray, util::increment, Bytes};
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeHasReverse, ThingEdgeLinks, ThingEdgeLinksIndex},
            property::{build_object_vertex_property_has_order, build_object_vertex_property_links_order},
            vertex_attribute::{AttributeID, AttributeVertex},
            vertex_generator::ThingVertexGenerator,
            vertex_object::ObjectVertex,
            ThingVertex,
        },
        type_::{
            property::{TypeVertexProperty, TypeVertexPropertyEncoding},
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
        long_bytes::LongBytes,
        primitive_encoding::{decode_u64, encode_u64},
        string_bytes::StringBytes,
        struct_bytes::StructBytes,
        value::Value,
        value_struct::{StructIndexEntry, StructIndexEntryKey, StructValue},
        value_type::{ValueType, ValueTypeCategory},
        ValueEncodable,
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use primitive::either::Either;
use resource::constants::{
    encoding::StructFieldIDUInt,
    snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
};
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::{StorageKey, StorageKeyArray},
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
    ) -> InstanceIterator<T> {
        if thing_type.is_abstract(snapshot, self.type_manager()).unwrap() {
            return InstanceIterator::empty();
        }

        let prefix = <T as ThingAPI>::prefix_for_type(thing_type);
        let storage_key_prefix =
            <T as ThingAPI>::Vertex::build_prefix_type(prefix, thing_type.vertex().type_id_(), keyspace);
        let snapshot_iterator =
            snapshot.iterate_range(&KeyRange::new_within(storage_key_prefix, prefix.fixed_width_keys()));
        InstanceIterator::new(snapshot_iterator)
    }

    fn get_instances<T: ThingAPI>(
        &self,
        keyspace: EncodingKeyspace,
        snapshot: &impl ReadableSnapshot,
    ) -> InstanceIterator<T> {
        let (prefix_start, prefix_end_exclusive) = T::PREFIX_RANGE_INCLUSIVE;
        let key_start = T::Vertex::build_prefix_prefix(prefix_start, keyspace);
        let key_end = T::Vertex::build_prefix_prefix(prefix_end_exclusive, keyspace);
        let snapshot_iterator = snapshot.iterate_range(&KeyRange::new_variable_width(
            RangeStart::Inclusive(key_start),
            RangeEnd::EndPrefixInclusive(key_end),
        ));
        InstanceIterator::new(snapshot_iterator)
    }

    pub fn get_entities(&self, snapshot: &impl ReadableSnapshot) -> InstanceIterator<Entity> {
        self.get_instances::<Entity>(<Entity as ThingAPI>::Vertex::KEYSPACE, snapshot)
    }

    pub fn get_relations(&self, snapshot: &impl ReadableSnapshot) -> InstanceIterator<Relation> {
        self.get_instances::<Relation>(<Relation as ThingAPI>::Vertex::KEYSPACE, snapshot)
    }

    pub fn get_entities_in(&self, snapshot: &impl ReadableSnapshot, type_: EntityType) -> InstanceIterator<Entity> {
        self.get_instances_in(snapshot, type_, <Entity as ThingAPI>::Vertex::KEYSPACE)
    }

    pub fn get_relations_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_: RelationType,
    ) -> InstanceIterator<Relation> {
        self.get_instances_in(snapshot, type_, <Relation as ThingAPI>::Vertex::KEYSPACE)
    }

    pub fn get_objects_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        object_type: ObjectType,
    ) -> InstanceIterator<Object> {
        self.get_instances_in(snapshot, object_type, <Object as ThingAPI>::Vertex::KEYSPACE)
    }

    pub fn instance_exists(
        &self,
        snapshot: &impl ReadableSnapshot,
        instance: &impl ThingAPI,
    ) -> Result<bool, Box<ConceptReadError>> {
        let storage_key = instance.vertex().into_storage_key();
        snapshot
            .get::<BUFFER_KEY_INLINE>(storage_key.as_reference())
            .map(|value| value.is_some())
            .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))
    }

    pub(crate) fn get_relations_roles(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: impl ObjectAPI,
    ) -> RelationRoleIterator {
        let prefix = ThingEdgeLinks::prefix_reverse_from_player(player.vertex());
        RelationRoleIterator::new(
            snapshot.iterate_range(&KeyRange::new_within(prefix, ThingEdgeLinks::FIXED_WIDTH_ENCODING_REVERSE)),
        )
    }

    pub(crate) fn get_relations_player(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: impl ObjectAPI,
    ) -> impl Iterator<Item = Result<Relation, Box<ConceptReadError>>> {
        self.get_relations_roles(snapshot, player).map::<Result<Relation, _>, _>(|res| {
            let (rel, _, _) = res?;
            Ok(rel)
        })
    }

    pub(crate) fn get_relations_player_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: impl ObjectAPI,
        role_type: RoleType,
    ) -> impl Iterator<Item = Result<(Relation, u64), Box<ConceptReadError>>> {
        self.get_relations_roles(snapshot, player).filter_map::<Result<(Relation, u64), _>, _>(move |item| match item {
            Ok((rel, role, count)) => (role == role_type).then_some(Ok((rel, count))),
            Err(error) => Some(Err(error)),
        })
    }

    pub fn get_attributes<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
    ) -> Result<impl Iterator<Item = Result<Attribute, Box<ConceptReadError>>>, Box<ConceptReadError>> {
        Ok(self.get_attributes_short(snapshot)?.chain(self.get_attributes_long(snapshot)?))
    }

    pub fn get_attributes_short<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
    ) -> Result<AttributeIterator<InstanceIterator<Attribute>>, Box<ConceptReadError>> {
        let has_reverse_start = ThingEdgeHasReverse::prefix_from_prefix_short(Prefix::VertexAttribute);
        let range = KeyRange::new_within(has_reverse_start, Prefix::VertexAttribute.fixed_width_keys());
        let has_reverse_iterator_buffer = snapshot.iterate_writes_range(&range);
        let has_reverse_iterator_storage = snapshot.iterate_storage_range(&range);
        Ok(AttributeIterator::new(
            self.get_instances::<Attribute>(AttributeVertex::keyspace_for_is_short(true), snapshot),
            has_reverse_iterator_buffer,
            has_reverse_iterator_storage,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    pub fn get_attributes_long<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
    ) -> Result<AttributeIterator<InstanceIterator<Attribute>>, Box<ConceptReadError>> {
        let has_reverse_start = ThingEdgeHasReverse::prefix_from_prefix_short(Prefix::VertexAttribute);
        let range = KeyRange::new_within(has_reverse_start, Prefix::VertexAttribute.fixed_width_keys());
        let has_reverse_iterator_buffer = snapshot.iterate_writes_range(&range);
        let has_reverse_iterator_storage = snapshot.iterate_storage_range(&range);
        Ok(AttributeIterator::new(
            self.get_instances::<Attribute>(AttributeVertex::keyspace_for_is_short(false), snapshot),
            has_reverse_iterator_buffer,
            has_reverse_iterator_storage,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    pub fn get_attributes_in(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
    ) -> Result<AttributeIterator<InstanceIterator<Attribute>>, Box<ConceptReadError>> {
        let attribute_value_type =
            attribute_type.get_value_type_without_source(snapshot, self.type_manager.as_ref())?;
        let Some(value_type) = attribute_value_type.as_ref() else {
            return Ok(AttributeIterator::new_empty());
        };

        let has_reverse_prefix =
            ThingEdgeHasReverse::prefix_from_attribute_type(value_type.category(), attribute_type.vertex().type_id_());
        let range = KeyRange::new_within(has_reverse_prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        let has_reverse_iterator_buffer = snapshot.iterate_writes_range(&range);
        let has_reverse_iterator_storage = snapshot.iterate_storage_range(&range);

        Ok(AttributeIterator::new(
            self.get_instances_in(
                snapshot,
                attribute_type,
                AttributeVertex::keyspace_for_category(value_type.category()),
            ),
            has_reverse_iterator_buffer,
            has_reverse_iterator_storage,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    pub(crate) fn get_attribute_value(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: &Attribute,
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
    ) -> Result<Option<Attribute>, Box<ConceptReadError>> {
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
        range: &'a impl RangeBounds<Value<'a>>,
    ) -> Result<AttributeIterator<InstanceIterator<Attribute>>, Box<ConceptReadError>> {
        if matches!(range.start_bound(), Bound::Unbounded) && matches!(range.end_bound(), Bound::Unbounded) {
            return self.get_attributes_in(snapshot, attribute_type);
        }
        let Some(attribute_value_type) = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?
        else {
            return Ok(AttributeIterator::new_empty());
        };

        let Some((value_lower_bound, value_upper_bound)) = Self::get_value_range(&attribute_value_type, range)? else {
            return Ok(AttributeIterator::new_empty());
        };

        let start_attribute_vertex_prefix_range = match value_lower_bound {
            Bound::Included(lower_value) => {
                let vertex_or_prefix = AttributeVertex::build_or_prefix_for_value(
                    attribute_type.vertex().type_id_(),
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
                    attribute_type.vertex().type_id_(),
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
                    attribute_type.vertex().type_id_(),
                    AttributeVertex::keyspace_for_category(attribute_value_type.category()),
                )
                .resize_to(),
            ),
        };

        let end_attribute_vertex_prefix_range = match value_upper_bound {
            Bound::Included(upper_value) => {
                let vertex_or_prefix = AttributeVertex::build_or_prefix_for_value(
                    attribute_type.vertex().type_id_(),
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
                    attribute_type.vertex().type_id_(),
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
                    attribute_type.vertex().type_id_(),
                    AttributeVertex::keyspace_for_category(attribute_value_type.category()),
                );
                let keyspace = prefix.keyspace_id();
                let mut array = prefix.into_bytes().into_array();
                array.increment().unwrap();
                let prefix_key = StorageKey::Array(StorageKeyArray::new_raw(keyspace, array));
                RangeEnd::EndPrefixExclusive(prefix_key.resize_to())
            }
        };

        let has_reverse_start_prefix = ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(
            attribute_value_type.category(),
            start_attribute_vertex_prefix_range.get_value().as_reference().bytes(),
        );
        let has_reverse_end_prefix = end_attribute_vertex_prefix_range.map(|end| {
            ThingEdgeHasReverse::prefix_from_attribute_vertex_prefix(
                attribute_value_type.category(),
                end.as_reference().bytes(),
            )
        });
        let has_reverse_range =
            KeyRange::new_variable_width(RangeStart::Inclusive(has_reverse_start_prefix), has_reverse_end_prefix);
        let has_reverse_iterator_buffer = snapshot.iterate_writes_range(&has_reverse_range);
        let has_reverse_iterator_storage = snapshot.iterate_storage_range(&has_reverse_range);

        let range =
            KeyRange::new_variable_width(start_attribute_vertex_prefix_range, end_attribute_vertex_prefix_range);
        let snapshot_iterator = snapshot.iterate_range(&range);
        let attributes_iterator = InstanceIterator::new(snapshot_iterator);
        Ok(AttributeIterator::new(
            attributes_iterator,
            has_reverse_iterator_buffer,
            has_reverse_iterator_storage,
            self.type_manager().get_independent_attribute_types(snapshot)?,
        ))
    }

    fn get_value_range<'a>(
        expected_value_type: &ValueType,
        range: &'a impl RangeBounds<Value<'a>>,
    ) -> Result<Option<(Bound<Value<'a>>, Bound<Value<'a>>)>, Box<ConceptReadError>> {
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
        if !range_value_type.is_approximately_castable_to(expected_value_type) {
            return Ok(None);
        }
        let value_type = expected_value_type;

        let start_value_lower_bound = range.start_bound().map(|value| {
            if *value_type != range_value_type {
                value.as_reference().approximate_cast_lower_bound(value_type).unwrap()
            } else {
                value.as_reference()
            }
        });
        let end_value_upper_bound = range.end_bound().map(|value| {
            if *value_type != range_value_type {
                value.as_reference().approximate_cast_upper_bound(value_type).unwrap()
            } else {
                value.as_reference()
            }
        });
        Ok(Some((start_value_lower_bound, end_value_upper_bound)))
    }

    fn get_attribute_with_value_inline(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<Option<Attribute>, Box<ConceptReadError>> {
        debug_assert!(AttributeID::is_inlineable(value.as_reference()));
        let attribute_value_type = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?;
        if attribute_value_type.is_none() || attribute_value_type.as_ref().unwrap() != &value.value_type() {
            return Ok(None);
        }
        let vertex = AttributeVertex::new(attribute_type.vertex().type_id_(), AttributeID::build_inline(value));
        snapshot
            .get_mapped(vertex.into_storage_key().as_reference(), |_| Attribute::new(vertex))
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))
    }

    pub(crate) fn has_attribute_with_value(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        value: Value<'_>,
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
            let attribute = self.get_attribute_with_value(snapshot, attribute_type, value)?;
            match attribute {
                Some(attribute) => attribute.vertex(),
                None => return Ok(false),
            }
        };

        let has = ThingEdgeHas::new(owner.vertex(), vertex);
        let has_exists = snapshot
            .get_mapped(has.into_storage_key().as_reference(), |_value| true)
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or(false);
        Ok(has_exists)
    }

    pub(crate) fn has_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: impl ObjectAPI,
        attribute: &Attribute,
    ) -> Result<bool, Box<ConceptReadError>> {
        let has = ThingEdgeHas::new(owner.vertex(), attribute.vertex());
        let has_exists = snapshot
            .get_mapped(has.into_storage_key().as_reference(), |_value| true)
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or(false);
        Ok(has_exists)
    }

    pub fn get_has_from_owner_type_range_unordered<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner_type_range: &impl RangeBounds<ObjectType>,
    ) -> HasIterator {
        let range_start = match owner_type_range.start_bound() {
            Bound::Included(start_type) => RangeStart::Inclusive(ThingEdgeHas::prefix_from_type(start_type.vertex())),
            Bound::Excluded(start_type) => {
                RangeStart::ExcludePrefix(ThingEdgeHas::prefix_from_type(start_type.vertex()))
            }
            Bound::Unbounded => RangeStart::Inclusive(ThingEdgeHas::prefix_from_type_parts(
                Prefix::min_object_type_prefix(),
                TypeID::MIN,
            )),
        };
        let range_end = match owner_type_range.end_bound() {
            Bound::Included(end_type) => {
                RangeEnd::EndPrefixInclusive(ThingEdgeHas::prefix_from_type(end_type.vertex()))
            }
            Bound::Excluded(end_type) => {
                RangeEnd::EndPrefixExclusive(ThingEdgeHas::prefix_from_type(end_type.vertex()))
            }
            Bound::Unbounded => RangeEnd::EndPrefixInclusive(ThingEdgeHas::prefix_from_type_parts(
                Prefix::max_object_type_prefix(),
                TypeID::MAX,
            )),
        };
        let key_range = KeyRange::new(range_start, range_end, ThingEdgeHas::FIXED_WIDTH_ENCODING);
        HasIterator::new(snapshot.iterate_range(&key_range))
    }

    pub fn get_has_reverse(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
    ) -> Result<HasReverseIterator, Box<ConceptReadError>> {
        let Some(value_type) = attribute_type.get_value_type_without_source(snapshot, self.type_manager())? else {
            return Ok(HasReverseIterator::new_empty());
        };
        let prefix =
            ThingEdgeHasReverse::prefix_from_attribute_type(value_type.category(), attribute_type.vertex().type_id_());
        let range = KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        Ok(HasReverseIterator::new(snapshot.iterate_range(&range)))
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
    ) -> Result<HasReverseIterator, Box<ConceptReadError>> {
        if matches!(range.start_bound(), Bound::Unbounded) && matches!(range.end_bound(), Bound::Unbounded) {
            return self.get_has_reverse(snapshot, attribute_type);
        }
        let Some(attribute_value_type) = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?
        else {
            return Ok(HasReverseIterator::new_empty());
        };

        let Some((value_lower_bound, value_upper_bound)) = Self::get_value_range(&attribute_value_type, range)? else {
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
        Ok(HasReverseIterator::new(snapshot.iterate_range(&key_range)))
    }

    pub fn get_attributes_by_struct_field<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        attribute_type: AttributeType,
        path_to_field: Vec<StructFieldIDUInt>,
        value: Value<'_>,
    ) -> Result<impl Iterator<Item = Result<Attribute, Box<ConceptReadError>>>, Box<ConceptReadError>> {
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
            .iterate_range(&KeyRange::new_within(prefix, Prefix::IndexValueToStruct.fixed_width_keys()))
            .map_static::<Result<Attribute, _>, _>(|result| {
                result
                    .map(|(key, _)| {
                        Attribute::new(
                            StructIndexEntry::new(StructIndexEntryKey::new(key.into_bytes()), None).attribute_vertex(),
                        )
                    })
                    .map_err(|err| Box::new(ConceptReadError::SnapshotIterate { source: err }))
            })
            .into_iter();

        let has_reverse_prefix = ThingEdgeHasReverse::prefix_from_attribute_type(
            ValueTypeCategory::Struct,
            attribute_type.vertex().type_id_(),
        );
        let range = KeyRange::new_within(has_reverse_prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        let has_reverse_iterator_buffer = snapshot.iterate_writes_range(&range);
        let has_reverse_iterator_storage = snapshot.iterate_storage_range(&range);

        let iter = AttributeIterator::new(
            index_attribute_iterator,
            has_reverse_iterator_buffer,
            has_reverse_iterator_storage,
            self.type_manager.get_independent_attribute_types(snapshot)?,
        );
        Ok(iter)
    }

    pub(crate) fn get_has_from_thing_unordered<'this, 'snapshot>(
        &'this self,
        snapshot: &'snapshot impl ReadableSnapshot,
        owner: &'this impl ObjectAPI,
        attribute_type_range_hint: &'this impl RangeBounds<AttributeType>,
    ) -> HasIterator {
        let range_start = match attribute_type_range_hint.start_bound() {
            Bound::Included(attribute_type) => RangeStart::Inclusive(ThingEdgeHas::prefix_from_object_to_type(
                owner.vertex(),
                attribute_type.vertex().type_id_(),
            )),
            Bound::Excluded(attribute_type) => RangeStart::ExcludePrefix(ThingEdgeHas::prefix_from_object_to_type(
                owner.vertex(),
                attribute_type.vertex().type_id_(),
            )),
            Bound::Unbounded => {
                RangeStart::Inclusive(ThingEdgeHas::prefix_from_object_to_type(owner.vertex(), TypeID::MIN))
            }
        };
        let range_end =
            match attribute_type_range_hint.end_bound() {
                Bound::Included(attribute_type) => RangeEnd::EndPrefixInclusive(
                    ThingEdgeHas::prefix_from_object_to_type(owner.vertex(), attribute_type.vertex().type_id_()),
                ),
                Bound::Excluded(attribute_type) => RangeEnd::EndPrefixExclusive(
                    ThingEdgeHas::prefix_from_object_to_type(owner.vertex(), attribute_type.vertex().type_id_()),
                ),
                Bound::Unbounded => {
                    RangeEnd::EndPrefixInclusive(ThingEdgeHas::prefix_from_object_to_type(owner.vertex(), TypeID::MAX))
                }
            };
        let key_range = KeyRange::new(range_start, range_end, ThingEdgeHas::FIXED_WIDTH_ENCODING);
        HasIterator::new(snapshot.iterate_range(&key_range))
    }

    pub(crate) fn get_has_from_thing_to_type_unordered(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
    ) -> HasAttributeIterator {
        let prefix = ThingEdgeHas::prefix_from_object_to_type(owner.vertex(), attribute_type.vertex().type_id_());
        HasAttributeIterator::new(
            snapshot.iterate_range(&KeyRange::new_within(prefix, ThingEdgeHas::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn get_has_from_thing_to_type_ordered(
        &self,
        snapshot: &impl ReadableSnapshot,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
    ) -> Result<Vec<Attribute>, Box<ConceptReadError>> {
        let key = build_object_vertex_property_has_order(owner.vertex(), attribute_type.vertex());
        let attribute_value_type = attribute_type.get_value_type_without_source(snapshot, self.type_manager())?;
        let value_type = match attribute_value_type.as_ref() {
            None => return Ok(Vec::new()),
            Some(value_type) => value_type,
        };
        let attributes = snapshot
            .get_mapped(key.into_storage_key().as_reference(), |bytes| {
                decode_attribute_ids(value_type.category(), bytes)
                    .map(|id| Attribute::new(AttributeVertex::new(attribute_type.vertex().type_id_(), id)))
                    .collect()
            })
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or_else(Vec::new);
        Ok(attributes)
    }

    pub(crate) fn get_owners(&self, snapshot: &impl ReadableSnapshot, attribute: &Attribute) -> AttributeOwnerIterator {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute.vertex());
        AttributeOwnerIterator::new(
            snapshot.iterate_range(&KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn get_owners_by_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: &Attribute,
        owner_type: impl ObjectTypeAPI,
    ) -> AttributeOwnerIterator {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute_to_type(attribute.vertex(), owner_type.vertex());
        AttributeOwnerIterator::new(
            snapshot.iterate_range(&KeyRange::new_within(prefix, ThingEdgeHasReverse::FIXED_WIDTH_ENCODING)),
        )
    }

    pub fn get_has_reverse_by_attribute_and_owner_type_range<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute: &Attribute,
        owner_type_range: &'a impl RangeBounds<ObjectType>,
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
        HasReverseIterator::new(snapshot.iterate_range(&key_range))
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
    ) -> LinksIterator {
        let range_start = match relation_type_range.start_bound() {
            Bound::Included(start_type) => {
                RangeStart::Inclusive(ThingEdgeLinks::prefix_from_relation_type(start_type.vertex().type_id_()))
            }
            Bound::Excluded(start_type) => {
                RangeStart::ExcludePrefix(ThingEdgeLinks::prefix_from_relation_type(start_type.vertex().type_id_()))
            }
            Bound::Unbounded => RangeStart::Inclusive(ThingEdgeLinks::prefix_from_relation_type(TypeID::MIN)),
        };
        let range_end = match relation_type_range.end_bound() {
            Bound::Included(end_type) => {
                RangeEnd::EndPrefixInclusive(ThingEdgeLinks::prefix_from_relation_type(end_type.vertex().type_id_()))
            }
            Bound::Excluded(end_type) => {
                RangeEnd::EndPrefixExclusive(ThingEdgeLinks::prefix_from_relation_type(end_type.vertex().type_id_()))
            }
            Bound::Unbounded => RangeEnd::EndPrefixInclusive(ThingEdgeLinks::prefix_from_relation_type(TypeID::MAX)),
        };
        LinksIterator::new(snapshot.iterate_range(&KeyRange::new(
            range_start,
            range_end,
            ThingEdgeLinks::FIXED_WIDTH_ENCODING,
        )))
    }

    pub fn get_links_by_relation_and_player_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        player_type_range: &impl RangeBounds<ObjectType>,
    ) -> LinksIterator {
        let range_start = match player_type_range.start_bound() {
            Bound::Included(start_type) => RangeStart::Inclusive(ThingEdgeLinks::prefix_from_relation_player_type(
                relation.vertex(),
                start_type.vertex(),
            )),
            Bound::Excluded(start_type) => RangeStart::ExcludePrefix(ThingEdgeLinks::prefix_from_relation_player_type(
                relation.vertex(),
                start_type.vertex(),
            )),
            Bound::Unbounded => RangeStart::Inclusive(ThingEdgeLinks::prefix_from_relation_player_type_parts(
                relation.vertex(),
                Prefix::min_object_type_prefix(),
                TypeID::MIN,
            )),
        };
        let range_end = match player_type_range.end_bound() {
            Bound::Included(end_type) => RangeEnd::EndPrefixInclusive(
                ThingEdgeLinks::prefix_from_relation_player_type(relation.vertex(), end_type.vertex()),
            ),
            Bound::Excluded(end_type) => RangeEnd::EndPrefixExclusive(
                ThingEdgeLinks::prefix_from_relation_player_type(relation.vertex(), end_type.vertex()),
            ),
            Bound::Unbounded => RangeEnd::EndPrefixInclusive(ThingEdgeLinks::prefix_from_relation_player_type_parts(
                relation.vertex(),
                Prefix::max_object_type_prefix(),
                TypeID::MAX,
            )),
        };
        let key_range = KeyRange::new(range_start, range_end, ThingEdgeLinks::FIXED_WIDTH_ENCODING);
        LinksIterator::new(snapshot.iterate_range(&key_range))
    }

    pub fn get_links_by_relation_and_player(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
    ) -> LinksIterator {
        let prefix = ThingEdgeLinks::prefix_from_relation_player(relation.vertex(), player.vertex());
        LinksIterator::new(snapshot.iterate_range(&KeyRange::new_within(prefix, ThingEdgeLinks::FIXED_WIDTH_ENCODING)))
    }

    pub fn get_links_reverse_by_player_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        player_type_range: &impl RangeBounds<ObjectType>,
    ) -> LinksIterator {
        let range_start = match player_type_range.start_bound() {
            Bound::Included(start_type) => RangeStart::Inclusive(ThingEdgeLinks::prefix_reverse_from_player_type(
                start_type.vertex().prefix(),
                start_type.vertex().type_id_(),
            )),
            Bound::Excluded(start_type) => RangeStart::ExcludePrefix(ThingEdgeLinks::prefix_reverse_from_player_type(
                start_type.vertex().prefix(),
                start_type.vertex().type_id_(),
            )),
            Bound::Unbounded => RangeStart::Inclusive(ThingEdgeLinks::prefix_reverse_from_player_type(
                Prefix::min_object_type_prefix(),
                TypeID::MIN,
            )),
        };
        let range_end = match player_type_range.end_bound() {
            Bound::Included(end_type) => RangeEnd::EndPrefixInclusive(ThingEdgeLinks::prefix_reverse_from_player_type(
                end_type.vertex().prefix(),
                end_type.vertex().type_id_(),
            )),
            Bound::Excluded(end_type) => RangeEnd::EndPrefixExclusive(ThingEdgeLinks::prefix_reverse_from_player_type(
                end_type.vertex().prefix(),
                end_type.vertex().type_id_(),
            )),
            Bound::Unbounded => RangeEnd::EndPrefixInclusive(ThingEdgeLinks::prefix_reverse_from_player_type(
                Prefix::max_object_type_prefix(),
                TypeID::MAX,
            )),
        };
        LinksIterator::new(snapshot.iterate_range(&KeyRange::new(
            range_start,
            range_end,
            ThingEdgeLinks::FIXED_WIDTH_ENCODING,
        )))
    }

    pub fn get_links_reverse_by_player_and_relation_type_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        player: impl ObjectAPI,
        relation_type_range: &impl RangeBounds<RelationType>,
    ) -> LinksIterator {
        let range_start = match relation_type_range.start_bound() {
            Bound::Included(type_start) => {
                RangeStart::Inclusive(ThingEdgeLinks::prefix_reverse_from_player_relation_type(
                    player.vertex(),
                    type_start.vertex().type_id_(),
                ))
            }
            Bound::Excluded(type_start) => {
                RangeStart::ExcludePrefix(ThingEdgeLinks::prefix_reverse_from_player_relation_type(
                    player.vertex(),
                    type_start.vertex().type_id_(),
                ))
            }
            Bound::Unbounded => RangeStart::Inclusive(ThingEdgeLinks::prefix_reverse_from_player_relation_type(
                player.vertex(),
                TypeID::MIN,
            )),
        };
        let range_end = match relation_type_range.end_bound() {
            Bound::Included(type_end) => RangeEnd::EndPrefixInclusive(
                ThingEdgeLinks::prefix_reverse_from_player_relation_type(player.vertex(), type_end.vertex().type_id_()),
            ),
            Bound::Excluded(type_end) => RangeEnd::EndPrefixExclusive(
                ThingEdgeLinks::prefix_reverse_from_player_relation_type(player.vertex(), type_end.vertex().type_id_()),
            ),
            Bound::Unbounded => RangeEnd::EndPrefixInclusive(ThingEdgeLinks::prefix_reverse_from_player_relation_type(
                player.vertex(),
                TypeID::MAX,
            )),
        };
        LinksIterator::new(snapshot.iterate_range(&KeyRange::new(
            range_start,
            range_end,
            ThingEdgeLinks::FIXED_WIDTH_ENCODING,
        )))
    }

    pub(crate) fn has_role_player(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.vertex());
        let links_exists = snapshot
            .get_mapped(links.into_storage_key().as_reference(), |_| true)
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or(false);
        Ok(links_exists)
    }

    pub(crate) fn get_role_players(&self, snapshot: &impl ReadableSnapshot, relation: Relation) -> RolePlayerIterator {
        let prefix = ThingEdgeLinks::prefix_from_relation(relation.vertex());
        RolePlayerIterator::new(
            snapshot.iterate_range(&KeyRange::new_within(prefix, ThingEdgeLinks::FIXED_WIDTH_ENCODING)),
        )
    }

    pub(crate) fn get_role_players_ordered(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        role_type: RoleType,
    ) -> Result<Vec<Object>, Box<ConceptReadError>> {
        let key = build_object_vertex_property_links_order(relation.vertex(), role_type.into_vertex());
        let players = snapshot
            .get_mapped(key.into_storage_key().as_reference(), |bytes| {
                decode_role_players(bytes).map(Object::new).collect()
            })
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?
            .unwrap_or_else(Vec::new);
        Ok(players)
    }

    pub(crate) fn get_role_players_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: Relation,
        role_type: RoleType,
    ) -> impl Iterator<Item = Result<(RolePlayer, u64), Box<ConceptReadError>>> {
        self.get_role_players(snapshot, relation).filter_map::<Result<(RolePlayer, u64), _>, _>(
            move |item| match item {
                Ok((role_player, count)) => (role_player.role_type() == role_type).then_some(Ok((role_player, count))),
                Err(error) => Some(Err(error)),
            },
        )
    }

    pub(crate) fn get_indexed_players(&self, snapshot: &impl ReadableSnapshot, from: Object) -> IndexedPlayersIterator {
        let prefix = ThingEdgeLinksIndex::prefix_from(from.vertex());
        IndexedPlayersIterator::new(
            snapshot.iterate_range(&KeyRange::new_within(prefix, ThingEdgeLinksIndex::FIXED_WIDTH_ENCODING)),
        )
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

    pub(crate) fn object_exists(
        &self,
        snapshot: &impl ReadableSnapshot,
        object: impl ObjectAPI,
    ) -> Result<bool, Box<ConceptReadError>> {
        snapshot
            .contains(object.vertex().into_storage_key().as_reference())
            .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))
    }

    pub(crate) fn type_exists(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeAPI,
    ) -> Result<bool, Box<ConceptReadError>> {
        snapshot
            .contains(type_.vertex().into_storage_key().as_reference())
            .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))
    }
}

impl ThingManager {
    pub(crate) fn lock_existing_object(&self, snapshot: &mut impl WritableSnapshot, object: impl ObjectAPI) {
        snapshot.unmodifiable_lock_add(object.vertex().into_storage_key().into_owned_array())
    }

    pub(crate) fn lock_existing_attribute(&self, snapshot: &mut impl WritableSnapshot, attribute: &Attribute) {
        snapshot.unmodifiable_lock_add(attribute.vertex().into_storage_key().into_owned_array())
    }

    pub fn finalise(&self, snapshot: &mut impl WritableSnapshot) -> Result<(), Vec<ConceptWriteError>> {
        self.validate(snapshot)?;

        self.cleanup_relations(snapshot).map_err(|err| vec![*err])?;
        self.cleanup_attributes(snapshot).map_err(|err| vec![*err])?;

        match self.create_commit_locks(snapshot) {
            Ok(_) => Ok(()),
            Err(error) => Err(vec![ConceptWriteError::ConceptRead { source: error }]),
        }
    }

    fn create_commit_locks(&self, snapshot: &mut impl WritableSnapshot) -> Result<(), Box<ConceptReadError>> {
        // TODO: Should not collect here (iterate_writes() already copies)
        for (key, _write) in snapshot.iterate_writes().collect_vec() {
            if ThingEdgeHas::is_has(&key) {
                let has = ThingEdgeHas::decode(Bytes::Reference(key.bytes()));
                let object = Object::new(has.from());
                let attribute = Attribute::new(has.to());
                let attribute_type = attribute.type_();

                self.add_exclusive_lock_for_unique_constraint(snapshot, &object, attribute)?;
                self.add_exclusive_lock_for_owns_cardinality_constraint(snapshot, &object, attribute_type)?;
            } else if ThingEdgeLinks::is_links(&key) {
                let role_player = ThingEdgeLinks::new(Bytes::Reference(key.bytes()));
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
    ) -> Result<(), Box<ConceptReadError>> {
        let unique_constraint_opt = owner.type_().get_owned_attribute_type_constraint_unique(
            snapshot,
            self.type_manager(),
            attribute.type_(),
        )?;
        if let Some(unique_constraint) = unique_constraint_opt {
            let attribute_key = attribute.vertex();
            let attribute_value = snapshot
                .get_last_existing::<BUFFER_VALUE_INLINE>(attribute_key.into_storage_key().as_reference())
                .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))?
                .ok_or(ConceptReadError::CorruptMissingMandatoryAttributeValue)?;

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

    fn cleanup_relations(&self, snapshot: &mut impl WritableSnapshot) -> Result<(), Box<ConceptWriteError>> {
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
                .iterate_writes_range(&KeyRange::new_within(
                    ObjectVertex::build_prefix_prefix(Prefix::VertexRelation, ObjectVertex::KEYSPACE),
                    ObjectVertex::FIXED_WIDTH_ENCODING,
                ))
                .filter_map(|(key, write)| (!matches!(write, Write::Delete)).then_some(key))
            {
                let relation = Relation::new(ObjectVertex::decode(key.bytes()));
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
                if !self.type_exists(snapshot, relation_type)? {
                    continue;
                }
                let subtypes = relation_type.get_subtypes_transitive(snapshot, self.type_manager())?;
                once(&relation_type).chain(subtypes.into_iter()).try_for_each(|type_| {
                    let is_cascade = true; // TODO: Always consider cascade now, can be changed later.
                    if is_cascade {
                        let mut relations: InstanceIterator<Relation> =
                            self.get_instances_in(snapshot, *type_, <Relation as ThingAPI>::Vertex::KEYSPACE);
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
            .iterate_writes_range(&KeyRange::new_within(ThingEdgeHas::prefix(), ThingEdgeHas::FIXED_WIDTH_ENCODING))
            .filter(|(_, write)| matches!(write, Write::Delete))
        {
            let edge = ThingEdgeHas::decode(Bytes::Reference(key.byte_array()));
            let attribute = Attribute::new(edge.to());
            let is_independent = attribute.type_().is_independent(snapshot, self.type_manager())?;
            if attribute.get_status(snapshot, self) == ConceptStatus::Deleted {
                continue;
            }
            if !is_independent && !attribute.has_owners(snapshot, self) {
                attribute.delete(snapshot, self)?;
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
            if !is_independent && !attribute.has_owners(snapshot, self) {
                self.unput_attribute(snapshot, &attribute)?;
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
            if !self.type_exists(snapshot, attribute_type)? {
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
                        );
                        while let Some(attribute) = attributes.next().transpose()? {
                            if !attribute.has_owners(snapshot, self) {
                                attribute.delete(snapshot, self)?;
                            }
                        }
                    }
                }
                Ok::<(), Box<ConceptWriteError>>(())
            })?;
        }

        Ok(())
    }

    fn validate(&self, snapshot: &mut impl WritableSnapshot) -> Result<(), Vec<ConceptWriteError>> {
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
        collect_errors!(errors, res, |source| DataValidationError::ConceptRead { source });
        res = self.collect_modified_has(snapshot, &mut modified_objects_attribute_types);
        collect_errors!(errors, res, |source| DataValidationError::ConceptRead { source });
        res =
            self.collect_modified_links(snapshot, &mut modified_relations_role_types, &mut modified_objects_role_types);
        collect_errors!(errors, res, |source| DataValidationError::ConceptRead { source });

        for (object, modified_owns) in modified_objects_attribute_types {
            res = CommitTimeValidation::validate_object_has(snapshot, self, object, modified_owns, &mut errors);
            collect_errors!(errors, res, |source| DataValidationError::ConceptRead { source });
        }

        for (object, modified_plays) in modified_objects_role_types {
            res = CommitTimeValidation::validate_object_links(snapshot, self, object, modified_plays, &mut errors);
            collect_errors!(errors, res, |source| DataValidationError::ConceptRead { source });
        }

        for (relation, modified_relates) in modified_relations_role_types {
            res =
                CommitTimeValidation::validate_relation_links(snapshot, self, relation, modified_relates, &mut errors);
            collect_errors!(errors, res, |source| DataValidationError::ConceptRead { source });
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

    fn collect_modified_has(
        &self,
        snapshot: &impl WritableSnapshot,
        out_object_attribute_types: &mut HashMap<Object, HashSet<AttributeType>>,
    ) -> Result<(), Box<ConceptReadError>> {
        for (key, _) in snapshot
            .iterate_writes_range(&KeyRange::new_within(ThingEdgeHas::prefix(), ThingEdgeHas::FIXED_WIDTH_ENCODING))
        {
            let edge = ThingEdgeHas::decode(Bytes::Reference(key.byte_array()));
            let owner = Object::new(edge.from());
            let attribute = Attribute::new(edge.to());
            if self.object_exists(snapshot, owner)? {
                let updated_attribute_types = out_object_attribute_types.entry(owner).or_default();
                updated_attribute_types.insert(attribute.type_());
            }
        }

        Ok(())
    }

    fn collect_modified_links(
        &self,
        snapshot: &impl WritableSnapshot,
        out_relation_role_types: &mut HashMap<Relation, HashSet<RoleType>>,
        out_object_role_types: &mut HashMap<Object, HashSet<RoleType>>,
    ) -> Result<(), Box<ConceptReadError>> {
        for (key, _) in snapshot
            .iterate_writes_range(&KeyRange::new_within(ThingEdgeLinks::prefix(), ThingEdgeLinks::FIXED_WIDTH_ENCODING))
        {
            let edge = ThingEdgeLinks::new(Bytes::reference(key.bytes()));
            let relation = Relation::new(edge.relation());
            let player = Object::new(edge.player());
            let role_type = RoleType::build_from_type_id(edge.role_id());

            if self.object_exists(snapshot, relation)? {
                let updated_role_types = out_relation_role_types.entry(relation).or_default();
                updated_role_types.insert(role_type);
            }

            if self.object_exists(snapshot, player)? {
                let updated_role_types = out_object_role_types.entry(player).or_default();
                updated_role_types.insert(role_type);
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

        OperationTimeValidation::validate_value_type_matches_attribute_type_for_write(
            snapshot,
            self,
            attribute_type,
            value.value_type(),
            value.as_reference(),
        )?;

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

    pub(crate) fn delete_entity(&self, snapshot: &mut impl WritableSnapshot, entity: Entity) {
        let key = entity.vertex().into_storage_key().into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        snapshot.delete(key)
    }

    pub(crate) fn delete_relation(&self, snapshot: &mut impl WritableSnapshot, relation: Relation) {
        let key = relation.vertex().into_storage_key().into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        snapshot.delete(key)
    }

    pub(crate) fn delete_attribute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute: Attribute,
    ) -> Result<(), Box<ConceptWriteError>> {
        let key = attribute.vertex().into_storage_key().into_owned_array();
        snapshot.delete(key);
        Ok(())
    }

    pub(crate) fn uninsert_entity(&self, snapshot: &mut impl WritableSnapshot, entity: Entity) {
        let key = entity.vertex().into_storage_key().into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        snapshot.uninsert(key)
    }

    pub(crate) fn uninsert_relation(&self, snapshot: &mut impl WritableSnapshot, relation: Relation) {
        let key = relation.vertex().into_storage_key().into_owned_array();
        snapshot.unmodifiable_lock_remove(&key);
        snapshot.uninsert(key)
    }

    pub(crate) fn unput_attribute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute: &Attribute,
    ) -> Result<(), Box<ConceptWriteError>> {
        let value = match attribute
            .get_value(snapshot, self)
            .map_err(|error| ConceptWriteError::ConceptRead { source: error })?
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
    ) -> Result<(), Box<ConceptWriteError>> {
        self.set_has_count(snapshot, owner, attribute, 1)
    }

    pub(crate) fn set_has_count(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: impl ObjectAPI,
        attribute: &Attribute,
        count: u64,
    ) -> Result<(), Box<ConceptWriteError>> {
        let attribute_type = attribute.type_();
        let value = attribute.get_value(snapshot, self)?.into_owned();

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

        let has = ThingEdgeHas::new(owner.vertex(), attribute.vertex());
        let has_reverse = ThingEdgeHasReverse::new(attribute.vertex(), owner.vertex());

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

    pub(crate) fn unset_has(&self, snapshot: &mut impl WritableSnapshot, owner: impl ObjectAPI, attribute: &Attribute) {
        let owner_status = owner.get_status(snapshot, self);
        let has = ThingEdgeHas::new(owner.vertex(), attribute.vertex()).into_storage_key().into_owned_array();
        let has_reverse =
            ThingEdgeHasReverse::new(attribute.vertex(), owner.vertex()).into_storage_key().into_owned_array();
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

    pub(crate) fn set_has_ordered(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        attributes: Vec<Attribute>,
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
    ) -> Result<(), Box<ConceptWriteError>> {
        let count: u64 = 1;
        // must be idempotent, so no lock required -- cannot fail

        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.vertex());
        snapshot.put_val(links.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));

        let links_reverse =
            ThingEdgeLinks::build_links_reverse(player.clone().vertex(), relation.clone().vertex(), role_type.vertex());
        snapshot.put_val(links_reverse.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(count)));

        if self.type_manager.relation_index_available(snapshot, relation.type_())? {
            self.relation_index_player_regenerate(snapshot, relation, Object::new(player.vertex()), role_type, 1)?;
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
    ) -> Result<(), Box<ConceptWriteError>> {
        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.vertex());
        let links_reverse = ThingEdgeLinks::build_links_reverse(player.vertex(), relation.vertex(), role_type.vertex());

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
    pub fn unset_links(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.vertex())
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
    pub(crate) fn increment_links_count(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.vertex());
        let count = snapshot
            .get_mapped(links.into_storage_key().as_reference(), |arr| decode_u64(arr.try_into().unwrap()))
            .map_err(|snapshot_err| Box::new(ConceptReadError::SnapshotGet { source: snapshot_err }))?;

        #[cfg(debug_assertions)]
        {
            let links_reverse =
                ThingEdgeLinks::build_links_reverse(player.vertex(), relation.vertex(), role_type.vertex());
            let reverse_count = snapshot
                .get_mapped(links_reverse.into_storage_key().as_reference(), |arr| decode_u64(arr.try_into().unwrap()))
                .unwrap();
            debug_assert_eq!(&count, &reverse_count, "canonical and reverse links edge count mismatch!");
        }

        self.set_links_count(snapshot, relation, player, role_type, count.unwrap_or(0) + 1)
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
    ) -> Result<(), Box<ConceptWriteError>> {
        let links = ThingEdgeLinks::build_links(relation.vertex(), player.vertex(), role_type.vertex());
        let count = snapshot
            .get_mapped(links.into_storage_key().as_reference(), |arr| decode_u64(arr.try_into().unwrap()))
            .map_err(|snapshot_err| Box::new(ConceptReadError::SnapshotGet { source: snapshot_err }))?;

        #[cfg(debug_assertions)]
        {
            let links_reverse =
                ThingEdgeLinks::build_links_reverse(player.vertex(), relation.vertex(), role_type.vertex());
            let reverse_count = snapshot
                .get_mapped(links_reverse.into_storage_key().as_reference(), |arr| decode_u64(arr.try_into().unwrap()))
                .unwrap();
            debug_assert_eq!(&count, &reverse_count, "canonical and reverse links edge count mismatch!");
        }

        debug_assert!(*count.as_ref().unwrap() >= decrement_count);
        self.set_links_count(snapshot, relation, player, role_type, count.unwrap() - decrement_count)
    }

    // TODO:
    // * Call index regenerations when cardinality changes in schema
    //   (create role type, set cardinality annotation, unset cardinality annotation, ...)
    // * Clean up all parts of a relation index to do with a specific role player
    //   after the player has been deleted.
    pub(crate) fn relation_index_player_deleted(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let players = relation
            .get_players(snapshot, self)
            .map_ok(|(roleplayer, _count)| (roleplayer.player(), roleplayer.role_type()));
        for rp in players {
            let (rp_player, rp_role_type) = rp?;
            debug_assert!(!(rp_player == Object::new(player.vertex()) && role_type == rp_role_type));
            let index = ThingEdgeLinksIndex::new(
                player.vertex(),
                rp_player.vertex(),
                relation.vertex(),
                role_type.vertex().type_id_(),
                rp_role_type.vertex().type_id_(),
            );
            snapshot.delete(index.into_storage_key().into_owned_array());
            let index_reverse = ThingEdgeLinksIndex::new(
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
        relation: Relation,
        player: Object,
        role_type: RoleType,
        count_for_player: u64,
    ) -> Result<(), Box<ConceptWriteError>> {
        debug_assert_ne!(count_for_player, 0);
        let players = relation
            .get_players(snapshot, self)
            .map_ok(|(roleplayer, count)| (roleplayer.player(), roleplayer.role_type(), count));
        for rp in players {
            let (rp_player, rp_role_type, rp_count) = rp?;
            let is_same_rp = rp_player == player && rp_role_type == role_type;
            if is_same_rp {
                let repetitions = count_for_player - 1;
                if repetitions > 0 {
                    let index = ThingEdgeLinksIndex::new(
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
                let index = ThingEdgeLinksIndex::new(
                    player.vertex(),
                    rp_player.vertex(),
                    relation.vertex(),
                    role_type.vertex().type_id_(),
                    rp_role_type.vertex().type_id_(),
                );
                snapshot
                    .put_val(index.into_storage_key().into_owned_array(), ByteArray::copy(&encode_u64(rp_repetitions)));
                let player_repetitions = count_for_player;
                let index_reverse = ThingEdgeLinksIndex::new(
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
