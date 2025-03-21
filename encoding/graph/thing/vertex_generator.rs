/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use bytes::{byte_array::ByteArray, Bytes};
use resource::profile::StorageCounters;
use storage::{
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyReference},
    snapshot::{iterator::SnapshotIteratorError, ReadableSnapshot, WritableSnapshot},
    MVCCKey, MVCCStorage,
};

use super::vertex_attribute::{
    BooleanAttributeID, DateAttributeID, DateTimeAttributeID, DateTimeTZAttributeID, DecimalAttributeID,
    DoubleAttributeID, DurationAttributeID, StructAttributeID,
};
use crate::{
    error::EncodingError,
    graph::{
        thing::{
            vertex_attribute::{AttributeID, AttributeVertex, IntegerAttributeID, StringAttributeID},
            vertex_object::{ObjectID, ObjectVertex},
            ThingVertex,
        },
        type_::vertex::{build_type_vertex_prefix_key, TypeID, TypeIDUInt, TypeVertex},
        Typed,
    },
    layout::prefix::Prefix,
    value::{
        boolean_bytes::BooleanBytes, date_bytes::DateBytes, date_time_bytes::DateTimeBytes,
        date_time_tz_bytes::DateTimeTZBytes, decimal_bytes::DecimalBytes, double_bytes::DoubleBytes,
        duration_bytes::DurationBytes, integer_bytes::IntegerBytes, string_bytes::StringBytes,
        struct_bytes::StructBytes,
    },
    AsBytes, Keyable,
};

#[derive(Debug)]
pub struct ThingVertexGenerator {
    entity_ids: Box<[AtomicU64]>,
    relation_ids: Box<[AtomicU64]>,
    large_value_hasher: fn(&[u8]) -> u64,
}

impl Default for ThingVertexGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl ThingVertexGenerator {
    pub fn new() -> Self {
        // TODO: we should create a resizable Vector linked to the id of types/highest id of each type
        //       this will speed up booting time on load (loading this will require MAX types * 3 iterator searches) and reduce memory footprint
        Self::new_with_hasher(seahash::hash)
    }

    pub fn new_with_hasher(large_value_hasher: fn(&[u8]) -> u64) -> Self {
        // TODO: we should create a resizable Vector linked to the id of types/highest id of each type
        //       this will speed up booting time on load (loading this will require MAX types * 3 iterator searches) and reduce memory footprint
        ThingVertexGenerator {
            entity_ids: Self::allocate_empty_ids(),
            relation_ids: Self::allocate_empty_ids(),
            large_value_hasher,
        }
    }

    pub fn load<D>(storage: Arc<MVCCStorage<D>>) -> Result<Self, EncodingError> {
        Self::load_with_hasher(storage, seahash::hash)
    }

    pub fn load_with_hasher<D>(
        storage: Arc<MVCCStorage<D>>,
        large_value_hasher: fn(&[u8]) -> u64,
    ) -> Result<Self, EncodingError> {
        let read_snapshot = storage.clone().open_snapshot_read();
        let entity_types = read_snapshot
            .iterate_range(
                &KeyRange::new_within(
                    build_type_vertex_prefix_key(Prefix::VertexEntityType),
                    Prefix::VertexEntityType.fixed_width_keys(),
                ),
                StorageCounters::DISABLED,
            )
            .collect_cloned_vec(|k, _v| TypeVertex::decode(Bytes::Reference(k.bytes())).type_id_().as_u16())
            .map_err(|err| EncodingError::ExistingTypesRead { source: err })?;
        let relation_types = read_snapshot
            .iterate_range(
                &KeyRange::new_within(
                    build_type_vertex_prefix_key(Prefix::VertexRelationType),
                    Prefix::VertexRelationType.fixed_width_keys(),
                ),
                StorageCounters::DISABLED,
            )
            .collect_cloned_vec(|k, _v| TypeVertex::decode(Bytes::Reference(k.bytes())).type_id_().as_u16())
            .map_err(|err| EncodingError::ExistingTypesRead { source: err })?;
        read_snapshot.close_resources();

        let entity_ids = Self::allocate_empty_ids();
        let relation_ids = Self::allocate_empty_ids();
        for type_id in entity_types {
            let mut max_object_id =
                ObjectVertex::build_entity(TypeID::new(type_id), ObjectID::new(u64::MAX)).to_bytes().into_array();
            bytes::util::increment(&mut max_object_id).unwrap();
            let next_storage_key: StorageKey<'_, { ObjectVertex::LENGTH }> =
                StorageKey::new_ref(ObjectVertex::KEYSPACE, &max_object_id);
            if let Some(prev_bytes) =
                storage.get_prev_raw(next_storage_key.as_reference(), |key, _| Vec::from(key.key()))
            {
                if ObjectVertex::is_entity_vertex(StorageKeyReference::new(ObjectVertex::KEYSPACE, &prev_bytes)) {
                    let object_vertex = ObjectVertex::decode(&prev_bytes);
                    if object_vertex.type_id_() == TypeID::new(type_id) {
                        entity_ids[type_id as usize].store(object_vertex.object_id().as_u64() + 1, Ordering::Relaxed);
                    }
                }
            }
        }
        for type_id in relation_types {
            let mut max_object_id =
                ObjectVertex::build_relation(TypeID::new(type_id), ObjectID::new(u64::MAX)).to_bytes().into_array();
            bytes::util::increment(&mut max_object_id).unwrap();
            let next_storage_key: StorageKey<'_, { ObjectVertex::LENGTH }> =
                StorageKey::new_ref(ObjectVertex::KEYSPACE, &max_object_id);
            if let Some(prev_bytes) =
                storage.get_prev_raw(next_storage_key.as_reference(), |key, _| Vec::from(key.key()))
            {
                if ObjectVertex::is_relation_vertex(StorageKeyReference::new(ObjectVertex::KEYSPACE, &prev_bytes)) {
                    let object_vertex = ObjectVertex::decode(&prev_bytes);
                    if object_vertex.type_id_() == TypeID::new(type_id) {
                        relation_ids[type_id as usize].store(object_vertex.object_id().as_u64() + 1, Ordering::Relaxed);
                    }
                }
            }
        }

        Ok(ThingVertexGenerator { entity_ids, relation_ids, large_value_hasher })
    }

    fn allocate_empty_ids() -> Box<[AtomicU64]> {
        (0..=TypeIDUInt::MAX).map(|_| AtomicU64::new(0)).collect::<Vec<AtomicU64>>().into_boxed_slice()
    }

    pub fn hasher(&self) -> &impl Fn(&[u8]) -> u64 {
        &self.large_value_hasher
    }

    pub fn create_entity<Snapshot>(&self, type_id: TypeID, snapshot: &mut Snapshot) -> ObjectVertex
    where
        Snapshot: WritableSnapshot,
    {
        let entity_id = self.entity_ids[type_id.as_u16() as usize].fetch_add(1, Ordering::Relaxed);
        let vertex = ObjectVertex::build_entity(type_id, ObjectID::new(entity_id));
        snapshot.insert(vertex.into_storage_key().into_owned_array());
        vertex
    }

    pub fn create_relation<Snapshot>(&self, type_id: TypeID, snapshot: &mut Snapshot) -> ObjectVertex
    where
        Snapshot: WritableSnapshot,
    {
        let relation_id = self.relation_ids[type_id.as_u16() as usize].fetch_add(1, Ordering::Relaxed);
        let vertex = ObjectVertex::build_relation(type_id, ObjectID::new(relation_id));
        snapshot.insert(vertex.into_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_boolean<Snapshot>(
        &self,
        type_id: TypeID,
        value: BooleanBytes,
        snapshot: &mut Snapshot,
    ) -> AttributeVertex
    where
        Snapshot: WritableSnapshot,
    {
        let boolean_attribute_id = self.create_attribute_id_boolean(value);
        let vertex = AttributeVertex::new(type_id, AttributeID::Boolean(boolean_attribute_id));
        snapshot.put(vertex.into_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_integer<Snapshot>(
        &self,
        type_id: TypeID,
        value: IntegerBytes,
        snapshot: &mut Snapshot,
    ) -> AttributeVertex
    where
        Snapshot: WritableSnapshot,
    {
        let integer_attribute_id = self.create_attribute_id_integer(value);
        let vertex = AttributeVertex::new(type_id, AttributeID::Integer(integer_attribute_id));
        snapshot.put(vertex.into_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_double<Snapshot>(
        &self,
        type_id: TypeID,
        value: DoubleBytes,
        snapshot: &mut Snapshot,
    ) -> AttributeVertex
    where
        Snapshot: WritableSnapshot,
    {
        let double_attribute_id = self.create_attribute_id_double(value);
        let vertex = AttributeVertex::new(type_id, AttributeID::Double(double_attribute_id));
        snapshot.put(vertex.into_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_decimal<Snapshot>(
        &self,
        type_id: TypeID,
        value: DecimalBytes,
        snapshot: &mut Snapshot,
    ) -> AttributeVertex
    where
        Snapshot: WritableSnapshot,
    {
        let decimal_attribute_id = self.create_attribute_id_decimal(value);
        let vertex = AttributeVertex::new(type_id, AttributeID::Decimal(decimal_attribute_id));
        snapshot.put(vertex.into_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_date<Snapshot>(
        &self,
        type_id: TypeID,
        value: DateBytes,
        snapshot: &mut Snapshot,
    ) -> AttributeVertex
    where
        Snapshot: WritableSnapshot,
    {
        let date_attribute_id = self.create_attribute_id_date(value);
        let vertex = AttributeVertex::new(type_id, AttributeID::Date(date_attribute_id));
        snapshot.put(vertex.into_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_date_time<Snapshot>(
        &self,
        type_id: TypeID,
        value: DateTimeBytes,
        snapshot: &mut Snapshot,
    ) -> AttributeVertex
    where
        Snapshot: WritableSnapshot,
    {
        let date_time_attribute_id = self.create_attribute_id_date_time(value);
        let vertex = AttributeVertex::new(type_id, AttributeID::DateTime(date_time_attribute_id));
        snapshot.put(vertex.into_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_date_time_tz<Snapshot>(
        &self,
        type_id: TypeID,
        value: DateTimeTZBytes,
        snapshot: &mut Snapshot,
    ) -> AttributeVertex
    where
        Snapshot: WritableSnapshot,
    {
        let date_time_tz_attribute_id = self.create_attribute_id_date_time_tz(value);
        let vertex = AttributeVertex::new(type_id, AttributeID::DateTimeTZ(date_time_tz_attribute_id));
        snapshot.put(vertex.into_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_duration<Snapshot>(
        &self,
        type_id: TypeID,
        value: DurationBytes,
        snapshot: &mut Snapshot,
    ) -> AttributeVertex
    where
        Snapshot: WritableSnapshot,
    {
        let duration_attribute_id = self.create_attribute_id_duration(value);
        let vertex = AttributeVertex::new(type_id, AttributeID::Duration(duration_attribute_id));
        snapshot.put(vertex.into_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_id_boolean(&self, value: BooleanBytes) -> BooleanAttributeID {
        BooleanAttributeID::build(value)
    }

    pub fn create_attribute_id_integer(&self, value: IntegerBytes) -> IntegerAttributeID {
        IntegerAttributeID::build(value)
    }

    pub fn create_attribute_id_double(&self, value: DoubleBytes) -> DoubleAttributeID {
        DoubleAttributeID::build(value)
    }

    pub fn create_attribute_id_decimal(&self, value: DecimalBytes) -> DecimalAttributeID {
        DecimalAttributeID::build(value)
    }

    pub fn create_attribute_id_date(&self, value: DateBytes) -> DateAttributeID {
        DateAttributeID::build(value)
    }

    pub fn create_attribute_id_date_time(&self, value: DateTimeBytes) -> DateTimeAttributeID {
        DateTimeAttributeID::build(value)
    }

    pub fn create_attribute_id_date_time_tz(&self, value: DateTimeTZBytes) -> DateTimeTZAttributeID {
        DateTimeTZAttributeID::build(value)
    }

    pub fn create_attribute_id_duration(&self, value: DurationBytes) -> DurationAttributeID {
        DurationAttributeID::build(value)
    }

    ///
    /// We create a unique attribute ID representing the string value.
    /// We guarantee that the same value will map the same ID as long as the value remains mapped, and concurrent creation
    ///   will lead to an isolation error to maintain this invariant. The user should retry.
    ///
    /// If the value is fully removed and recreated it is possible to get a different tail of the ID.
    ///
    /// We do not need to retain a reverse index from String -> ID, since 99.9% of the time the prefix + hash
    /// lets us retrieve the ID from the forward index by prefix (ID -> String).
    ///
    pub fn create_attribute_string<const INLINE_LENGTH: usize, Snapshot>(
        &self,
        type_id: TypeID,
        value: StringBytes<INLINE_LENGTH>,
        snapshot: &mut Snapshot,
    ) -> Result<AttributeVertex, Arc<SnapshotIteratorError>>
    where
        Snapshot: WritableSnapshot,
    {
        let string_attribute_id = self.create_attribute_id_string(type_id, value.as_reference(), snapshot)?;
        let vertex = AttributeVertex::new(type_id, AttributeID::String(string_attribute_id));
        snapshot.put_val(vertex.into_storage_key().into_owned_array(), ByteArray::from(value.bytes()));
        Ok(vertex)
    }

    pub fn create_attribute_id_string<const INLINE_LENGTH: usize, Snapshot>(
        &self,
        type_id: TypeID,
        string: StringBytes<INLINE_LENGTH>,
        snapshot: &mut Snapshot,
    ) -> Result<StringAttributeID, Arc<SnapshotIteratorError>>
    where
        Snapshot: WritableSnapshot,
    {
        if StringAttributeID::is_inlineable(string.as_reference()) {
            Ok(StringAttributeID::build_inline_id(string))
        } else {
            let id = StringAttributeID::build_hashed_id(type_id, string, snapshot, &self.large_value_hasher)?;
            let hash = id.get_hash_hash();
            let lock =
                ByteArray::copy_concat([&Prefix::VertexAttribute.prefix_id().to_bytes(), &type_id.to_bytes(), &hash]);
            snapshot.exclusive_lock_add(lock);
            Ok(id)
        }
    }

    pub fn find_attribute_id_string_noinline<const INLINE_LENGTH: usize, Snapshot>(
        &self,
        type_id: TypeID,
        string: StringBytes<INLINE_LENGTH>,
        snapshot: &Snapshot,
    ) -> Result<Option<StringAttributeID>, Arc<SnapshotIteratorError>>
    where
        Snapshot: ReadableSnapshot,
    {
        assert!(!StringAttributeID::is_inlineable(string.as_reference()));
        StringAttributeID::find_hashed_id(type_id, string, snapshot, &self.large_value_hasher)
    }

    pub fn create_attribute_struct<const INLINE_LENGTH: usize, Snapshot>(
        &self,
        type_id: TypeID,
        value: StructBytes<'_, INLINE_LENGTH>,
        snapshot: &mut Snapshot,
    ) -> Result<AttributeVertex, Arc<SnapshotIteratorError>>
    where
        Snapshot: WritableSnapshot,
    {
        let struct_attribute_id = self.create_attribute_id_struct(type_id, value.as_reference(), snapshot)?;
        let vertex = AttributeVertex::new(type_id, AttributeID::Struct(struct_attribute_id));
        snapshot.put_val(vertex.into_storage_key().into_owned_array(), ByteArray::from(value.bytes()));
        Ok(vertex)
    }

    pub fn create_attribute_id_struct<const INLINE_LENGTH: usize, Snapshot>(
        &self,
        type_id: TypeID,
        struct_bytes: StructBytes<'_, INLINE_LENGTH>,
        snapshot: &mut Snapshot,
    ) -> Result<StructAttributeID, Arc<SnapshotIteratorError>>
    where
        Snapshot: WritableSnapshot,
    {
        // We don't inline structs
        let id = StructAttributeID::build_hashed_id(type_id, struct_bytes, snapshot, &self.large_value_hasher)?;
        let hash = id.get_hash_hash();
        let lock =
            ByteArray::copy_concat([&Prefix::VertexAttribute.prefix_id().to_bytes(), &type_id.to_bytes(), &hash]);
        snapshot.exclusive_lock_add(lock);
        Ok(id)
    }

    pub fn find_attribute_id_struct<const INLINE_LENGTH: usize, Snapshot>(
        &self,
        type_id: TypeID,
        struct_bytes: StructBytes<'_, INLINE_LENGTH>,
        snapshot: &Snapshot,
    ) -> Result<Option<StructAttributeID>, Arc<SnapshotIteratorError>>
    where
        Snapshot: ReadableSnapshot,
    {
        StructAttributeID::find_hashed_id(type_id, struct_bytes, snapshot, &self.large_value_hasher)
    }

    pub fn reset(&mut self) {
        self.entity_ids.iter().for_each(|id| id.store(0, Ordering::SeqCst));
        self.relation_ids.iter().for_each(|id| id.store(0, Ordering::SeqCst));
    }
}
