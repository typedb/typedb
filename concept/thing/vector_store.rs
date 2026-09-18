/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Primary store for vector attribute values.
//!
//! Vector attribute values are NOT materialised in the KV store: the attribute vertex key is
//! written with an empty value (existence + MVCC visibility), and the single copy of the vector
//! lives here, in one usearch HNSW index per attribute type. The mapping is derivable in both
//! directions with no side table: the vector attribute ID's disambiguated hash is exactly 8
//! bytes and is used as the usearch key ([`VectorAttributeID::as_vector_index_key`]).
//!
//! MVCC: this store is versionless. Attribute IDs are value-derived, so a key -> vector mapping
//! is immutable; whether an attribute *exists* for a snapshot is answered by the KV store key.
//! Vectors are therefore never removed on delete (older snapshots may still read them) and
//! inserts are idempotent. Application happens inside the storage commit path (CommitObserver),
//! after the KV write and before the watermark advances, so a checkpoint at watermark W always
//! contains every vector of commits <= W; recovery replays only the WAL tail through the same
//! observer.

use std::{
    collections::HashMap,
    fmt,
    io::{Read, Write},
    sync::{Arc, RwLock},
};

use bytes::{Bytes, byte_array::ByteArray};
use encoding::{
    graph::{
        Typed,
        thing::vertex_attribute::{AttributeVertex, VectorAttributeID},
        type_::vertex::TypeID,
    },
    value::vector_bytes::VectorBytes,
};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use serde::{Deserialize, Serialize};
use storage::{
    CommitObserver, key_value::StorageKeyArray, keyspace::KeyspaceId, recovery::checkpoint::CheckpointAdditionalData,
    sequence_number::SequenceNumber,
};
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

pub struct VectorStore {
    indexes: RwLock<HashMap<TypeID, Arc<TypeVectorIndex>>>,
}

struct TypeVectorIndex {
    // ponytail: one big RwLock per type index; usearch's internal locking allows finer
    // concurrency (lock-free adds after reserve) if this ever contends
    index: RwLock<Index>,
}

impl TypeVectorIndex {
    fn new(dimensions: usize) -> Self {
        let options = IndexOptions {
            dimensions,
            metric: MetricKind::Cos,
            quantization: ScalarKind::F32,
            connectivity: 0,     // usearch defaults
            expansion_add: 0,    // usearch defaults
            expansion_search: 0, // usearch defaults
            multi: false,
        };
        let index = usearch::new_index(&options)
            .unwrap_or_else(|err| panic!("failed to create vector index (dimensions: {dimensions}): {err}"));
        Self { index: RwLock::new(index) }
    }

    fn add(&self, key: u64, vector: &[f32]) {
        let index = self.index.write().unwrap();
        if index.contains(key) {
            return; // idempotent: value-derived key, same vector (WAL replay, duplicate puts)
        }
        if index.size() >= index.capacity() {
            index
                .reserve((index.capacity() * 2).max(1024))
                .unwrap_or_else(|err| panic!("failed to grow vector index: {err}"));
        }
        // the vector store is the only copy of the value: failing to add would silently lose
        // committed data, so a broken index must announce itself
        index.add(key, vector).unwrap_or_else(|err| panic!("failed to add vector to index: {err}"));
    }

    fn get(&self, key: u64) -> Option<Vec<f32>> {
        let index = self.index.read().unwrap();
        let mut buffer = vec![0.0f32; index.dimensions()];
        let found = index.get(key, &mut buffer).unwrap_or_else(|err| panic!("failed to read vector from index: {err}"));
        (found > 0).then_some(buffer)
    }

    fn search(&self, query: &[f32], count: usize) -> Vec<(u64, f32)> {
        let index = self.index.read().unwrap();
        let matches = index.search(query, count).unwrap_or_else(|err| panic!("vector index search failed: {err}"));
        matches.keys.into_iter().zip(matches.distances).collect()
    }

    fn size(&self) -> usize {
        self.index.read().unwrap().size()
    }
}

impl VectorStore {
    pub fn new() -> Self {
        Self { indexes: RwLock::new(HashMap::new()) }
    }

    fn index_for_type(&self, type_id: TypeID, dimensions_if_created: usize) -> Arc<TypeVectorIndex> {
        if let Some(index) = self.indexes.read().unwrap().get(&type_id) {
            return index.clone();
        }
        self.indexes
            .write()
            .unwrap()
            .entry(type_id)
            .or_insert_with(|| Arc::new(TypeVectorIndex::new(dimensions_if_created)))
            .clone()
    }

    pub fn add(&self, vertex: AttributeVertex, vector: &[f32]) {
        let id = vertex.attribute_id().unwrap_vector();
        self.index_for_type(vertex.type_id_(), vector.len()).add(id.as_vector_index_key(), vector);
    }

    pub fn get_vector(&self, vertex: AttributeVertex) -> Option<Vec<f32>> {
        self.get_by_key(vertex.type_id_(), vertex.attribute_id().unwrap_vector().as_vector_index_key())
    }

    pub fn get_by_key(&self, type_id: TypeID, key: u64) -> Option<Vec<f32>> {
        let index = self.indexes.read().unwrap().get(&type_id)?.clone();
        index.get(key)
    }

    /// Approximate nearest neighbours: (attribute ID, cosine distance) pairs, nearest first.
    /// Candidates only — existence for a snapshot must be re-checked against the KV store.
    pub fn search(&self, type_id: TypeID, query: &[f32], count: usize) -> Vec<(VectorAttributeID, f32)> {
        let Some(index) = self.indexes.read().unwrap().get(&type_id).cloned() else { return Vec::new() };
        index
            .search(query, count)
            .into_iter()
            .map(|(key, distance)| (VectorAttributeID::from_vector_index_key(key), distance))
            .collect()
    }

    pub fn indexed_vector_count(&self, type_id: TypeID) -> usize {
        self.indexes.read().unwrap().get(&type_id).map_or(0, |index| index.size())
    }

    /// Drop all indexes (database reset).
    pub fn clear(&self) {
        self.indexes.write().unwrap().clear();
    }
}

impl Default for VectorStore {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for VectorStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let indexes = self.indexes.read().unwrap();
        let mut map = f.debug_map();
        for (type_id, index) in indexes.iter() {
            map.entry(type_id, &index.size());
        }
        map.finish()
    }
}

impl CommitObserver for VectorStore {
    fn owns_value(&self, keyspace_id: KeyspaceId, key: &[u8]) -> bool {
        AttributeVertex::is_vector_attribute_vertex(keyspace_id, key)
    }

    fn apply(
        &self,
        _sequence_number: SequenceNumber,
        owned: &[(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)],
    ) {
        for (key, value) in owned {
            let vertex = AttributeVertex::decode(key.bytes());
            assert!(!value.is_empty(), "vector attribute committed without a value: {vertex:?}");
            let vector = VectorBytes::new(Bytes::<BUFFER_VALUE_INLINE>::Reference(value)).as_vector();
            self.add(vertex, &vector);
        }
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// Checkpoint format: our own versioned envelope around usearch's serialisation (usearch's header
/// is unverified on load, and we need dimensions to reconstruct the index options).
#[derive(Serialize, Deserialize)]
struct VectorStoreCheckpointData {
    format_version: u32,
    // (type id, dimensions, serialised usearch index)
    indexes: Vec<(u16, u64, Vec<u8>)>,
}

const VECTOR_STORE_CHECKPOINT_FORMAT_VERSION: u32 = 1;

impl CheckpointAdditionalData for VectorStore {
    const NAME: &'static str = "VECTOR_STORE";

    fn serialise_into(&self, writer: &mut impl Write) -> bincode::Result<()> {
        let indexes = self.indexes.read().unwrap();
        let mut serialised = Vec::with_capacity(indexes.len());
        for (type_id, type_index) in indexes.iter() {
            let index = type_index.index.read().unwrap();
            let mut buffer = vec![0u8; index.serialized_length()];
            index.save_to_buffer(&mut buffer).unwrap_or_else(|err| panic!("failed to serialise vector index: {err}"));
            serialised.push((u16::from_be_bytes(type_id.to_bytes()), index.dimensions() as u64, buffer));
        }
        drop(indexes);
        let data =
            VectorStoreCheckpointData { format_version: VECTOR_STORE_CHECKPOINT_FORMAT_VERSION, indexes: serialised };
        bincode::serialize_into(writer, &data)
    }

    fn deserialise_from(reader: &mut impl Read) -> bincode::Result<Self> {
        let data: VectorStoreCheckpointData = bincode::deserialize_from(reader)?;
        assert_eq!(
            data.format_version, VECTOR_STORE_CHECKPOINT_FORMAT_VERSION,
            "unsupported vector store checkpoint format version"
        );
        let store = VectorStore::new();
        let mut indexes = store.indexes.write().unwrap();
        for (type_id, dimensions, buffer) in data.indexes {
            let type_index = TypeVectorIndex::new(dimensions as usize);
            type_index
                .index
                .write()
                .unwrap()
                .load_from_buffer(&buffer)
                .unwrap_or_else(|err| panic!("failed to load vector index from checkpoint: {err}"));
            indexes.insert(TypeID::decode(type_id.to_be_bytes()), Arc::new(type_index));
        }
        drop(indexes);
        Ok(store)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn vertex(type_id: u16, key: u64) -> AttributeVertex {
        use encoding::graph::thing::vertex_attribute::AttributeID;
        AttributeVertex::new(
            TypeID::decode(type_id.to_be_bytes()),
            AttributeID::Vector(VectorAttributeID::from_vector_index_key(key)),
        )
    }

    #[test]
    fn add_get_search_roundtrip() {
        let store = VectorStore::new();
        store.add(vertex(1, 100), &[1.0, 0.0, 0.0]);
        store.add(vertex(1, 200), &[0.0, 1.0, 0.0]);
        store.add(vertex(1, 100), &[1.0, 0.0, 0.0]); // idempotent re-add

        assert_eq!(store.get_vector(vertex(1, 100)), Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(store.get_vector(vertex(1, 300)), None);
        assert_eq!(store.get_vector(vertex(2, 100)), None);
        assert_eq!(store.indexed_vector_count(TypeID::decode(1u16.to_be_bytes())), 2);

        let results = store.search(TypeID::decode(1u16.to_be_bytes()), &[0.9, 0.1, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.as_vector_index_key(), 100);
    }

    #[test]
    fn checkpoint_roundtrip() {
        let store = VectorStore::new();
        store.add(vertex(1, 100), &[1.0, 0.0]);
        store.add(vertex(7, 200), &[0.0, 1.0, 0.0, 0.0]);

        let mut buffer = Vec::new();
        store.serialise_into(&mut buffer).unwrap();
        let loaded = VectorStore::deserialise_from(&mut buffer.as_slice()).unwrap();

        assert_eq!(loaded.get_vector(vertex(1, 100)), Some(vec![1.0, 0.0]));
        assert_eq!(loaded.get_vector(vertex(7, 200)), Some(vec![0.0, 1.0, 0.0, 0.0]));
    }
}
