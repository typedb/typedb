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
    error::Error,
    fmt, fs,
    fs::File,
    io,
    io::Write,
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
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
    // set by every successful add, cleared when a checkpoint serialises this index: an index
    // that is clean at save time is reused from the previous checkpoint instead of
    // re-serialised (serialisation holds the read lock, stalling every vector commit)
    dirty: AtomicBool,
}

type VectorStoreError = Box<dyn Error + Send + Sync>;

impl TypeVectorIndex {
    fn new(dimensions: usize) -> Result<Self, VectorStoreError> {
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
            .map_err(|err| format!("failed to create vector index (dimensions: {dimensions}): {err}"))?;
        Ok(Self { index: RwLock::new(index), dirty: AtomicBool::new(false) })
    }

    fn dimensions(&self) -> usize {
        self.index.read().unwrap().dimensions()
    }

    /// Pre-commit validation: everything `add` needs that can fail is checked or allocated here,
    /// so that failures abort the commit before it is durably written.
    fn prepare(&self, dimensions: usize, additional: usize) -> Result<(), VectorStoreError> {
        let index = self.index.write().unwrap();
        if index.dimensions() != dimensions {
            return Err(format!(
                "vector dimension mismatch: index has {} dimensions, vector has {dimensions}",
                index.dimensions()
            )
            .into());
        }
        let required = index.size() + additional;
        if required > index.capacity() {
            index
                .reserve((index.capacity() * 2).max(1024).max(required))
                .map_err(|err| format!("failed to grow vector index: {err}"))?;
        }
        Ok(())
    }

    fn add(&self, key: u64, vector: &[f32]) -> Result<(), VectorStoreError> {
        let index = self.index.write().unwrap();
        if index.contains(key) {
            return Ok(()); // idempotent: value-derived key, same vector (WAL replay, duplicate puts)
        }
        if index.dimensions() != vector.len() {
            return Err(format!(
                "vector dimension mismatch: index has {} dimensions, vector has {}",
                index.dimensions(),
                vector.len()
            )
            .into());
        }
        if index.size() >= index.capacity() {
            index
                .reserve((index.capacity() * 2).max(1024))
                .map_err(|err| format!("failed to grow vector index: {err}"))?;
        }
        // the vector store is the only copy of the value: failing to add would silently lose
        // committed data, so a broken index must surface the error
        index.add(key, vector).map_err(|err| format!("failed to add vector to index: {err}"))?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
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

    fn index_for_type(&self, type_id: TypeID, dimensions_if_created: usize) -> Result<Arc<TypeVectorIndex>, VectorStoreError> {
        if let Some(index) = self.indexes.read().unwrap().get(&type_id) {
            return Ok(index.clone());
        }
        let mut indexes = self.indexes.write().unwrap();
        if let Some(index) = indexes.get(&type_id) {
            return Ok(index.clone());
        }
        let index = Arc::new(TypeVectorIndex::new(dimensions_if_created)?);
        indexes.insert(type_id, index.clone());
        Ok(index)
    }

    pub fn add(&self, vertex: AttributeVertex, vector: &[f32]) -> Result<(), VectorStoreError> {
        let id = vertex.attribute_id().unwrap_vector();
        self.index_for_type(vertex.type_id_(), vector.len())?.add(id.as_vector_index_key(), vector)
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

    /// Drop indexes whose type is no longer a live vector attribute type (or whose dimensions no
    /// longer match the schema): TypeIDs are reused after undefine, so a stale index would reject
    /// every vector of a future type with the same TypeID and different dimensions.
    ///
    /// Called on schema commit. ponytail: a read snapshot opened before the undefine commit that
    /// still iterates deleted instances of the undefined type loses access to their vectors; a
    /// versioned drop (defer until the watermark passes all open readers) closes that window.
    pub fn retain_indexes(&self, is_live: impl Fn(TypeID, usize) -> bool) {
        self.indexes.write().unwrap().retain(|type_id, index| is_live(*type_id, index.dimensions()));
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

    /// Runs before the commit is durably written: creates missing indexes, checks dimensions, and
    /// reserves capacity, so that `apply` (which runs after the KV write, where a failure can no
    /// longer abort the commit) cannot fail for any data-dependent reason.
    fn validate(
        &self,
        owned: &[(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut pending: HashMap<TypeID, (usize, usize)> = HashMap::new();
        for (key, value) in owned {
            let vertex = AttributeVertex::decode(key.bytes());
            if value.is_empty() {
                return Err(format!("vector attribute committed without a value: {vertex:?}").into());
            }
            let dimensions = value.len() / VectorBytes::<BUFFER_VALUE_INLINE>::ELEMENT_LENGTH;
            let (existing_dimensions, count) = pending.entry(vertex.type_id_()).or_insert((dimensions, 0));
            if *existing_dimensions != dimensions {
                return Err(format!(
                    "conflicting vector dimensions ({existing_dimensions} and {dimensions}) for one attribute type in a single commit"
                )
                .into());
            }
            *count += 1;
        }
        for (type_id, (dimensions, count)) in pending {
            self.index_for_type(type_id, dimensions)?.prepare(dimensions, count)?;
        }
        Ok(())
    }

    fn apply(
        &self,
        _sequence_number: SequenceNumber,
        owned: &[(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for (key, value) in owned {
            let vertex = AttributeVertex::decode(key.bytes());
            let vector = VectorBytes::new(Bytes::<BUFFER_VALUE_INLINE>::Reference(value)).as_vector();
            self.add(vertex, &vector)?;
        }
        Ok(())
    }
}

/// Checkpoint format: a directory of plain usearch index files — one per attribute type, named
/// `{type id hex}-{metric}.usearch`, each exactly usearch's own on-disk format (openable with any
/// usearch tooling) — plus a text `MANIFEST`. The manifest carries what usearch's unverified file
/// header can't be trusted for: a format version, the dimensions needed to reconstruct index
/// options, and each file's byte length as a torn-file guard. One line per index:
/// `{type id hex} {metric} {dimensions} {file length}`.
const MANIFEST_FILE_NAME: &str = "MANIFEST";
const MANIFEST_VERSION_LINE: &str = "version 1";
const METRIC_NAME: &str = "cosine";

fn index_file_name(type_id: u16) -> String {
    format!("{type_id:#06x}-{METRIC_NAME}.usearch")
}

fn invalid_data(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl CheckpointAdditionalData for VectorStore {
    const NAME: &'static str = "VECTOR_STORE";

    fn save_into_dir(&self, dir: &Path, previous: Option<&Path>) -> io::Result<()> {
        let indexes = self.indexes.read().unwrap();
        let mut manifest = format!("{MANIFEST_VERSION_LINE}\n");
        for (type_id, type_index) in indexes.iter() {
            let type_id = u16::from_be_bytes(type_id.to_bytes());
            let file_name = index_file_name(type_id);
            let path = dir.join(&file_name);

            // clean index: its last serialisation is in the previous checkpoint (clean means no
            // add since the last save, and load-from-checkpoint starts clean) - hard-link it
            // (fall back to copy) instead of re-serialising under the index read lock
            let reused = !type_index.dirty.swap(false, Ordering::Acquire)
                && previous
                    .map(|previous| previous.join(&file_name))
                    .filter(|file| file.exists())
                    .is_some_and(|prev| {
                        fs::hard_link(&prev, &path).or_else(|_| fs::copy(&prev, &path).map(|_| ())).is_ok()
                    });
            let dimensions = if reused {
                // contents were fsynced when the previous checkpoint wrote them
                type_index.dimensions()
            } else {
                let index = type_index.index.read().unwrap();
                index.save(path.to_str().unwrap()).map_err(|err| {
                    // the swap above cleared the flag for a save that did not happen
                    type_index.dirty.store(true, Ordering::Release);
                    io::Error::other(format!("failed to save vector index {file_name}: {err}"))
                })?;
                // usearch's save is fwrite+fclose with no fsync
                let file = File::open(&path)?;
                file.sync_all()?;
                index.dimensions()
            };
            let file_length = fs::metadata(&path)?.len();
            manifest.push_str(&format!("{type_id:#06x} {METRIC_NAME} {dimensions} {file_length}\n"));
        }
        let mut manifest_file = File::create(dir.join(MANIFEST_FILE_NAME))?;
        manifest_file.write_all(manifest.as_bytes())?;
        manifest_file.sync_all()?;
        Ok(())
    }

    fn load_from_dir(dir: &Path) -> io::Result<Self> {
        let manifest = fs::read_to_string(dir.join(MANIFEST_FILE_NAME))?;
        let mut lines = manifest.lines();
        let version_line = lines.next().unwrap_or("");
        if version_line != MANIFEST_VERSION_LINE {
            return Err(invalid_data(format!("unsupported vector store manifest version: '{version_line}'")));
        }
        let store = VectorStore::new();
        let mut indexes = store.indexes.write().unwrap();
        for line in lines {
            let parts: Vec<&str> = line.split_whitespace().collect();
            let &[type_id, metric, dimensions, file_length] = parts.as_slice() else {
                return Err(invalid_data(format!("malformed vector store manifest line: '{line}'")));
            };
            let type_id = type_id
                .strip_prefix("0x")
                .and_then(|hex| u16::from_str_radix(hex, 16).ok())
                .ok_or_else(|| invalid_data(format!("malformed type id in vector store manifest: '{line}'")))?;
            if metric != METRIC_NAME {
                return Err(invalid_data(format!("unsupported vector index metric: '{metric}'")));
            }
            let dimensions: usize = dimensions
                .parse()
                .map_err(|_| invalid_data(format!("malformed dimensions in vector store manifest: '{line}'")))?;
            let file_length: u64 = file_length
                .parse()
                .map_err(|_| invalid_data(format!("malformed file length in vector store manifest: '{line}'")))?;

            let file_name = index_file_name(type_id);
            let path = dir.join(&file_name);
            let actual_length = fs::metadata(&path)?.len();
            if actual_length != file_length {
                return Err(invalid_data(format!(
                    "vector index file {file_name} is {actual_length} bytes, manifest says {file_length}"
                )));
            }
            let type_index = TypeVectorIndex::new(dimensions)
                .map_err(|err| invalid_data(format!("failed to create vector index {file_name}: {err}")))?;
            type_index
                .index
                .write()
                .unwrap()
                .load(path.to_str().unwrap())
                .map_err(|err| invalid_data(format!("failed to load vector index {file_name}: {err}")))?;
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
        store.add(vertex(1, 100), &[1.0, 0.0, 0.0]).unwrap();
        store.add(vertex(1, 200), &[0.0, 1.0, 0.0]).unwrap();
        store.add(vertex(1, 100), &[1.0, 0.0, 0.0]).unwrap(); // idempotent re-add

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
        store.add(vertex(1, 100), &[1.0, 0.0]).unwrap();
        store.add(vertex(7, 200), &[0.0, 1.0, 0.0, 0.0]).unwrap();

        let dir = std::env::temp_dir().join(format!("vector_store_checkpoint_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        store.save_into_dir(&dir, None).unwrap();

        // the on-disk files are plain usearch indexes plus a text manifest
        assert!(dir.join("0x0001-cosine.usearch").exists());
        assert!(dir.join("0x0007-cosine.usearch").exists());
        assert!(fs::read_to_string(dir.join(MANIFEST_FILE_NAME)).unwrap().starts_with(MANIFEST_VERSION_LINE));

        let loaded = VectorStore::load_from_dir(&dir).unwrap();
        assert_eq!(loaded.get_vector(vertex(1, 100)), Some(vec![1.0, 0.0]));
        assert_eq!(loaded.get_vector(vertex(7, 200)), Some(vec![0.0, 1.0, 0.0, 0.0]));

        // a truncated index file must be rejected by the manifest's length check
        let index_file = dir.join("0x0001-cosine.usearch");
        let bytes = fs::read(&index_file).unwrap();
        fs::write(&index_file, &bytes[..bytes.len() - 1]).unwrap();
        assert!(VectorStore::load_from_dir(&dir).is_err());

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn checkpoint_save_reuses_clean_index_files_and_reserialises_dirty_ones() {
        let store = VectorStore::new();
        store.add(vertex(1, 100), &[1.0, 0.0]).unwrap();
        store.add(vertex(7, 200), &[0.0, 1.0]).unwrap();

        let root = std::env::temp_dir().join(format!("vector_store_dirty_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let (first, second) = (root.join("first"), root.join("second"));
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();

        store.save_into_dir(&first, None).unwrap();
        store.add(vertex(7, 300), &[0.0, -1.0]).unwrap(); // dirties only type 7
        store.save_into_dir(&second, Some(&first)).unwrap();

        // type 1 was clean: its file is reused from the first checkpoint (hard link or identical copy)
        let first_clean = fs::read(first.join("0x0001-cosine.usearch")).unwrap();
        assert_eq!(fs::read(second.join("0x0001-cosine.usearch")).unwrap(), first_clean);

        // the second checkpoint is complete and loadable, including the re-serialised dirty index
        let loaded = VectorStore::load_from_dir(&second).unwrap();
        assert_eq!(loaded.get_vector(vertex(1, 100)), Some(vec![1.0, 0.0]));
        assert_eq!(loaded.get_vector(vertex(7, 200)), Some(vec![0.0, 1.0]));
        assert_eq!(loaded.get_vector(vertex(7, 300)), Some(vec![0.0, -1.0]));

        // a loaded store starts clean: a save with no adds since reuses every file
        let third = root.join("third");
        fs::create_dir_all(&third).unwrap();
        loaded.save_into_dir(&third, Some(&second)).unwrap();
        assert_eq!(fs::read(third.join("0x0007-cosine.usearch")).unwrap(), fs::read(second.join("0x0007-cosine.usearch")).unwrap());

        fs::remove_dir_all(&root).unwrap();
    }
}
