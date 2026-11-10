/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    error::Error,
    fmt, fs,
    fs::File,
    io,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::{Bytes, byte_array::ByteArray};
use encoding::{
    graph::{
        Typed,
        thing::vertex_attribute::{AttributeID, AttributeVertex, VectorAttributeID},
        type_::vertex::{TypeID, TypeVertexEncoding},
    },
    value::{
        value_type::{ValueType, ValueTypeCategory},
        vector_bytes::VectorBytes,
    },
};
use error::{TypeDBError, typedb_error};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    SecondaryStore,
    keyspace::{KeyspaceId, KeyspaceSet},
    recovery::checkpoint::CheckpointAdditionalData,
    sequence_number::SequenceNumber,
    snapshot::{buffer::OperationsBuffer, write::Write as StorageWrite},
};
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

use crate::type_::type_manager::type_cache::TypeCache;

pub struct VectorIndex {
    per_type_indices: RwLock<HashMap<TypeID, Arc<PerTypeVectorIndex>>>,
    live_directory: RwLock<Option<PathBuf>>,
}

impl VectorIndex {
    pub fn new() -> Self {
        Self { per_type_indices: RwLock::new(HashMap::new()), live_directory: RwLock::new(None) }
    }

    pub fn set_live_directory(&self, directory: PathBuf) {
        *self.live_directory.write().unwrap() = Some(directory);
    }

    fn get_or_create_index_for_type(
        &self,
        type_id: TypeID,
        dimensions_if_created: usize,
    ) -> Result<Arc<PerTypeVectorIndex>, VectorIndexError> {
        if let Some(index) = self.per_type_indices.read().unwrap().get(&type_id) {
            return Ok(index.clone());
        }
        let mut indices = self.per_type_indices.write().unwrap();
        if let Some(index) = indices.get(&type_id) {
            return Ok(index.clone());
        }
        let index = Arc::new(PerTypeVectorIndex::new(dimensions_if_created)?);
        indices.insert(type_id, index.clone());
        Ok(index)
    }

    pub fn get_count_for_type(&self, type_id: TypeID) -> usize {
        self.per_type_indices.read().unwrap().get(&type_id).map_or(0, |index| index.get_size())
    }

    pub fn add_instance(&self, vertex: AttributeVertex, vector: &[f32]) -> Result<(), VectorIndexError> {
        let id = vertex.attribute_id().unwrap_vector();
        self.get_or_create_index_for_type(vertex.type_id_(), vector.len())?
            .add_value(id.as_type_vector_index_key(), vector)
    }

    pub fn list_instances_by_ann(
        &self,
        type_id: TypeID,
        reference_point: &[f32],
        count: usize,
    ) -> Vec<(AttributeVertex, f32)> {
        let Some(index) = self.per_type_indices.read().unwrap().get(&type_id).cloned() else { return Vec::new() };
        index
            .list_by_ann(reference_point, count)
            .into_iter()
            .map(|(key, distance)| {
                let id = AttributeID::Vector(VectorAttributeID::from_type_vector_index_key(key));
                (AttributeVertex::new(type_id, id), distance)
            })
            .collect()
    }

    pub fn get_value_by_attr_vertex(&self, vertex: AttributeVertex) -> Option<Vec<f32>> {
        self.get_value_by_key(vertex.type_id_(), vertex.attribute_id().unwrap_vector().as_type_vector_index_key())
    }

    pub fn get_value_by_key(&self, type_id: TypeID, key: u64) -> Option<Vec<f32>> {
        let index = self.per_type_indices.read().unwrap().get(&type_id)?.clone();
        index.get_value(key)
    }

    pub fn retain_live_types(&self, type_cache: &TypeCache) {
        let live: HashMap<TypeID, usize> = type_cache
            .get_attribute_types_with_value_types()
            .filter_map(|(attribute_type, value_type)| match value_type {
                ValueType::Vector(definition_key) => {
                    let parameters = type_cache.get_vector_definition(definition_key).parameters;
                    Some((attribute_type.vertex().type_id_(), parameters.length as usize))
                }
                _ => None,
            })
            .collect();
        self.per_type_indices
            .write()
            .unwrap()
            .retain(|type_id, index| live.get(type_id) == Some(&index.get_dimensions()));
    }

    pub fn clear(&self) {
        self.per_type_indices.write().unwrap().clear();
    }
}

impl SecondaryStore for VectorIndex {
    fn stores_value(&self, keyspace_id: KeyspaceId, key: &[u8]) -> bool {
        AttributeVertex::is_vector_attribute_vertex(keyspace_id, key)
    }

    fn validate_commit(&self, operations: &OperationsBuffer) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut pending: HashMap<TypeID, (usize, usize)> = HashMap::new();
        for (vertex, value) in stored_vector_writes(operations) {
            if value.is_empty() {
                return Err(Box::new(VectorIndexError::CommittedWithoutValue { vertex: format!("{vertex:?}") }));
            }
            let dimensions = value.len() / VectorBytes::<BUFFER_VALUE_INLINE>::ELEMENT_LENGTH;
            let (existing_dimensions, count) = pending.entry(vertex.type_id_()).or_insert((dimensions, 0));
            if *existing_dimensions != dimensions {
                return Err(Box::new(VectorIndexError::ConflictingDimensionsInCommit {
                    first: *existing_dimensions,
                    second: dimensions,
                }));
            }
            *count += 1;
        }
        for (type_id, (dimensions, count)) in pending {
            self.get_or_create_index_for_type(type_id, dimensions)?.reserve(dimensions, count)?;
        }
        Ok(())
    }

    fn apply_commit(
        &self,
        _sequence_number: SequenceNumber,
        operations: &OperationsBuffer,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for (vertex, value) in stored_vector_writes(operations) {
            let vector = VectorBytes::new(Bytes::<BUFFER_VALUE_INLINE>::Reference(value)).as_vector();
            self.add_instance(vertex, &vector)?;
        }
        Ok(())
    }
}

impl CheckpointAdditionalData for VectorIndex {
    const NAME: &'static str = "VECTOR_INDEX";

    fn save_to_path(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)?;
        let live_directory = self.live_directory.read().unwrap().clone();
        let indexes = self.per_type_indices.read().unwrap();
        let mut manifest = format!("{MANIFEST_VERSION_LINE}\n");
        for (type_id, type_index) in indexes.iter() {
            let type_id = u16::from_be_bytes(type_id.to_bytes());
            let dir_name = type_dir_name(type_id);
            let live_type_dir = live_directory.as_ref().map(|live| live.join(&dir_name));
            manifest.push_str(&type_index.checkpoint_to(type_id, live_type_dir.as_deref(), &path.join(&dir_name))?);
        }
        // Written and fsynced last: a parseable MANIFEST implies every file it names is durable.
        let mut manifest_file = File::create(path.join(MANIFEST_FILE_NAME))?;
        manifest_file.write_all(manifest.as_bytes())?;
        manifest_file.sync_all()?;
        File::open(path)?.sync_all()?;
        Ok(())
    }

    fn load_from_path(dir: &Path) -> io::Result<Self> {
        let manifest = fs::read_to_string(dir.join(MANIFEST_FILE_NAME))?;
        let mut lines = manifest.lines();
        let version_line = lines.next().unwrap_or("");
        if version_line != MANIFEST_VERSION_LINE {
            return Err(invalid_data(format!("unsupported vector store manifest version: '{version_line}'")));
        }
        let mut entries: HashMap<u16, Vec<ManifestEntry>> = HashMap::new();
        for line in lines {
            let (type_id, entry) = parse_manifest_line(line)?;
            entries.entry(type_id).or_default().push(entry);
        }
        let store = VectorIndex::new();
        let mut indexes = store.per_type_indices.write().unwrap();
        for (type_id, type_entries) in entries {
            let type_index = PerTypeVectorIndex::restore_from(dir, type_id, &type_entries)?;
            indexes.insert(TypeID::decode(type_id.to_be_bytes()), Arc::new(type_index));
        }
        drop(indexes);
        Ok(store)
    }
}

impl Default for VectorIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for VectorIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let indexes = self.per_type_indices.read().unwrap();
        let mut map = f.debug_map();
        for (type_id, index) in indexes.iter() {
            map.entry(type_id, &index.get_size());
        }
        map.finish()
    }
}

struct PerTypeVectorIndex {
    /// The hot shard: the single mutable, RAM-resident index. Sealed (immutable, mmap'd)
    /// shards will sit alongside it in the larger-than-memory design; the `Arc` slot
    /// exists so sealing can later freeze the hot index by swapping handles.
    /// Never clone this `Arc` and mutate the index after releasing the guard: saving
    /// (and the future freeze) relies on the lock excluding all in-flight writers.
    hot: RwLock<Arc<Index>>,
    dirty: AtomicBool,
}

impl PerTypeVectorIndex {
    fn new(dimensions: usize) -> Result<Self, VectorIndexError> {
        let options = IndexOptions {
            dimensions,
            // matches HnswMetric::Cosine, the only metric TypeQL accepts in @index(hnsw:...);
            // plumb the annotation's metric through here when a second metric is added
            metric: MetricKind::Cos,
            quantization: ScalarKind::F32,
            connectivity: 0,
            expansion_add: 0,
            expansion_search: 0,
            multi: false,
        };
        let index = usearch::new_index(&options)
            .map_err(|err| VectorIndexError::IndexCreate { dimensions, reason: err.to_string() })?;
        Ok(Self { hot: RwLock::new(Arc::new(index)), dirty: AtomicBool::new(true) })
    }

    fn get_dimensions(&self) -> usize {
        self.hot.read().unwrap().dimensions()
    }

    fn reserve(&self, dimensions: usize, additional: usize) -> Result<(), VectorIndexError> {
        let index = self.hot.write().unwrap();
        if index.dimensions() != dimensions {
            return Err(VectorIndexError::DimensionMismatch {
                index_dimensions: index.dimensions(),
                vector_dimensions: dimensions,
            });
        }
        let required = index.size() + additional;
        if required > index.capacity() {
            index
                .reserve((index.capacity() * 2).max(1024).max(required))
                .map_err(|err| VectorIndexError::IndexGrow { reason: err.to_string() })?;
        }
        Ok(())
    }

    fn add_value(&self, key: u64, vector: &[f32]) -> Result<(), VectorIndexError> {
        let index = self.hot.write().unwrap();
        if index.contains(key) {
            return Ok(());
        }
        if index.dimensions() != vector.len() {
            return Err(VectorIndexError::DimensionMismatch {
                index_dimensions: index.dimensions(),
                vector_dimensions: vector.len(),
            });
        }
        if index.size() >= index.capacity() {
            index
                .reserve((index.capacity() * 2).max(1024))
                .map_err(|err| VectorIndexError::IndexGrow { reason: err.to_string() })?;
        }
        index.add(key, vector).map_err(|err| VectorIndexError::IndexAdd { reason: err.to_string() })?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn get_value(&self, key: u64) -> Option<Vec<f32>> {
        let index = self.hot.read().unwrap();
        let mut buffer = vec![0.0f32; index.dimensions()];
        let found = index.get(key, &mut buffer).unwrap_or_else(|err| panic!("failed to read vector from index: {err}"));
        (found > 0).then_some(buffer)
    }

    fn list_by_ann(&self, query: &[f32], count: usize) -> Vec<(u64, f32)> {
        let index = self.hot.read().unwrap();
        let matches = index.search(query, count).unwrap_or_else(|err| panic!("vector index search failed: {err}"));
        matches.keys.into_iter().zip(matches.distances).collect()
    }

    fn get_size(&self) -> usize {
        self.hot.read().unwrap().size()
    }

    /// Serializes the hot shard to `target` via a staging file, fsyncing file and parent.
    fn write_file(&self, target: &Path) -> io::Result<()> {
        let staging = target.with_extension("tmp");
        // The read guard is held across the whole save: usearch's save() is not safe
        // against concurrent add() (it iterates internal tables without locking), so
        // writers (which take the write lock) must stay excluded for the duration.
        let index = self.hot.read().unwrap();
        index.save(staging.to_str().unwrap()).map_err(|err| {
            self.dirty.store(true, Ordering::Release);
            io::Error::other(format!("failed to save vector index {target:?}: {err}"))
        })?;
        File::open(&staging)?.sync_all()?;
        fs::rename(&staging, target)?;
        File::open(target.parent().unwrap())?.sync_all()?;
        Ok(())
    }

    /// Writes this type's shard files into `checkpoint_dir`, reusing unchanged live
    /// files via hard links, and returns this type's MANIFEST lines.
    fn checkpoint_to(&self, type_id: u16, live_dir: Option<&Path>, checkpoint_dir: &Path) -> io::Result<String> {
        fs::create_dir_all(checkpoint_dir)?;
        let checkpoint_file = checkpoint_dir.join(HOT_FILE_NAME);
        match live_dir {
            Some(live_dir) => {
                fs::create_dir_all(live_dir)?;
                let live_file = live_dir.join(HOT_FILE_NAME);
                if self.dirty.swap(false, Ordering::Acquire) || !live_file.exists() {
                    self.write_file(&live_file)?;
                }
                fs::hard_link(&live_file, &checkpoint_file)
                    .or_else(|_| fs::copy(&live_file, &checkpoint_file).map(|_| ()))?;
            }
            None => self.write_file(&checkpoint_file)?,
        }
        let file_length = fs::metadata(&checkpoint_file)?.len();
        Ok(format!("{} {METRIC_NAME} {} hot {file_length}\n", type_dir_name(type_id), self.get_dimensions()))
    }

    /// Inverse of [`Self::checkpoint_to`]: reconstructs this type's index from its
    /// directory under `dir` and its parsed MANIFEST entries.
    fn restore_from(dir: &Path, type_id: u16, entries: &[ManifestEntry]) -> io::Result<Self> {
        let [entry] = entries else {
            return Err(invalid_data(format!(
                "expected exactly one hot shard for type {}, found {}",
                type_dir_name(type_id),
                entries.len()
            )));
        };
        let path = dir.join(type_dir_name(type_id)).join(HOT_FILE_NAME);
        let actual_length = fs::metadata(&path)?.len();
        if actual_length != entry.file_length {
            return Err(invalid_data(format!(
                "vector index file {path:?} is {actual_length} bytes, manifest says {}",
                entry.file_length
            )));
        }
        let type_index = PerTypeVectorIndex::new(entry.dimensions)
            .map_err(|err| invalid_data(format!("failed to create vector index for {path:?}: {err}")))?;
        type_index
            .hot
            .write()
            .unwrap()
            .load(path.to_str().unwrap())
            .map_err(|err| invalid_data(format!("failed to load vector index {path:?}: {err}")))?;
        Ok(type_index)
    }
}

fn stored_vector_writes(
    operations: &OperationsBuffer,
) -> impl Iterator<Item = (AttributeVertex, &ByteArray<BUFFER_VALUE_INLINE>)> + '_ {
    let keyspace = AttributeVertex::keyspace_for_category(ValueTypeCategory::Vector);
    operations.writes_in_keyspace(keyspace.id()).filter_map(move |(key, write)| {
        let value = match write {
            StorageWrite::Insert { value } => value,
            StorageWrite::Put { value, .. } => value,
            StorageWrite::Delete => return None,
        };
        AttributeVertex::is_vector_attribute_vertex(keyspace.id(), key).then(|| (AttributeVertex::decode(key), value))
    })
}

/// Persistence layout (format version 2):
///
/// ```text
/// <dir>/
/// ├── MANIFEST                       "version 2" + one line per shard file
/// └── 0x0001/                        one directory per attribute type
///     └── hot.usearch                the hot shard's snapshot
/// ```
///
/// MANIFEST grammar — the COMPLETE grammar is fixed here even though this version only
/// ever emits `hot` lines, so that the future multi-shard version reads today's data
/// with no migration (a hot-only manifest is simply "a database that never sealed"):
///
/// ```text
/// version 2
/// <type_id_hex> <metric> <dimensions> hot <file_length>
/// <type_id_hex> <metric> <dimensions> sealed <shard_number> <file_length>   (reserved)
/// ```
///
/// Parsing rules: an unknown shard KIND is a hard error (that data would be unservable;
/// skipping would silently lose vectors); unknown TRAILING columns beyond a kind's known
/// arity are ignored (future annotations without a format version bump). The version is
/// bumped only to reinterpret existing bytes, never to add kinds or trailing columns.
const MANIFEST_FILE_NAME: &str = "MANIFEST";

const MANIFEST_VERSION_LINE: &str = "version 2";

const METRIC_NAME: &str = "cosine";

const HOT_FILE_NAME: &str = "hot.usearch";

fn type_dir_name(type_id: u16) -> String {
    format!("{type_id:#06x}")
}

fn invalid_data(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// A parsed MANIFEST line for one shard file. This version only ever holds `hot`
/// entries: `sealed` lines are rejected at parse time.
struct ManifestEntry {
    dimensions: usize,
    file_length: u64,
}

fn parse_manifest_line(line: &str) -> io::Result<(u16, ManifestEntry)> {
    let malformed = |what: &str| invalid_data(format!("malformed {what} in vector store manifest line: '{line}'"));
    let parts: Vec<&str> = line.split_whitespace().collect();
    let &[type_id, metric, dimensions, kind, ref rest @ ..] = parts.as_slice() else {
        return Err(malformed("line"));
    };
    let type_id = type_id
        .strip_prefix("0x")
        .and_then(|hex| u16::from_str_radix(hex, 16).ok())
        .ok_or_else(|| malformed("type id"))?;
    if metric != METRIC_NAME {
        return Err(invalid_data(format!("unsupported vector index metric: '{metric}'")));
    }
    let dimensions: usize = dimensions.parse().map_err(|_| malformed("dimensions"))?;
    match kind {
        // Trailing columns beyond the known arity are ignored for forward compatibility.
        "hot" => {
            let file_length: u64 =
                rest.first().ok_or_else(|| malformed("line"))?.parse().map_err(|_| malformed("file length"))?;
            Ok((type_id, ManifestEntry { dimensions, file_length }))
        }
        "sealed" => Err(invalid_data(format!(
            "vector store manifest contains a sealed shard, which this version does not support: '{line}'"
        ))),
        _ => Err(invalid_data(format!("unknown shard kind '{kind}' in vector store manifest line: '{line}'"))),
    }
}

impl fmt::Display for VectorIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self as &dyn TypeDBError, f)
    }
}

impl Error for VectorIndexError {}

typedb_error! {
    pub VectorIndexError(component = "Vector store", prefix = "VEC") {
        IndexCreate(1, "Failed to create vector index with {dimensions} dimensions: {reason}", dimensions: usize, reason: String),
        IndexGrow(2, "Failed to grow vector index: {reason}", reason: String),
        IndexAdd(3, "Failed to add vector to index: {reason}", reason: String),
        DimensionMismatch(4, "Vector dimension mismatch: index has {index_dimensions} dimensions, vector has {vector_dimensions}.", index_dimensions: usize, vector_dimensions: usize),
        ConflictingDimensionsInCommit(5, "Conflicting vector dimensions ({first} and {second}) for one attribute type in a single commit.", first: usize, second: usize),
        CommittedWithoutValue(6, "Vector attribute committed without a value: {vertex}.", vertex: String),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn vertex(type_id: u16, key: u64) -> AttributeVertex {
        use encoding::graph::thing::vertex_attribute::AttributeID;
        AttributeVertex::new(
            TypeID::decode(type_id.to_be_bytes()),
            AttributeID::Vector(VectorAttributeID::from_type_vector_index_key(key)),
        )
    }

    fn hot_file(dir: &Path, type_id: u16) -> PathBuf {
        dir.join(type_dir_name(type_id)).join(HOT_FILE_NAME)
    }

    #[test]
    fn add_get_search_roundtrip() {
        let store = VectorIndex::new();
        store.add_instance(vertex(1, 100), &[1.0, 0.0, 0.0]).unwrap();
        store.add_instance(vertex(1, 200), &[0.0, 1.0, 0.0]).unwrap();
        store.add_instance(vertex(1, 100), &[1.0, 0.0, 0.0]).unwrap();

        assert_eq!(store.get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(store.get_value_by_attr_vertex(vertex(1, 300)), None);
        assert_eq!(store.get_value_by_attr_vertex(vertex(2, 100)), None);
        assert_eq!(store.get_count_for_type(TypeID::decode(1u16.to_be_bytes())), 2);

        let results = store.list_instances_by_ann(TypeID::decode(1u16.to_be_bytes()), &[0.9, 0.1, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, vertex(1, 100));
    }

    #[test]
    fn checkpoint_roundtrip() {
        let store = VectorIndex::new();
        store.add_instance(vertex(1, 100), &[1.0, 0.0]).unwrap();
        store.add_instance(vertex(7, 200), &[0.0, 1.0, 0.0, 0.0]).unwrap();

        let root = std::env::temp_dir().join(format!("vector_index_checkpoint_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let dir = root.join("extension");
        store.save_to_path(&dir).unwrap();

        assert!(hot_file(&dir, 1).exists());
        assert!(hot_file(&dir, 7).exists());
        assert!(fs::read_to_string(dir.join(MANIFEST_FILE_NAME)).unwrap().starts_with(MANIFEST_VERSION_LINE));

        let loaded = VectorIndex::load_from_path(&dir).unwrap();
        assert_eq!(loaded.get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));
        assert_eq!(loaded.get_value_by_attr_vertex(vertex(7, 200)), Some(vec![0.0, 1.0, 0.0, 0.0]));

        let index_file = hot_file(&dir, 1);
        let bytes = fs::read(&index_file).unwrap();
        fs::write(&index_file, &bytes[..bytes.len() - 1]).unwrap();
        assert!(VectorIndex::load_from_path(&dir).is_err());

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn checkpoint_save_reuses_live_files_for_clean_indexes_and_rewrites_dirty_ones() {
        let store = VectorIndex::new();
        store.add_instance(vertex(1, 100), &[1.0, 0.0]).unwrap();
        store.add_instance(vertex(7, 200), &[0.0, 1.0]).unwrap();

        let root = std::env::temp_dir().join(format!("vector_index_dirty_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        store.set_live_directory(root.join("live"));

        let (first, second) = (root.join("first"), root.join("second"));
        store.save_to_path(&first).unwrap();
        assert!(hot_file(&root.join("live"), 1).exists());

        store.add_instance(vertex(7, 300), &[0.0, -1.0]).unwrap();
        store.save_to_path(&second).unwrap();

        let first_clean = fs::read(hot_file(&first, 1)).unwrap();
        assert_eq!(fs::read(hot_file(&second, 1)).unwrap(), first_clean);

        let loaded = VectorIndex::load_from_path(&second).unwrap();
        assert_eq!(loaded.get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));
        assert_eq!(loaded.get_value_by_attr_vertex(vertex(7, 200)), Some(vec![0.0, 1.0]));
        assert_eq!(loaded.get_value_by_attr_vertex(vertex(7, 300)), Some(vec![0.0, -1.0]));

        let third = root.join("third");
        loaded.set_live_directory(root.join("live"));
        loaded.save_to_path(&third).unwrap();
        assert_eq!(fs::read(hot_file(&third, 7)).unwrap(), fs::read(hot_file(&second, 7)).unwrap());

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn empty_store_checkpoint_roundtrip() {
        let root = std::env::temp_dir().join(format!("vector_index_empty_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let dir = root.join("extension");

        VectorIndex::new().save_to_path(&dir).unwrap();
        assert_eq!(fs::read_to_string(dir.join(MANIFEST_FILE_NAME)).unwrap(), format!("{MANIFEST_VERSION_LINE}\n"));

        let loaded = VectorIndex::load_from_path(&dir).unwrap();
        assert!(loaded.per_type_indices.read().unwrap().is_empty());

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn manifest_forward_compatibility_rules() {
        let store = VectorIndex::new();
        store.add_instance(vertex(1, 100), &[1.0, 0.0]).unwrap();

        let root = std::env::temp_dir().join(format!("vector_index_compat_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let dir = root.join("extension");
        store.save_to_path(&dir).unwrap();
        let manifest_path = dir.join(MANIFEST_FILE_NAME);
        let manifest = fs::read_to_string(&manifest_path).unwrap();

        // Unknown trailing columns on a known kind are ignored (forward compatibility).
        fs::write(&manifest_path, format!("{} future-annotation\n", manifest.trim_end())).unwrap();
        assert_eq!(
            VectorIndex::load_from_path(&dir).unwrap().get_value_by_attr_vertex(vertex(1, 100)),
            Some(vec![1.0, 0.0])
        );

        // A sealed line (reserved for the multi-shard version) is a hard, named error.
        fs::write(&manifest_path, format!("{manifest}0x0001 cosine 2 sealed 0001 1024\n")).unwrap();
        let error = VectorIndex::load_from_path(&dir).unwrap_err();
        assert!(error.to_string().contains("sealed"));

        // An unknown shard kind is a hard error, never silently skipped.
        fs::write(&manifest_path, format!("{manifest}0x0001 cosine 2 warm 1024\n")).unwrap();
        assert!(VectorIndex::load_from_path(&dir).is_err());

        fs::remove_dir_all(&root).unwrap();
    }
}
