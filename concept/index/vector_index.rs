/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt, fs,
    fs::File,
    io,
    io::Write,
    mem,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

use bytes::{Bytes, byte_array::ByteArray};
use encoding::{
    Keyable,
    graph::{
        Typed,
        thing::vertex_attribute::{AttributeID, AttributeVertex, VectorAttributeID},
        type_::vertex::{PrefixedTypeVertexEncoding, TypeID, TypeVertexEncoding},
    },
    value::{
        value_type::{ValueType, ValueTypeCategory, VectorPrecision},
        vector_bytes::VectorBytes,
    },
};
use error::{TypeDBError, typedb_error};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    SecondaryStore,
    keyspace::{KeyspaceId, KeyspaceSet},
    recovery::checkpoint::{CheckpointAdditionalData, CheckpointLoadError, CheckpointReader},
    sequence_number::SequenceNumber,
    snapshot::{buffer::OperationsBuffer, write::Write as StorageWrite},
};

use super::per_type_vector_index::{PerTypeVectorIndex, VectorShape, invalid_data, parse_type_dir_name, type_dir_name};
use crate::{
    thing::{ThingAPI, attribute::Attribute},
    type_::{attribute_type::AttributeType, type_manager::type_cache::TypeCache},
};

pub struct VectorIndex {
    latest_version: RwLock<Arc<VectorIndexVersion>>,
    open_views: Arc<Mutex<BTreeMap<SequenceNumber, usize>>>,
    watermark_at_previous_purge: Mutex<SequenceNumber>,
    live_directory: Option<PathBuf>,
}

#[derive(Default)]
pub struct VectorIndexView {
    version: Arc<VectorIndexVersion>,
    _registration: Option<ViewRegistration>,
}

impl Deref for VectorIndexView {
    type Target = VectorIndexVersion;

    fn deref(&self) -> &Self::Target {
        &self.version
    }
}

impl From<Arc<VectorIndexVersion>> for VectorIndexView {
    fn from(version: Arc<VectorIndexVersion>) -> Self {
        Self { version, _registration: None }
    }
}

struct ViewRegistration {
    opened_at: SequenceNumber,
    open_views: Arc<Mutex<BTreeMap<SequenceNumber, usize>>>,
}

impl Drop for ViewRegistration {
    fn drop(&mut self) {
        let mut open_views = self.open_views.lock().unwrap();
        if let Some(count) = open_views.get_mut(&self.opened_at) {
            *count -= 1;
            if *count == 0 {
                open_views.remove(&self.opened_at);
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct VectorIndexVersion {
    per_type_indices: HashMap<TypeID, Arc<PerTypeVectorIndex>>,
}

impl VectorIndexVersion {
    pub fn get_count_for_type(&self, type_id: TypeID) -> usize {
        self.per_type_indices.get(&type_id).map_or(0, |index| index.get_count())
    }

    pub fn search_attributes_by_approximate_nearest_neighbour(
        &self,
        type_id: TypeID,
        reference_point: &[f32],
        count: usize,
    ) -> Vec<(Attribute, f32)> {
        let Some(index) = self.per_type_indices.get(&type_id) else { return Vec::new() };
        index
            .search_by_approximate_nearest_neighbour(reference_point, count)
            .into_iter()
            .map(|(key, distance)| {
                let id = AttributeID::Vector(VectorAttributeID::from_type_vector_index_key(key));
                (Attribute::new(AttributeVertex::new(type_id, id)), distance)
            })
            .collect()
    }

    pub fn get_value_by_attr_vertex(&self, vertex: AttributeVertex) -> Option<Vec<f32>> {
        self.get_value_by_key(vertex.type_id_(), vertex.attribute_id().unwrap_vector().as_type_vector_index_key())
    }

    pub fn get_value_by_key(&self, type_id: TypeID, key: u64) -> Option<Vec<f32>> {
        self.per_type_indices.get(&type_id)?.get_value(key)
    }

    pub fn get_deleted_attributes(&self, type_id: TypeID) -> Vec<Attribute> {
        let Some(index) = self.per_type_indices.get(&type_id) else { return Vec::new() };
        index
            .get_deleted_keys()
            .into_iter()
            .map(|key| {
                let id = AttributeID::Vector(VectorAttributeID::from_type_vector_index_key(key));
                Attribute::new(AttributeVertex::new(type_id, id))
            })
            .collect()
    }
}

impl VectorIndex {
    pub fn create(live_directory: PathBuf) -> Self {
        Self { live_directory: Some(live_directory), ..Self::new() }
    }

    pub fn load(checkpoint: Option<&CheckpointReader>, live_directory: PathBuf) -> Result<Self, CheckpointLoadError> {
        let index = match checkpoint {
            Some(reader) => match reader.get_additional_data::<Self>() {
                Ok(index) => index,
                Err(CheckpointLoadError::AdditionalDataNotFound { .. }) => Self::new(),
                Err(err) => return Err(err),
            },
            None => Self::new(),
        };
        Ok(Self { live_directory: Some(live_directory), ..index })
    }

    fn load_checkpoint_dir(dir: &Path) -> io::Result<Self> {
        check_format_version(dir)?;
        let mut per_type_indices = HashMap::new();
        for entry in fs::read_dir(dir)? {
            let Some(type_id) = entry?.file_name().to_str().and_then(parse_type_dir_name) else { continue };
            let type_index = PerTypeVectorIndex::load(dir, type_id)?;
            per_type_indices.insert(TypeID::decode(type_id.to_be_bytes()), Arc::new(type_index));
        }
        Ok(Self { latest_version: RwLock::new(Arc::new(VectorIndexVersion { per_type_indices })), ..Self::new() })
    }

    pub fn new() -> Self {
        Self {
            latest_version: RwLock::default(),
            open_views: Arc::default(),
            watermark_at_previous_purge: Mutex::new(SequenceNumber::MIN),
            live_directory: None,
        }
    }

    pub fn latest_version(&self) -> Arc<VectorIndexVersion> {
        self.latest_version.read().unwrap().clone()
    }

    pub fn open_view(&self, opened_at: SequenceNumber) -> VectorIndexView {
        *self.open_views.lock().unwrap().entry(opened_at).or_default() += 1;
        let registration = ViewRegistration { opened_at, open_views: self.open_views.clone() };
        VectorIndexView { version: self.latest_version(), _registration: Some(registration) }
    }

    fn get_or_create_index_for_type(
        &self,
        type_id: TypeID,
        dimensions: usize,
    ) -> Result<Arc<PerTypeVectorIndex>, VectorIndexError> {
        match self.get_index_for_type(type_id) {
            Some(index) if index.is_reusable_for(dimensions) => Ok(index),
            _ => self.create_index_for_type(type_id, dimensions),
        }
    }

    fn get_index_for_type(&self, type_id: TypeID) -> Option<Arc<PerTypeVectorIndex>> {
        self.latest_version.read().unwrap().per_type_indices.get(&type_id).cloned()
    }

    fn create_index_for_type(
        &self,
        type_id: TypeID,
        dimensions: usize,
    ) -> Result<Arc<PerTypeVectorIndex>, VectorIndexError> {
        let mut latest_version = self.latest_version.write().unwrap();
        if let Some(existing) = latest_version.per_type_indices.get(&type_id) {
            if existing.is_reusable_for(dimensions) {
                return Ok(existing.clone());
            }
        }
        let shape = VectorShape { dimensions, precision: VectorPrecision::Float32 };
        let index = Arc::new(PerTypeVectorIndex::create(shape)?);
        let mut updated = VectorIndexVersion::clone(&latest_version);
        updated.per_type_indices.insert(type_id, index.clone());
        *latest_version = Arc::new(updated);
        Ok(index)
    }

    fn replace_index_for_type(
        &self,
        type_id: TypeID,
        dimensions: usize,
    ) -> Result<Arc<PerTypeVectorIndex>, VectorIndexError> {
        let shape = VectorShape { dimensions, precision: VectorPrecision::Float32 };
        let index = Arc::new(PerTypeVectorIndex::create(shape)?);
        let mut latest_version = self.latest_version.write().unwrap();
        let mut updated = VectorIndexVersion::clone(&latest_version);
        updated.per_type_indices.insert(type_id, index.clone());
        *latest_version = Arc::new(updated);
        Ok(index)
    }

    fn apply_commit_creating_type(
        &self,
        type_id: TypeID,
        sequence_number: SequenceNumber,
        inserts: &[(u64, Vec<f32>)],
        deletes: &[u64],
    ) -> Result<(), VectorIndexError> {
        if let Some(previous) = self.get_index_for_type(type_id) {
            previous.apply_commit(sequence_number, &[], deletes)?;
        }
        match inserts.first() {
            Some((_, vector)) => {
                self.replace_index_for_type(type_id, vector.len())?.apply_commit(sequence_number, inserts, &[])
            }
            None => Ok(()),
        }
    }

    fn apply_commit(
        &self,
        type_id: TypeID,
        sequence_number: SequenceNumber,
        inserts: &[(u64, Vec<f32>)],
        deletes: &[u64],
    ) -> Result<(), VectorIndexError> {
        let index = match inserts.first() {
            Some((_, vector)) => self.get_or_create_index_for_type(type_id, vector.len())?,
            None => match self.get_index_for_type(type_id) {
                Some(index) => index,
                None => return Ok(()),
            },
        };
        index.apply_commit(sequence_number, inserts, deletes)
    }

    pub fn purge_deleted(&self, watermark: SequenceNumber) {
        let settled = mem::replace(&mut *self.watermark_at_previous_purge.lock().unwrap(), watermark);
        let earliest_open_view = self.open_views.lock().unwrap().keys().next().copied();
        let bound = earliest_open_view.map_or(settled, |opened_at| opened_at.min(settled));
        for index in self.latest_version().per_type_indices.values() {
            index.purge_deleted(bound);
        }
    }

    pub fn retain_live_types(&self, type_cache: &TypeCache) {
        let live: HashMap<TypeID, VectorShape> = type_cache
            .get_attribute_types_with_value_types()
            .filter_map(|(attribute_type, value_type)| match value_type {
                ValueType::Vector(parameters) => {
                    let type_id = attribute_type.vertex().type_id_();
                    let shape = VectorShape { dimensions: parameters.length as usize, precision: parameters.precision };
                    Some((type_id, shape))
                }
                _ => None,
            })
            .collect();
        self.retain_types(&live);
    }

    fn retain_types(&self, live: &HashMap<TypeID, VectorShape>) {
        let mut latest_version = self.latest_version.write().unwrap();
        let mut updated = VectorIndexVersion::clone(&latest_version);
        updated.per_type_indices.retain(|type_id, index| live.get(type_id) == Some(&index.get_shape()));
        *latest_version = Arc::new(updated);
    }

    pub fn clear(&self) {
        *self.latest_version.write().unwrap() = Arc::default();
        *self.watermark_at_previous_purge.lock().unwrap() = SequenceNumber::MIN;
    }
}

impl SecondaryStore for VectorIndex {
    fn stores_value(&self, keyspace_id: KeyspaceId, key: &[u8]) -> bool {
        AttributeVertex::is_vector_attribute_vertex(keyspace_id, key)
    }

    fn validate_commit(&self, operations: &OperationsBuffer) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut pending: HashMap<TypeID, (usize, usize)> = HashMap::new();
        for (vertex, value) in stored_vector_writes(operations) {
            let Some(value) = value else { continue }; // deletes need no validation or capacity
            if value.is_empty() {
                return Err(Box::new(VectorIndexError::CommittedWithoutValue { vertex: format!("{vertex:?}") }));
            }
            let dimensions = VectorBytes::<BUFFER_VALUE_INLINE>::new(Bytes::Reference(value)).dimensions();
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
            if !creates_type(operations, type_id) {
                self.get_or_create_index_for_type(type_id, dimensions)?.reserve(dimensions, count)?;
            }
        }
        Ok(())
    }

    fn apply_commit(
        &self,
        sequence_number: SequenceNumber,
        operations: &OperationsBuffer,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut writes_by_type: HashMap<TypeID, (Vec<(u64, Vec<f32>)>, Vec<u64>)> = HashMap::new();
        for (vertex, value) in stored_vector_writes(operations) {
            let (inserts, deletes) = writes_by_type.entry(vertex.type_id_()).or_default();
            match value {
                Some(value) => inserts.push((
                    index_key(vertex),
                    VectorBytes::new(Bytes::<BUFFER_VALUE_INLINE>::Reference(value)).as_vector(),
                )),
                None => deletes.push(index_key(vertex)),
            }
        }
        for (type_id, (inserts, deletes)) in writes_by_type {
            if creates_type(operations, type_id) {
                self.apply_commit_creating_type(type_id, sequence_number, &inserts, &deletes)?;
            } else {
                self.apply_commit(type_id, sequence_number, &inserts, &deletes)?;
            }
        }
        Ok(())
    }
}

fn creates_type(operations: &OperationsBuffer, type_id: TypeID) -> bool {
    let type_key = AttributeType::build_from_type_id(type_id).vertex().into_storage_key();
    let write = operations.writes_in(type_key.keyspace_id()).writes().get(type_key.bytes());
    matches!(write, Some(StorageWrite::Insert { .. } | StorageWrite::Put { .. }))
}

fn index_key(vertex: AttributeVertex) -> u64 {
    vertex.attribute_id().unwrap_vector().as_type_vector_index_key()
}

fn stored_vector_writes(
    operations: &OperationsBuffer,
) -> impl Iterator<Item = (AttributeVertex, Option<&ByteArray<BUFFER_VALUE_INLINE>>)> + '_ {
    let keyspace = AttributeVertex::keyspace_for_category(ValueTypeCategory::Vector);
    operations.writes_in(keyspace.id()).writes().iter().filter_map(move |(key, write)| {
        let key: &[u8] = key;
        let value = match write {
            StorageWrite::Insert { value } => Some(value),
            StorageWrite::Put { value, .. } => Some(value),
            StorageWrite::Delete { .. } => None,
        };
        AttributeVertex::is_vector_attribute_vertex(keyspace.id(), key).then(|| (AttributeVertex::decode(key), value))
    })
}

impl CheckpointAdditionalData for VectorIndex {
    const NAME: &'static str = "VECTOR_INDEX";

    fn save_to_path(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)?;
        let version = self.latest_version();
        for (type_id, type_index) in &version.per_type_indices {
            let dir_name = type_dir_name(u16::from_be_bytes(type_id.to_bytes()));
            let live_type_dir = self.live_directory.as_ref().map(|live| live.join(&dir_name));
            type_index.checkpoint_to(live_type_dir.as_deref(), &path.join(&dir_name))?;
        }
        write_format_version(path)
    }

    fn load_from_path(dir: &Path) -> io::Result<Self> {
        Self::load_checkpoint_dir(dir)
    }
}

const FORMAT_VERSION_FILE_NAME: &str = "VERSION";

/// Bumped whenever the checkpoint layout changes, so an older TypeDB refuses a newer checkpoint.
const FORMAT_VERSION: u32 = 1;

fn write_format_version(dir: &Path) -> io::Result<()> {
    let mut file = File::create(dir.join(FORMAT_VERSION_FILE_NAME))?;
    file.write_all(format!("{FORMAT_VERSION}\n").as_bytes())?;
    file.sync_all()?;
    durability::sync_directory(dir)
}

fn check_format_version(dir: &Path) -> io::Result<()> {
    let version = fs::read_to_string(dir.join(FORMAT_VERSION_FILE_NAME))?;
    match version.trim().parse::<u32>() {
        Ok(FORMAT_VERSION) => Ok(()),
        _ => Err(invalid_data(format!("unsupported vector index format version: '{}'", version.trim()))),
    }
}

impl Error for VectorIndexError {}

impl Default for VectorIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for VectorIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.latest_version(), f)
    }
}

impl fmt::Debug for VectorIndexView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.version, f)
    }
}

impl fmt::Debug for VectorIndexVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for (type_id, index) in &self.per_type_indices {
            map.entry(type_id, &index.get_count());
        }
        map.finish()
    }
}

impl fmt::Display for VectorIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self as &dyn TypeDBError, f)
    }
}

typedb_error! {
    pub VectorIndexError(component = "Vector index", prefix = "VEC") {
        IndexCreate(1, "Failed to create vector index with {dimensions} dimensions: {reason}", dimensions: usize, reason: String),
        IndexGrow(2, "Failed to grow vector index: {reason}", reason: String),
        IndexInsert(3, "Failed to insert vector into index: {reason}", reason: String),
        DimensionMismatch(4, "Vector dimension mismatch: index has {index_dimensions} dimensions, vector has {vector_dimensions}.", index_dimensions: usize, vector_dimensions: usize),
        ConflictingDimensionsInCommit(5, "Conflicting vector dimensions ({first} and {second}) for one attribute type in a single commit.", first: usize, second: usize),
        CommittedWithoutValue(6, "Vector attribute committed without a value: {vertex}.", vertex: String),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::index::per_type_vector_index::HOT_SHARD_FILE_NAME;

    fn type_id(id: u16) -> TypeID {
        TypeID::decode(id.to_be_bytes())
    }

    fn vertex(type_id: u16, key: u64) -> AttributeVertex {
        use encoding::graph::thing::vertex_attribute::AttributeID;
        AttributeVertex::new(
            TypeID::decode(type_id.to_be_bytes()),
            AttributeID::Vector(VectorAttributeID::from_type_vector_index_key(key)),
        )
    }

    fn hot_file(dir: &Path, type_id: u16) -> PathBuf {
        dir.join(type_dir_name(type_id)).join(HOT_SHARD_FILE_NAME)
    }

    fn live_types(types: &[(u16, usize)]) -> HashMap<TypeID, VectorShape> {
        types
            .iter()
            .map(|&(id, dimensions)| (type_id(id), VectorShape { dimensions, precision: VectorPrecision::Float32 }))
            .collect()
    }

    fn has_live_index(store: &VectorIndex, type_id: TypeID) -> bool {
        store.latest_version().per_type_indices.contains_key(&type_id)
    }

    #[test]
    fn deleting_a_type_keeps_its_index_in_earlier_versions() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        store.apply_commit(type_id(7), SequenceNumber::MIN, &[(200, vec![0.0, 1.0])], &[]).unwrap();
        let earlier = store.latest_version();

        store.retain_types(&live_types(&[(7, 2)]));
        assert!(!has_live_index(&store, type_id(1)));
        assert!(has_live_index(&store, type_id(7)));
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 100)), None);
        assert_eq!(earlier.get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));
        assert_eq!(earlier.get_count_for_type(type_id(1)), 1);
        assert_eq!(earlier.search_attributes_by_approximate_nearest_neighbour(type_id(1), &[1.0, 0.0], 1).len(), 1);
    }

    #[test]
    fn deleting_a_nonexistent_type_is_a_no_op() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();

        store.retain_types(&live_types(&[(1, 2)]));
        assert!(has_live_index(&store, type_id(1)));
        assert_eq!(store.latest_version().get_count_for_type(type_id(9)), 0);
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));
    }

    #[test]
    fn reusing_a_type_id_leaves_earlier_versions_on_the_old_index() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        let earlier = store.latest_version();

        store.retain_types(&live_types(&[(1, 3)]));
        assert!(!has_live_index(&store, type_id(1)));

        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(200, vec![0.0, 1.0, 0.0])], &[]).unwrap();
        let current = store.latest_version();
        assert_eq!(current.get_count_for_type(type_id(1)), 1);
        assert_eq!(current.get_value_by_attr_vertex(vertex(1, 200)), Some(vec![0.0, 1.0, 0.0]));
        assert_eq!(current.get_value_by_attr_vertex(vertex(1, 100)), None);
        assert_eq!(earlier.get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));
        assert_eq!(earlier.get_value_by_attr_vertex(vertex(1, 200)), None);
    }

    #[test]
    fn a_commit_that_creates_a_type_starts_a_new_index() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        let earlier = store.latest_version();

        let redefined = [(200, vec![0.0, 1.0, 0.0])];
        store.apply_commit_creating_type(type_id(1), SequenceNumber::new(5), &redefined, &[100]).unwrap();

        let current = store.latest_version();
        assert_eq!(current.get_count_for_type(type_id(1)), 1);
        assert_eq!(current.get_value_by_attr_vertex(vertex(1, 200)), Some(vec![0.0, 1.0, 0.0]));
        assert_eq!(current.get_value_by_attr_vertex(vertex(1, 100)), None);
        assert!(current.get_deleted_attributes(type_id(1)).is_empty());

        assert_eq!(earlier.get_count_for_type(type_id(1)), 0);
        assert_eq!(earlier.get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));
        assert_eq!(earlier.get_deleted_attributes(type_id(1)).len(), 1);
        assert_eq!(earlier.get_value_by_attr_vertex(vertex(1, 200)), None);
    }

    #[test]
    fn a_commit_that_creates_a_type_without_vectors_keeps_the_index() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();

        store.apply_commit_creating_type(type_id(1), SequenceNumber::new(5), &[], &[100]).unwrap();
        assert!(has_live_index(&store, type_id(1)));
        assert_eq!(store.latest_version().get_count_for_type(type_id(1)), 0);
        assert_eq!(store.latest_version().get_deleted_attributes(type_id(1)).len(), 1);
    }

    #[test]
    fn empty_stale_index_is_recreated_on_dimension_change() {
        let type_id = type_id(1);
        let store = VectorIndex::new();
        store.apply_commit(type_id, SequenceNumber::MIN, &[(100, vec![1.0, 0.0, 0.0])], &[]).unwrap();

        assert!(store.apply_commit(type_id, SequenceNumber::MIN, &[(200, vec![1.0, 0.0])], &[]).is_err());

        store.apply_commit(type_id, SequenceNumber::MIN, &[], &[100]).unwrap();
        store.apply_commit(type_id, SequenceNumber::MIN, &[(200, vec![1.0, 0.0])], &[]).unwrap();
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 200)), Some(vec![1.0, 0.0]));
        assert_eq!(store.latest_version().get_count_for_type(type_id), 1);
    }

    #[test]
    fn insert_then_get_and_search() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0, 0.0])], &[]).unwrap();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(200, vec![0.0, 1.0, 0.0])], &[]).unwrap();

        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 300)), None);
        assert_eq!(store.latest_version().get_count_for_type(type_id(1)), 2);

        let results =
            store.latest_version().search_attributes_by_approximate_nearest_neighbour(type_id(1), &[0.9, 0.1, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.vertex(), vertex(1, 100));
    }

    #[test]
    fn insert_into_a_nonexistent_type_creates_its_index() {
        let store = VectorIndex::new();
        assert!(!has_live_index(&store, type_id(1)));
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 100)), None);
        assert!(
            store
                .latest_version()
                .search_attributes_by_approximate_nearest_neighbour(type_id(1), &[1.0, 0.0], 1)
                .is_empty()
        );

        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        assert!(has_live_index(&store, type_id(1)));
        assert_eq!(store.latest_version().get_count_for_type(type_id(1)), 1);
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(2, 100)), None, "other types are untouched");
    }

    #[test]
    fn insert_of_an_existing_key_is_a_no_op() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        assert_eq!(store.latest_version().get_count_for_type(type_id(1)), 1);
    }

    #[test]
    fn insert_with_mismatched_dimensions_is_rejected() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0, 0.0])], &[]).unwrap();
        assert!(store.apply_commit(type_id(1), SequenceNumber::MIN, &[(200, vec![1.0, 0.0])], &[]).is_err());
        assert_eq!(store.latest_version().get_count_for_type(type_id(1)), 1);
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 200)), None);
    }

    #[test]
    fn delete_drops_from_search_but_retains_value_until_purged() {
        let type_id = type_id(1);
        let store = VectorIndex::new();
        store.apply_commit(type_id, SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        store.apply_commit(type_id, SequenceNumber::MIN, &[(200, vec![0.0, 1.0])], &[]).unwrap();

        store.apply_commit(type_id, SequenceNumber::new(5), &[], &[100]).unwrap();
        assert_eq!(
            store.latest_version().get_count_for_type(type_id),
            1,
            "removed vectors must leave the searchable index"
        );
        let results =
            store.latest_version().search_attributes_by_approximate_nearest_neighbour(type_id, &[1.0, 0.0], 2);
        assert!(results.iter().all(|(attribute, _)| attribute.vertex() != vertex(1, 100)));
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));
        let deleted = store.latest_version().get_deleted_attributes(type_id);
        assert_eq!(deleted.iter().map(|attribute| attribute.vertex()).collect::<Vec<_>>(), vec![vertex(1, 100)]);

        let root = std::env::temp_dir().join(format!("vector_index_remove_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        store.save_to_path(&root.join("extension")).unwrap();
        fs::remove_dir_all(&root).unwrap();
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));

        let earlier = store.open_view(SequenceNumber::new(4));
        store.purge_deleted(SequenceNumber::new(5));
        store.purge_deleted(SequenceNumber::new(5));
        assert_eq!(
            earlier.get_value_by_attr_vertex(vertex(1, 100)),
            Some(vec![1.0, 0.0]),
            "a transaction opened before the delete can still read it"
        );

        drop(earlier);
        store.purge_deleted(SequenceNumber::new(5));
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 100)), None);
        assert!(store.latest_version().get_deleted_attributes(type_id).is_empty());
    }

    #[test]
    fn purge_drops_a_deleted_value_one_call_after_the_watermark_passes_it() {
        let type_id = type_id(1);
        let store = VectorIndex::new();
        store.apply_commit(type_id, SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        store.apply_commit(type_id, SequenceNumber::new(5), &[], &[100]).unwrap();

        store.purge_deleted(SequenceNumber::new(5));
        assert_eq!(
            store.latest_version().get_value_by_attr_vertex(vertex(1, 100)),
            Some(vec![1.0, 0.0]),
            "a transaction that opened just before this purge may not be registered yet"
        );

        store.purge_deleted(SequenceNumber::new(5));
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 100)), None);
    }

    #[test]
    fn views_opened_at_the_same_point_are_counted_separately() {
        let store = VectorIndex::new();
        let first = store.open_view(SequenceNumber::new(4));
        let second = store.open_view(SequenceNumber::new(4));
        drop(first);
        assert_eq!(store.open_views.lock().unwrap().get(&SequenceNumber::new(4)), Some(&1));
        drop(second);
        assert!(store.open_views.lock().unwrap().is_empty());
    }

    #[test]
    fn insert_of_a_deleted_key_makes_it_searchable_again() {
        let type_id = type_id(1);
        let store = VectorIndex::new();
        store.apply_commit(type_id, SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        store.apply_commit(type_id, SequenceNumber::new(5), &[], &[100]).unwrap();

        store.apply_commit(type_id, SequenceNumber::new(6), &[(100, vec![1.0, 0.0])], &[]).unwrap();
        assert_eq!(store.latest_version().get_count_for_type(type_id), 1);
        assert!(store.latest_version().get_deleted_attributes(type_id).is_empty());
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));
    }

    #[test]
    fn delete_of_a_nonexistent_key_is_a_no_op() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[], &[300]).unwrap();
        assert_eq!(store.latest_version().get_count_for_type(type_id(1)), 1);
        assert_eq!(store.latest_version().get_value_by_attr_vertex(vertex(1, 300)), None);
    }

    #[test]
    fn delete_from_a_nonexistent_type_is_a_no_op() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(9), SequenceNumber::MIN, &[], &[100]).unwrap();
        assert!(!has_live_index(&store, type_id(9)), "deletes must not create an index");
        assert_eq!(store.latest_version().get_count_for_type(type_id(9)), 0);
    }

    #[test]
    fn checkpoint_roundtrip() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        store.apply_commit(type_id(7), SequenceNumber::MIN, &[(200, vec![0.0, 1.0, 0.0, 0.0])], &[]).unwrap();

        let root = std::env::temp_dir().join(format!("vector_index_checkpoint_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let dir = root.join("extension");
        store.save_to_path(&dir).unwrap();

        assert!(hot_file(&dir, 1).exists());
        assert!(hot_file(&dir, 7).exists());
        assert_eq!(fs::read_to_string(dir.join(FORMAT_VERSION_FILE_NAME)).unwrap(), format!("{FORMAT_VERSION}\n"));

        let loaded = VectorIndex::load_from_path(&dir).unwrap();
        assert_eq!(loaded.latest_version().get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));
        assert_eq!(loaded.latest_version().get_value_by_attr_vertex(vertex(7, 200)), Some(vec![0.0, 1.0, 0.0, 0.0]));
        assert_eq!(loaded.latest_version().get_count_for_type(type_id(1)), 1);
        assert_eq!(loaded.latest_version().get_count_for_type(type_id(7)), 1);
        let results = loaded.latest_version().search_attributes_by_approximate_nearest_neighbour(
            type_id(7),
            &[0.0, 1.0, 0.0, 0.0],
            1,
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.vertex(), vertex(7, 200));

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn checkpoint_save_reuses_live_files_for_clean_indexes_and_rewrites_dirty_ones() {
        let root = std::env::temp_dir().join(format!("vector_index_dirty_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let store = VectorIndex::create(root.join("live"));
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        store.apply_commit(type_id(7), SequenceNumber::MIN, &[(200, vec![0.0, 1.0])], &[]).unwrap();

        let (first, second) = (root.join("first"), root.join("second"));
        store.save_to_path(&first).unwrap();
        assert!(hot_file(&root.join("live"), 1).exists());

        store.apply_commit(type_id(7), SequenceNumber::MIN, &[(300, vec![0.0, -1.0])], &[]).unwrap();
        store.save_to_path(&second).unwrap();

        let first_clean = fs::read(hot_file(&first, 1)).unwrap();
        assert_eq!(fs::read(hot_file(&second, 1)).unwrap(), first_clean);

        let loaded =
            VectorIndex { live_directory: Some(root.join("live")), ..VectorIndex::load_from_path(&second).unwrap() };
        assert_eq!(loaded.latest_version().get_value_by_attr_vertex(vertex(1, 100)), Some(vec![1.0, 0.0]));
        assert_eq!(loaded.latest_version().get_value_by_attr_vertex(vertex(7, 200)), Some(vec![0.0, 1.0]));
        assert_eq!(loaded.latest_version().get_value_by_attr_vertex(vertex(7, 300)), Some(vec![0.0, -1.0]));

        let third = root.join("third");
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
        assert_eq!(fs::read_to_string(dir.join(FORMAT_VERSION_FILE_NAME)).unwrap(), format!("{FORMAT_VERSION}\n"));

        let loaded = VectorIndex::load_from_path(&dir).unwrap();
        assert!(loaded.latest_version().per_type_indices.is_empty());

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn load_rejects_other_format_versions() {
        let store = VectorIndex::new();
        store.apply_commit(type_id(1), SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();

        let root = std::env::temp_dir().join(format!("vector_index_version_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let dir = root.join("extension");
        store.save_to_path(&dir).unwrap();

        fs::write(dir.join(FORMAT_VERSION_FILE_NAME), format!("{}\n", FORMAT_VERSION + 1)).unwrap();
        let error = VectorIndex::load_from_path(&dir).unwrap_err();
        assert!(error.to_string().contains("unsupported vector index format version"));

        fs::remove_file(dir.join(FORMAT_VERSION_FILE_NAME)).unwrap();
        assert!(VectorIndex::load_from_path(&dir).is_err());

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn concurrent_searches_and_adds_beyond_hardware_threads() {
        let type_id = type_id(1);
        let store = VectorIndex::new();
        let vector_for = |seed: u64| (0..64).map(|i| ((seed * 64 + i) as f32).sin()).collect::<Vec<_>>();
        for key in 0..2000u64 {
            store.apply_commit(type_id, SequenceNumber::MIN, &[(key << 8, vector_for(key))], &[]).unwrap();
        }
        let threads = 8 * std::thread::available_parallelism().unwrap().get();
        let start = std::sync::Barrier::new(threads);
        std::thread::scope(|scope| {
            for thread in 0..threads {
                let (store, start) = (&store, &start);
                scope.spawn(move || {
                    start.wait();
                    for i in 0..200u64 {
                        let key = 2000 + thread as u64 * 200 + i;
                        assert_eq!(
                            store
                                .latest_version()
                                .search_attributes_by_approximate_nearest_neighbour(type_id, &vector_for(key + 7), 10)
                                .len(),
                            10
                        );
                        store.apply_commit(type_id, SequenceNumber::MIN, &[(key << 8, vector_for(key))], &[]).unwrap();
                    }
                });
            }
        });
        assert_eq!(store.latest_version().get_count_for_type(type_id), 2000 + threads * 200);
    }
}
