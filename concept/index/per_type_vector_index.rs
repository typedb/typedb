/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fs,
    fs::File,
    io,
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use encoding::value::value_type::VectorPrecision;
use storage::sequence_number::SequenceNumber;
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

use super::vector_index::VectorIndexError;

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct VectorShape {
    pub dimensions: usize,
    pub precision: VectorPrecision,
}

pub(super) struct PerTypeVectorIndex {
    hot_shard: RwLock<Arc<Index>>,
    pub(super) recently_deleted: RwLock<HashMap<u64, (SequenceNumber, Vec<f32>)>>,
    dirty: AtomicBool,
    shape: VectorShape,
}

impl PerTypeVectorIndex {
    pub(super) fn create(shape: VectorShape) -> Result<Self, VectorIndexError> {
        let dimensions = shape.dimensions;
        let options = IndexOptions {
            dimensions,
            metric: MetricKind::Cos,
            quantization: match shape.precision {
                VectorPrecision::Float32 => ScalarKind::F32,
            },
            connectivity: 0,
            expansion_add: 0,
            expansion_search: 0,
            multi: false,
        };
        let index = usearch::new_index(&options)
            .map_err(|err| VectorIndexError::IndexCreate { dimensions, reason: err.to_string() })?;
        Ok(Self {
            hot_shard: RwLock::new(Arc::new(index)),
            recently_deleted: RwLock::new(HashMap::new()),
            dirty: AtomicBool::new(true),
            shape,
        })
    }

    /// Loads the type's index file; its dimensions and metric come from the file's header.
    pub(super) fn load(dir: &Path, type_id: u16) -> io::Result<Self> {
        let path = dir.join(type_dir_name(type_id)).join(HOT_SHARD_FILE_NAME);
        let index = Index::restore(path.to_str().unwrap())
            .map_err(|err| invalid_data(format!("failed to load vector index {path:?}: {err}")))?;
        let shape = VectorShape { dimensions: index.dimensions(), precision: VectorPrecision::Float32 };
        Ok(Self {
            hot_shard: RwLock::new(Arc::new(index)),
            recently_deleted: RwLock::new(HashMap::new()),
            dirty: AtomicBool::new(true),
            shape,
        })
    }

    pub(super) fn get_value(&self, key: u64) -> Option<Vec<f32>> {
        let index = self.hot_shard.read().unwrap();
        let mut buffer = vec![0.0f32; index.dimensions()];
        let found = index.get(key, &mut buffer).unwrap_or_else(|err| panic!("failed to read vector from index: {err}"));
        if found > 0 {
            return Some(buffer);
        }
        drop(index);
        self.recently_deleted.read().unwrap().get(&key).map(|(_, vector)| vector.clone())
    }

    pub(super) fn get_deleted_keys(&self) -> Vec<u64> {
        self.recently_deleted.read().unwrap().keys().copied().collect()
    }

    pub(super) fn purge_deleted(&self, bound: SequenceNumber) {
        self.recently_deleted.write().unwrap().retain(|_, (deleted_at, _)| *deleted_at > bound);
    }

    pub(super) fn search_by_approximate_nearest_neighbour(
        &self,
        reference_point: &[f32],
        count: usize,
    ) -> Vec<(u64, f32)> {
        let matches = self.hot_shard.read().unwrap().search(reference_point, count);
        let matches = matches.or_else(|_| self.hot_shard.write().unwrap().search(reference_point, count));
        let matches = matches.unwrap_or_else(|err| panic!("vector index search failed: {err}"));
        matches.keys.into_iter().zip(matches.distances).collect()
    }

    pub(super) fn get_count(&self) -> usize {
        self.hot_shard.read().unwrap().size()
    }

    pub(super) fn apply_commit(
        &self,
        sequence_number: SequenceNumber,
        inserts: &[(u64, Vec<f32>)],
        deletes: &[u64],
    ) -> Result<(), VectorIndexError> {
        let index = self.hot_shard.write().unwrap();
        self.apply_deletes(&index, sequence_number, deletes);
        self.apply_inserts(&index, inserts)
    }

    fn apply_inserts(&self, index: &Index, inserts: &[(u64, Vec<f32>)]) -> Result<(), VectorIndexError> {
        for (key, vector) in inserts {
            self.insert(index, *key, vector)?;
        }
        Ok(())
    }

    fn insert(&self, index: &Index, key: u64, vector: &[f32]) -> Result<(), VectorIndexError> {
        if index.contains(key) {
            return Ok(());
        }
        Self::check_dimensions(index, vector.len())?;
        if index.size() >= index.capacity() {
            index
                .reserve((index.capacity() * 2).max(1024))
                .map_err(|err| VectorIndexError::IndexGrow { reason: err.to_string() })?;
        }
        index.add(key, vector).map_err(|err| VectorIndexError::IndexInsert { reason: err.to_string() })?;
        self.recently_deleted.write().unwrap().remove(&key);
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn apply_deletes(&self, index: &Index, sequence_number: SequenceNumber, keys: &[u64]) {
        for key in keys {
            self.delete(index, sequence_number, *key);
        }
    }

    fn delete(&self, index: &Index, sequence_number: SequenceNumber, key: u64) {
        let mut buffer = vec![0.0f32; index.dimensions()];
        let found = index.get(key, &mut buffer).unwrap_or_else(|err| panic!("failed to read vector from index: {err}"));
        if found > 0 {
            index.remove(key).unwrap_or_else(|err| panic!("failed to remove vector from index: {err}"));
            self.recently_deleted.write().unwrap().insert(key, (sequence_number, buffer));
            self.dirty.store(true, Ordering::Release);
        }
    }

    pub(super) fn reserve(&self, dimensions: usize, additional: usize) -> Result<(), VectorIndexError> {
        {
            let index = self.hot_shard.read().unwrap();
            Self::check_dimensions(&index, dimensions)?;
            if index.size() + additional <= index.capacity() {
                return Ok(());
            }
        }
        let index = self.hot_shard.write().unwrap();
        let required = index.size() + additional;
        if required > index.capacity() {
            index
                .reserve((index.capacity() * 2).max(1024).max(required))
                .map_err(|err| VectorIndexError::IndexGrow { reason: err.to_string() })?;
        }
        Ok(())
    }

    pub(super) fn get_shape(&self) -> VectorShape {
        self.shape
    }

    pub(super) fn get_dimensions(&self) -> usize {
        self.hot_shard.read().unwrap().dimensions()
    }

    pub(super) fn is_reusable_for(&self, dimensions: usize) -> bool {
        self.get_dimensions() == dimensions || self.get_count() > 0
    }

    fn check_dimensions(index: &Index, dimensions: usize) -> Result<(), VectorIndexError> {
        if index.dimensions() != dimensions {
            return Err(VectorIndexError::DimensionMismatch {
                index_dimensions: index.dimensions(),
                vector_dimensions: dimensions,
            });
        }
        Ok(())
    }

    pub(super) fn checkpoint_to(&self, live_dir: Option<&Path>, checkpoint_dir: &Path) -> io::Result<()> {
        fs::create_dir_all(checkpoint_dir)?;
        let checkpoint_file = checkpoint_dir.join(HOT_SHARD_FILE_NAME);
        match live_dir {
            Some(live_dir) => {
                fs::create_dir_all(live_dir)?;
                let live_file = live_dir.join(HOT_SHARD_FILE_NAME);
                if self.dirty.swap(false, Ordering::Acquire) || !live_file.exists() {
                    if let Err(error) = self.write_file(&live_file) {
                        self.dirty.store(true, Ordering::Release);
                        return Err(error);
                    }
                }
                fs::hard_link(&live_file, &checkpoint_file)
                    .or_else(|_| fs::copy(&live_file, &checkpoint_file).map(|_| ()))
            }
            None => self.write_file(&checkpoint_file),
        }
    }

    fn write_file(&self, target: &Path) -> io::Result<()> {
        let staging = target.with_extension("tmp");
        let index = self.hot_shard.read().unwrap();
        index
            .save(staging.to_str().unwrap())
            .map_err(|err| io::Error::other(format!("failed to save vector index {target:?}: {err}")))?;
        File::options().write(true).open(&staging)?.sync_all()?;
        fs::rename(&staging, target)?;
        durability::sync_directory(target.parent().unwrap())?;
        Ok(())
    }
}

pub(super) const HOT_SHARD_FILE_NAME: &str = "hot.usearch";

pub(super) fn type_dir_name(type_id: u16) -> String {
    format!("{type_id:#06x}")
}

pub(super) fn parse_type_dir_name(name: &str) -> Option<u16> {
    name.strip_prefix("0x").and_then(|hex| u16::from_str_radix(hex, 16).ok())
}

pub(super) fn invalid_data(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::*;

    fn index(dimensions: usize) -> PerTypeVectorIndex {
        PerTypeVectorIndex::create(VectorShape { dimensions, precision: VectorPrecision::Float32 }).unwrap()
    }

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("per_type_vector_index_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        root
    }

    #[test]
    fn insert_then_get_search_and_count() {
        let index = index(3);
        index
            .apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0, 0.0]), (200, vec![0.0, 1.0, 0.0])], &[])
            .unwrap();

        assert_eq!(index.get_value(100), Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(index.get_value(300), None);
        assert_eq!(index.get_count(), 2);
        let results = index.search_by_approximate_nearest_neighbour(&[0.9, 0.1, 0.0], 2);
        assert_eq!(results.iter().map(|(key, _)| *key).collect::<Vec<_>>(), vec![100, 200]);
    }

    #[test]
    fn insert_of_an_existing_key_is_a_no_op() {
        let index = index(2);
        index.apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        index.apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        assert_eq!(index.get_count(), 1);
    }

    #[test]
    fn insert_with_mismatched_dimensions_is_rejected() {
        let index = index(3);
        assert!(index.apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).is_err());
        assert_eq!(index.get_count(), 0);
        assert_eq!(index.get_value(100), None);
    }

    #[test]
    fn delete_drops_from_search_but_keeps_value_until_reinserted() {
        let index = index(2);
        index.apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0]), (200, vec![0.0, 1.0])], &[]).unwrap();

        index.apply_commit(SequenceNumber::MIN, &[], &[100]).unwrap();
        assert_eq!(index.get_count(), 1);
        assert!(index.search_by_approximate_nearest_neighbour(&[1.0, 0.0], 2).iter().all(|(key, _)| *key != 100));
        assert_eq!(index.get_value(100), Some(vec![1.0, 0.0]), "deleted values stay readable for older snapshots");

        index.apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        assert!(index.recently_deleted.read().unwrap().is_empty(), "reinserting clears the retained copy");
        assert_eq!(index.get_count(), 2);
    }

    #[test]
    fn purge_drops_values_deleted_at_or_before_the_bound() {
        let index = index(2);
        index.apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0]), (200, vec![0.0, 1.0])], &[]).unwrap();
        index.apply_commit(SequenceNumber::new(5), &[], &[100]).unwrap();
        index.apply_commit(SequenceNumber::new(7), &[], &[200]).unwrap();
        assert_eq!(index.get_deleted_keys().len(), 2);

        index.purge_deleted(SequenceNumber::new(4));
        assert_eq!(index.get_value(100), Some(vec![1.0, 0.0]), "a transaction opened at 4 still sees it");

        index.purge_deleted(SequenceNumber::new(5));
        assert_eq!(index.get_value(100), None);
        assert_eq!(index.get_value(200), Some(vec![0.0, 1.0]));
        assert_eq!(index.get_deleted_keys(), vec![200]);
    }

    #[test]
    fn delete_of_a_missing_key_is_a_no_op() {
        let index = index(2);
        index.apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        index.apply_commit(SequenceNumber::MIN, &[], &[300]).unwrap();
        assert_eq!(index.get_count(), 1);
        assert!(index.recently_deleted.read().unwrap().is_empty());
    }

    #[test]
    fn reserve_checks_dimensions() {
        let index = index(2);
        index.reserve(2, 5000).unwrap();
        assert!(index.reserve(3, 1).is_err());
    }

    #[test]
    fn is_reusable_for_matching_dimensions_or_when_non_empty() {
        let index = index(2);
        assert!(index.is_reusable_for(2));
        assert!(!index.is_reusable_for(3), "an empty index with other dimensions should be replaced");

        index.apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();
        assert!(index.is_reusable_for(3), "a non-empty index is kept so the insert fails on dimensions");
    }

    #[test]
    fn checkpoint_then_load_roundtrip() {
        let root = temp_root("roundtrip");
        let index = index(2);
        index.apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0]), (200, vec![0.0, 1.0])], &[]).unwrap();
        index.checkpoint_to(None, &root.join(type_dir_name(1))).unwrap();

        let loaded = PerTypeVectorIndex::load(&root, 1).unwrap();
        assert_eq!(loaded.get_shape(), index.get_shape());
        assert_eq!(loaded.get_count(), 2);
        assert_eq!(loaded.get_value(200), Some(vec![0.0, 1.0]));
        assert_eq!(loaded.search_by_approximate_nearest_neighbour(&[1.0, 0.0], 1)[0].0, 100);

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn checkpoint_rewrites_the_live_file_only_when_dirty() {
        let root = temp_root("dirty");
        let live = root.join("live");
        let index = index(2);
        index.apply_commit(SequenceNumber::MIN, &[(100, vec![1.0, 0.0])], &[]).unwrap();

        index.checkpoint_to(Some(&live), &root.join("first")).unwrap();
        assert!(!index.dirty.load(Ordering::Acquire));
        let live_file = live.join(HOT_SHARD_FILE_NAME);
        let written = fs::metadata(&live_file).unwrap().modified().unwrap();

        index.checkpoint_to(Some(&live), &root.join("second")).unwrap();
        assert_eq!(fs::metadata(&live_file).unwrap().modified().unwrap(), written, "clean index reuses the live file");
        assert_eq!(fs::read(root.join("second").join(HOT_SHARD_FILE_NAME)).unwrap(), fs::read(&live_file).unwrap());

        index.apply_commit(SequenceNumber::MIN, &[], &[100]).unwrap();
        assert!(index.dirty.load(Ordering::Acquire));
        index.checkpoint_to(Some(&live), &root.join("third")).unwrap();
        assert!(!index.dirty.load(Ordering::Acquire));
        assert_ne!(
            fs::read(root.join("third").join(HOT_SHARD_FILE_NAME)).unwrap(),
            fs::read(root.join("second").join(HOT_SHARD_FILE_NAME)).unwrap(),
            "dirty index rewrites the live file"
        );

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn loaded_index_is_dirty_so_its_first_checkpoint_writes_the_live_file() {
        let root = temp_root("loaded_dirty");
        index(2).checkpoint_to(None, &root.join(type_dir_name(1))).unwrap();
        let loaded = PerTypeVectorIndex::load(&root, 1).unwrap();
        assert!(loaded.dirty.load(Ordering::Acquire));

        let live = root.join("live");
        loaded.checkpoint_to(Some(&live), &root.join("next")).unwrap();
        assert!(live.join(HOT_SHARD_FILE_NAME).exists());

        fs::remove_dir_all(&root).unwrap();
    }
}
