/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
pub(crate) mod iterator;
mod iterpool;
pub mod pool;

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::{util::MB, Bytes};
use error::{typedb_error, TypeDBError};
use primitive::key_range::KeyRange;
use resource::{constants::storage::ROCKSDB_CACHE_SIZE_MB, profile::StorageCounters};
use rocksdb::{
    checkpoint::Checkpoint, BlockBasedIndexType, BlockBasedOptions, Cache, DBCompressionType, IteratorMode, Options,
    ReadOptions, SliceTransform, WriteBatch, WriteOptions, DB,
};

use crate::{
    keyspaces::{KeyspaceId, KeyspaceSet, Keyspaces, KeyspacesError},
    rocks::iterator::RocksRangeIterator,
    KVStore, KVStoreID,
};

pub struct RocksKVStore {
    path: PathBuf,
    pub(crate) name: &'static str,
    id: KVStoreID,
    pub(crate) rocks: DB,
    rocks_raw_iterator_pool: iterpool::RocksRawIteratorPool,
    read_options: ReadOptions,
    write_options: WriteOptions,
    pub(crate) prefix_length: Option<usize>,
}

impl RocksKVStore {
    fn new(path: PathBuf, name: &'static str, id: KVStoreID, rocks: DB, prefix_length: Option<usize>) -> Self {
        // initial read options, should be customised to this storage's properties
        let read_options = ReadOptions::default();
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        let pool = iterpool::RocksRawIteratorPool::new();
        Self { path, name, id, rocks, rocks_raw_iterator_pool: pool, read_options, write_options, prefix_length }
    }

    pub(crate) fn open<'a>(
        storage_path: PathBuf,
        options: Options,
        prefix_length: Option<usize>,
        name: &'static str,
        id: KVStoreID,
    ) -> Result<Self, Box<dyn TypeDBError>> {
        use RocksKVError::Open;
        let path = storage_path.join(name);
        let kv_storage = DB::open(&options, &path).map_err(|error| Open { name, source: error })?;
        Ok(Self::new(path, name, id, kv_storage, prefix_length.to_owned()))
    }

    pub fn open_keyspaces<KS: KeyspaceSet>(storage_dir: &Path) -> Result<Keyspaces, KeyspacesError> {
        let cache = RocksKVStore::create_cache();

        let mut keyspaces = Keyspaces::new();
        for keyspace in KS::iter() {
            keyspaces.validate_new_keyspace(keyspace)?;
            let prefix_length = keyspace.prefix_length();
            let options = RocksKVStore::create_open_options(&cache, prefix_length);
            let kv = RocksKVStore::open(
                storage_dir.to_path_buf(),
                options,
                prefix_length,
                keyspace.name(),
                keyspace.id().into(),
            )
            .map_err(|e| KeyspacesError::KVStoreError { typedb_source: e.into() })?;
            keyspaces.keyspaces.push(KVStore::RocksDB(kv));
            keyspaces.index[keyspace.id().0 as usize] = Some(KeyspaceId(keyspaces.keyspaces.len() as u8 - 1));
        }
        Ok(keyspaces)
    }

    pub(crate) fn default_read_options(&self) -> ReadOptions {
        let mut options = ReadOptions::default();
        options.set_total_order_seek(true);
        options
    }

    pub(crate) fn bloom_read_options(&self) -> ReadOptions {
        let mut options = ReadOptions::default();
        options.set_prefix_same_as_start(true);
        options.set_total_order_seek(false);
        options
    }

    fn create_cache() -> Cache {
        Cache::new_lru_cache((ROCKSDB_CACHE_SIZE_MB * MB) as usize)
    }

    fn create_open_options(cache: &Cache, prefix_length: Option<usize>) -> Options {
        let mut options = rocksdb::Options::default();

        // Enable if we wanted to check bloom filter usage, cache hits, etc.
        // options.enable_statistics();
        // options.set_stats_dump_period_sec(100);

        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_max_background_jobs(10);
        options.set_target_file_size_base(64 * MB);
        options.set_write_buffer_size(64 * MB as usize);
        options.set_max_write_buffer_size_to_maintain(0);
        options.set_max_write_buffer_number(2);
        options.set_memtable_whole_key_filtering(false);
        options.set_optimize_filters_for_hits(false); // true => don't build bloom filters for the last level
        options.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
        ]);

        // TODO: 2.x has   enable_index_compression: 1 set to 0

        let mut block_options = BlockBasedOptions::default();
        block_options.set_block_cache(cache);
        block_options.set_block_restart_interval(16);
        block_options.set_index_block_restart_interval(16);
        block_options.set_format_version(6);
        block_options.set_block_size(16 * 1024);
        block_options.set_whole_key_filtering(false);

        block_options.set_bloom_filter(10.0, false);
        block_options.set_partition_filters(true);
        block_options.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
        block_options.set_optimize_filters_for_memory(true);
        block_options.set_pin_top_level_index_and_filter(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_options.set_cache_index_and_filter_blocks(true);

        if let Some(prefix_len) = prefix_length {
            options.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len))
        }
        options.set_block_based_table_factory(&block_options);

        options
    }

    pub(crate) fn iterpool(&self) -> &iterpool::RocksRawIteratorPool {
        &self.rocks_raw_iterator_pool
    }

    pub fn id(&self) -> KVStoreID {
        self.id
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn TypeDBError>> {
        self.rocks
            .put_opt(key, value, &self.write_options)
            .map_err(|error| RocksKVError::Put { name: self.name, source: error }.into())
    }

    pub fn get<M, V>(&self, key: &[u8], mut mapper: M) -> Result<Option<V>, Box<dyn TypeDBError>>
    where
        M: FnMut(&[u8]) -> V,
    {
        self.rocks
            .get_pinned_opt(key, &self.read_options)
            .map(|option| option.map(|value| mapper(value.as_ref())))
            .map_err(|error| RocksKVError::Get { name: self.name, source: error }.into())
    }

    pub fn get_prev<M, T>(&self, key: &[u8], mut mapper: M) -> Option<T>
    where
        M: FnMut(&[u8], &[u8]) -> T,
    {
        let mut iterator = self.rocks.raw_iterator_opt(self.default_read_options());
        iterator.seek_for_prev(key);
        iterator.item().map(|(k, v)| mapper(k, v))
    }

    pub fn iterate_range<const PREFIX_INLINE_SIZE: usize>(
        &self,
        range: &KeyRange<Bytes<'_, PREFIX_INLINE_SIZE>>,
        storage_counters: StorageCounters,
    ) -> RocksRangeIterator {
        RocksRangeIterator::new(self, range, storage_counters)
    }

    pub fn write(&self, write_batch: WriteBatch) -> Result<(), Box<dyn TypeDBError>> {
        self.rocks
            .write_opt(write_batch, &self.write_options)
            .map_err(|error| RocksKVError::BatchWrite { name: self.name, source: error }.into())
    }

    pub fn checkpoint(&self, checkpoint_dir: &Path) -> Result<(), Box<dyn TypeDBError>> {
        use RocksKVError::{CheckpointExists, CreateRocksDBCheckpoint};

        let checkpoint_dir = checkpoint_dir.join(self.name);
        if checkpoint_dir.exists() {
            return Err(CheckpointExists { name: self.name, dir: checkpoint_dir.display().to_string() }.into());
        }

        Checkpoint::new(&self.rocks)
            .and_then(|checkpoint| checkpoint.create_checkpoint(&checkpoint_dir))
            .map_err(|error| CreateRocksDBCheckpoint { name: self.name, source: error })?;

        Ok(())
    }

    pub fn delete(self) -> Result<(), Box<dyn TypeDBError>> {
        drop(self.rocks);
        fs::remove_dir_all(self.path.clone())
            .map_err(|error| RocksKVError::DeleteErrorDirectoryRemove { name: self.name, source: Arc::new(error) })?;
        Ok(())
    }

    pub fn reset(&mut self) -> Result<(), Box<dyn TypeDBError>> {
        let iterator = self.rocks.iterator(IteratorMode::Start);
        for entry in iterator {
            let (key, _) = entry.map_err(|err| RocksKVError::Iterate { name: self.name, source: err })?;
            self.rocks.delete(key).map_err(|err| RocksKVError::Iterate { name: self.name, source: err })?;
        }
        Ok(())
    }

    pub fn estimate_size_in_bytes(&self) -> Result<u64, Box<dyn TypeDBError>> {
        let property_name = rocks_constants::PROPERTY_ESTIMATE_LIVE_DATA_SIZE;
        self.rocks
            .property_int_value(property_name)
            .map_err(|source| RocksKVError::PropertyRead { name: property_name, source }.into())
            .map(|result_opt| result_opt.unwrap_or(0))
    }

    pub fn estimate_key_count(&self) -> Result<u64, Box<dyn TypeDBError>> {
        let property_name = rocks_constants::PROPERTY_ESTIMATE_NUM_KEYS;
        self.rocks
            .property_int_value(property_name)
            .map_err(|source| RocksKVError::PropertyRead { name: property_name, source }.into())
            .map(|result_opt| result_opt.unwrap_or(0))
    }
}

typedb_error! {
    pub RocksKVError(component = "RocksDB error", prefix = "RKV") {
        Open(1, "RocksDB error opening kv store {name}.", name: &'static str, source: rocksdb::Error),
        Get(2, "RocksDB error getting key in kv store {name}." , name: &'static str, source: rocksdb::Error),
        Put(3, "RocksDB error putting key in kv store {name}.", name: &'static str, source: rocksdb::Error),
        BatchWrite(4, "RocksDB error writing batch to kv store {name}.", name: &'static str, source: rocksdb::Error),
        Iterate(5, "RocksDB error iterating kv store {name}.", name: &'static str, source: rocksdb::Error),
        DeleteRange(6, "RocksDB error deleting range in kv store {name}.", name: &'static str, source: rocksdb::Error),
        PropertyRead(7, "RocksDB error reading property in kv store {name}.", name: &'static str, source: rocksdb::Error),

        CheckpointExists(20, "Cannot create checkpoint for kv store {name} - path {dir} exists.", name: &'static str, dir: String),
        CreateRocksDBCheckpoint(21, "RocksDB error creating checkpoint for kv store {name}.", name: &'static str, source: rocksdb::Error),

        DeleteErrorDirectoryRemove(30, "Failed to delete directory of kv store {name}",name: &'static str, source: Arc<io::Error>),
    }
}

impl std::fmt::Display for RocksKVError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use error::TypeDBError;
        write!(f, "{}", self.format_code_and_description())
    }
}

impl std::error::Error for RocksKVError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl std::fmt::Debug for RocksKVStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksKVStore")
            .field("path", &self.path)
            .field("name", &self.name)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

// RocksDB properties. The full list of available properties is available here:
// https://github.com/facebook/rocksdb/blob/20357988345b02efcef303bc274089111507e160/include/rocksdb/db.h#L750
pub(crate) mod rocks_constants {
    pub(crate) const PROPERTY_ESTIMATE_LIVE_DATA_SIZE: &str = "rocksdb.estimate-live-data-size";
    pub(crate) const PROPERTY_ESTIMATE_NUM_KEYS: &str = "rocksdb.estimate-num-keys";
}
