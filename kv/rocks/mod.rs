/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod iterator;

use crate::{KVStore, KVStoreError, KVStoreID};
use bytes::Bytes;
use error::typedb_error;
use primitive::key_range::KeyRange;
use resource::profile::StorageCounters;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions, DB};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

struct RocksKVStore {
    path: PathBuf,
    name: &'static str,
    id: KVStoreID,
    rocks: DB,
    read_options: ReadOptions,
    write_options: WriteOptions,
    prefix_length: Option<usize>,
}

impl RocksKVStore {
    fn new(path: PathBuf, name: &'static str, id: KVStoreID, rocks: DB, prefix_length: Option<usize>) -> Self {
        // initial read options, should be customised to this storage's properties
        let read_options = ReadOptions::default();
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        Self { path, name, id, rocks, read_options, write_options, prefix_length }
    }

    fn new_read_options(&self) -> ReadOptions {
        let mut options = ReadOptions::default();
        options.set_total_order_seek(true); // Set this to 'false' to use bloom-filters
        options
    }
}

impl KVStore for RocksKVStore {
    type OpenOptions<'a> = (PathBuf, &'a Options, Option<usize>);
    type IteratorPool = ();
    type RangeIterator = ();
    type WriteBatch = WriteBatch;
    type CheckpointArgs<'a> = &'a Path;

    fn open<'a>(open_options: &Self::OpenOptions<'a>, name: &'static str, id: KVStoreID) -> Result<Self, Box<dyn KVStoreError>> {
        use RocksKVError::Open;
        let (storage_path, options, prefix_length) = open_options;
        let path = storage_path.join(name);
        let kv_storage = DB::open(options, &path).map_err(|error| Open { name, source: error })?;
        Ok(Self::new(path, name, id, kv_storage, prefix_length.to_owned()))
    }

    fn id(&self) -> KVStoreID {
        self.id
    }

    fn name(&self) -> &str {
        self.name
    }
    //
    // async fn prefix_length(&self) -> Option<usize> {
    //     self.prefix_length.clone()
    // }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn KVStoreError>> {
        self.rocks
            .put_opt(key, value, &self.write_options)
            .map_err(|error| RocksKVError::Put { name: self.name, source: error }.into())
    }

    async fn get<M, V>(&self, key: &[u8], mapper: &mut M) -> Result<Option<V>, Box<dyn KVStoreError>>
    where
        M: FnMut(&[u8]) -> V,
    {
        self.rocks
            .get_pinned_opt(key, &self.read_options)
            .map(|option| option.map(|value| mapper(value.as_ref())))
            .map_err(|error| RocksKVError::Get { name: self.name, source: error }.into())
    }

    async fn get_prev<M, T>(&self, key: &[u8], mapper: &mut M) -> Option<T>
    where
        M: FnMut(&[u8], &[u8]) -> T,
    {
        let mut iterator = self.rocks.raw_iterator_opt(self.new_read_options());
        iterator.seek_for_prev(key);
        iterator.item().map(|(k, v)| mapper(k, v))
    }

    async fn iterate_range<const PREFIX_INLINE_SIZE: usize>(
        &self,
        iterpool: &Self::IteratorPool,
        range: &KeyRange<Bytes<'_, PREFIX_INLINE_SIZE>>,
        storage_counters: StorageCounters,
    ) -> Self::RangeIterator {
        // iterator::KeyspaceRangeIterator::new(self, iterpool, range, storage_counters)
    }

    async fn write(&self, write_batch: Self::WriteBatch) -> Result<(), Box<dyn KVStoreError>> {
        self.rocks
            .write_opt(write_batch, &self.write_options)
            .map_err(|error| RocksKVError::BatchWrite { name: self.name, source: error }.into())
    }

    async fn checkpoint<'a>(&self, checkpoint_dir: &Self::CheckpointArgs<'a>) -> Result<(), Box<dyn KVStoreError>> {
        use RocksKVError::{CheckpointExists, CreateRocksDBCheckpoint};

        let checkpoint_dir = checkpoint_dir.join(self.name);
        if checkpoint_dir.exists() {
            return Err(CheckpointExists { name: self.name, dir: checkpoint_dir.display().to_string() }.into());
        }

        // TODO: spawn_blocking
        Checkpoint::new(&self.rocks)
            .and_then(|checkpoint| checkpoint.create_checkpoint(&checkpoint_dir))
            .map_err(|error| CreateRocksDBCheckpoint { name: self.name, source: error })?;

        Ok(())
    }

    async fn delete(self) -> Result<(), Box<dyn KVStoreError>> {
        drop(self.rocks);
        fs::remove_dir_all(self.path.clone())
            .map_err(|error| RocksKVError::DeleteErrorDirectoryRemove { name: self.name, source: Arc::new(error) })?;
        Ok(())
    }

    async fn reset(&mut self) -> Result<(), Box<dyn KVStoreError>>  {
        let iterator = self.rocks.iterator(IteratorMode::Start);
        for entry in iterator {
            let (key, _) = entry.map_err(|err| RocksKVError::Iterate { name: self.name, source: err })?;
            self.rocks.delete(key).map_err(|err| RocksKVError::Iterate { name: self.name, source: err })?;
        }
        Ok(())
    }

    fn estimate_size_in_bytes(&self) -> Result<u64, Box<dyn KVStoreError>>  {
        let property_name = rocks_constants::PROPERTY_ESTIMATE_LIVE_DATA_SIZE;
        self.rocks
            .property_int_value(property_name)
            .map_err(|source| RocksKVError::PropertyRead{ name: property_name, source }.into())
            .map(|result_opt| result_opt.unwrap_or(0))
    }

    fn estimate_key_count(&self) -> Result<u64, Box<dyn KVStoreError>>  {
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

impl KVStoreError for RocksKVError {}


// RocksDB properties. The full list of available properties is available here:
// https://github.com/facebook/rocksdb/blob/20357988345b02efcef303bc274089111507e160/include/rocksdb/db.h#L750
pub(crate) mod rocks_constants {
    pub(crate) const PROPERTY_ESTIMATE_LIVE_DATA_SIZE: &str = "rocksdb.estimate-live-data-size";
    pub(crate) const PROPERTY_ESTIMATE_NUM_KEYS: &str = "rocksdb.estimate-num-keys";
}
