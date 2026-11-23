/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
};

use bytes::Bytes;
use fail_point::{KEYSPACE_CHECKPOINT_FAIL, KEYSPACE_DELETE_FAIL, KEYSPACE_OPEN_FAIL, fail_point};
use itertools::Itertools;
use resource::{constants::common::GB, profile::StorageCounters};
use rocksdb::{DB, IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions, checkpoint::Checkpoint};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::{IteratorPool, constants, iterator};
use crate::{key_range::KeyRange, keyspace::rocks_resources::RocksResources, write_batches::WriteBatches};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyspaceId(pub u8);

impl fmt::Debug for KeyspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for KeyspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WARNING: adjusting these constants affects many things, including serialised WAL records and in-memory data structures.  //
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
pub(crate) const KEYSPACE_MAXIMUM_COUNT: usize = 10;
pub(crate) const KEYSPACE_ID_MAX: KeyspaceId = KeyspaceId(KEYSPACE_MAXIMUM_COUNT as u8 - 1);
pub(crate) const KEYSPACE_ID_RESERVED_UNSET: KeyspaceId = KeyspaceId(KEYSPACE_ID_MAX.0 + 1);

struct CompactionConfiguration {
    disable_auto_compactions: bool,
    level_zero_slowdown_writes_trigger: i32,
    level_zero_stop_writes_trigger: i32,
    soft_pending_compaction_bytes_limit: usize,
    hard_pending_compaction_bytes_limit: usize,
}

impl CompactionConfiguration {
    const DEFAULT: Self = Self {
        disable_auto_compactions: false,
        level_zero_slowdown_writes_trigger: 20,
        level_zero_stop_writes_trigger: 36,
        soft_pending_compaction_bytes_limit: 64 * GB as usize,
        hard_pending_compaction_bytes_limit: 256 * GB as usize,
    };

    const BULK_LOAD: Self = Self {
        disable_auto_compactions: true,
        level_zero_slowdown_writes_trigger: 1 << 30,
        level_zero_stop_writes_trigger: 1 << 30,
        soft_pending_compaction_bytes_limit: 0,
        hard_pending_compaction_bytes_limit: 0,
    };

    fn configure(&self, options: &mut Options) {
        options.set_disable_auto_compactions(self.disable_auto_compactions);
        options.set_level_zero_slowdown_writes_trigger(self.level_zero_slowdown_writes_trigger);
        options.set_level_zero_stop_writes_trigger(self.level_zero_stop_writes_trigger);
        options.set_soft_pending_compaction_bytes_limit(self.soft_pending_compaction_bytes_limit);
        options.set_hard_pending_compaction_bytes_limit(self.hard_pending_compaction_bytes_limit);
    }

    // an open keyspace only takes these options by name
    fn apply(&self, kv_storage: &DB) -> Result<(), rocksdb::Error> {
        kv_storage.set_options(&[
            ("disable_auto_compactions", &self.disable_auto_compactions.to_string()),
            ("level0_slowdown_writes_trigger", &self.level_zero_slowdown_writes_trigger.to_string()),
            ("level0_stop_writes_trigger", &self.level_zero_stop_writes_trigger.to_string()),
            ("soft_pending_compaction_bytes_limit", &self.soft_pending_compaction_bytes_limit.to_string()),
            ("hard_pending_compaction_bytes_limit", &self.hard_pending_compaction_bytes_limit.to_string()),
        ])
    }
}

pub trait KeyspaceSet: Copy {
    fn iter() -> impl Iterator<Item = Self>;
    fn id(&self) -> KeyspaceId;
    fn name(&self) -> &'static str;
    fn rocks_configuration(&self, _resources: &RocksResources) -> rocksdb::Options {
        let mut options = Options::default();
        options.create_if_missing(true);
        options
    }
    fn prefix_length(&self) -> Option<usize>;
}

#[derive(Debug)]
pub struct Keyspaces {
    keyspaces: Vec<Keyspace>,
    index: [Option<KeyspaceId>; KEYSPACE_MAXIMUM_COUNT],
}

impl Keyspaces {
    pub(crate) fn new() -> Self {
        Self { keyspaces: Vec::new(), index: std::array::from_fn(|_| None) }
    }

    pub(crate) fn open<KS: KeyspaceSet>(
        storage_dir: impl AsRef<Path>,
        resources: &RocksResources,
    ) -> Result<Self, KeyspaceOpenError> {
        let path = storage_dir.as_ref();

        let mut keyspaces = Keyspaces::new();
        for keyspace in KS::iter() {
            keyspaces
                .validate_new_keyspace(keyspace)
                .map_err(|error| KeyspaceOpenError::Validation { source: error })?;
            fail_point!(KEYSPACE_OPEN_FAIL);
            let mut options = keyspace.rocks_configuration(resources);
            CompactionConfiguration::DEFAULT.configure(&mut options);
            keyspaces.keyspaces.push(Keyspace::open(path, keyspace, &options)?);
            keyspaces.index[keyspace.id().0 as usize] = Some(KeyspaceId(keyspaces.keyspaces.len() as u8 - 1));
        }
        Ok(keyspaces)
    }

    fn validate_new_keyspace(&self, keyspace_id: impl KeyspaceSet) -> Result<(), KeyspaceValidationError> {
        use KeyspaceValidationError::{IdExists, IdReserved, IdTooLarge, NameExists};

        let name = keyspace_id.name();

        if keyspace_id.id() == KEYSPACE_ID_RESERVED_UNSET {
            return Err(IdReserved { name, id: keyspace_id.id().0 });
        }

        if keyspace_id.id() > KEYSPACE_ID_MAX {
            return Err(IdTooLarge { name, id: keyspace_id.id().0, max_id: KEYSPACE_ID_MAX.0 });
        }

        for (existing_id, existing_keyspace_index) in self.index.iter().enumerate() {
            if let Some(existing_index) = existing_keyspace_index {
                let keyspace = &self.keyspaces[existing_index.0 as usize];
                if keyspace.name() == name {
                    return Err(NameExists { name });
                }
                if existing_id == keyspace_id.id().0 as usize {
                    return Err(IdExists { new_name: name, id: keyspace_id.id().0, existing_name: keyspace.name() });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn get(&self, keyspace_id: KeyspaceId) -> &Keyspace {
        let keyspace_index = self.index[keyspace_id.0 as usize].unwrap();
        &self.keyspaces[keyspace_index.0 as usize]
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Keyspace> {
        self.keyspaces.iter()
    }

    pub(crate) fn write(&self, write_batches: WriteBatches) -> Result<(), KeyspaceError> {
        for (index, write_batch) in write_batches.into_iter() {
            debug_assert!(index < KEYSPACE_MAXIMUM_COUNT);
            self.get(KeyspaceId(index as u8)).write(write_batch)?;
        }
        Ok(())
    }

    pub(crate) fn checkpoint(&self, current_checkpoint_dir: &Path) -> Result<(), KeyspaceCheckpointError> {
        for keyspace in &self.keyspaces {
            fail_point!(KEYSPACE_CHECKPOINT_FAIL);
            keyspace.checkpoint(current_checkpoint_dir)?;
        }
        Ok(())
    }

    pub(crate) fn delete(self) -> Result<(), Vec<KeyspaceDeleteError>> {
        let errors = self
            .keyspaces
            .into_iter()
            .filter_map(|keyspace| {
                fail_point!(KEYSPACE_DELETE_FAIL);
                keyspace.delete().err()
            })
            .collect_vec();
        if !errors.is_empty() {
            return Err(errors);
        }
        Ok(())
    }

    pub(crate) fn reset(&mut self) -> Result<(), KeyspaceError> {
        for keyspace in self.keyspaces.iter_mut() {
            keyspace.reset()?
        }
        Ok(())
    }

    pub(crate) fn enable_bulk_loading(&self) -> Result<(), KeyspaceError> {
        self.keyspaces.iter().try_for_each(Keyspace::enable_bulk_loading)
    }

    pub(crate) fn finish_bulk_loading(&self) -> Result<CompactionHandle, KeyspaceError> {
        let (finished, receiver) = mpsc::channel();
        for keyspace in &self.keyspaces {
            keyspace.finish_bulk_loading(finished.clone())?;
        }
        Ok(CompactionHandle { receiver })
    }

    pub fn estimate_size_in_bytes(&self) -> Result<u64, KeyspaceError> {
        self.keyspaces.iter().try_fold(0, |total, keyspace| {
            let size = keyspace.estimate_size_in_bytes()?;
            Ok(total + size)
        })
    }

    pub fn estimate_key_count(&self) -> Result<u64, KeyspaceError> {
        self.keyspaces.iter().try_fold(0, |total, keyspace| {
            let count = keyspace.estimate_key_count()?;
            Ok(total + count)
        })
    }
}

#[derive(Debug, Clone)]
pub enum KeyspaceValidationError {
    IdReserved { name: &'static str, id: u8 },
    IdTooLarge { name: &'static str, id: u8, max_id: u8 },
    NameExists { name: &'static str },
    IdExists { new_name: &'static str, id: u8, existing_name: &'static str },
}

impl fmt::Display for KeyspaceValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NameExists { name, .. } => write!(f, "keyspace '{name}' is defined multiple times."),
            Self::IdReserved { name, id, .. } => {
                write!(f, "reserved keyspace id '{id}' cannot be used for new keyspace '{name}'.")
            }
            Self::IdTooLarge { name, id, max_id, .. } => write!(
                f,
                "keyspace id '{id}' cannot be used for new keyspace '{name}' since it is larger than maximum keyspace id '{max_id}'.",
            ),
            Self::IdExists { new_name, id, existing_name, .. } => write!(
                f,
                "keyspace id '{}' cannot be used for new keyspace '{}' since it is already used by keyspace '{}'.",
                id, new_name, existing_name
            ),
        }
    }
}

impl Error for KeyspaceValidationError {}

/// A non-durable key-value store that supports put, get, delete, iterate and checkpointing.
pub(crate) struct Keyspace {
    path: PathBuf,
    name: &'static str,
    id: KeyspaceId,
    pub(super) kv_storage: Arc<DB>,
    read_options: ReadOptions,
    write_options: WriteOptions,
    prefix_length: Option<usize>,
    background_compaction: Mutex<Option<BackgroundCompaction>>,
}

impl Keyspace {
    pub(crate) fn open(
        storage_path: &Path,
        keyspace: impl KeyspaceSet,
        options: &Options,
    ) -> Result<Keyspace, KeyspaceOpenError> {
        use KeyspaceOpenError::RocksDB;
        let name = keyspace.name();
        let path = storage_path.join(name);
        let kv_storage = DB::open(options, &path).map_err(|error| RocksDB { name, source: error })?;
        Ok(Self::new(path, keyspace, kv_storage))
    }

    fn new(path: PathBuf, keyspace: impl KeyspaceSet, kv_storage: DB) -> Self {
        // initial read options, should be customised to this storage's properties
        let read_options = ReadOptions::default();
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        let prefix_length = keyspace.prefix_length();
        Self {
            path,
            name: keyspace.name(),
            id: keyspace.id(),
            background_compaction: Mutex::new(None),
            kv_storage: Arc::new(kv_storage),
            read_options,
            write_options,
            prefix_length,
        }
    }

    pub(super) fn new_read_options(&self) -> ReadOptions {
        let mut options = ReadOptions::default();
        options.set_total_order_seek(true); // Set this to 'false' to use bloom-filters
        options
    }

    pub(crate) fn id(&self) -> KeyspaceId {
        self.id
    }

    pub(crate) fn name(&self) -> &'static str {
        self.name
    }

    pub(crate) fn prefix_length(&self) -> Option<usize> {
        self.prefix_length
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> Result<(), KeyspaceError> {
        self.kv_storage
            .put_opt(key, value, &self.write_options)
            .map_err(|error| KeyspaceError::Put { name: self.name, source: error })
    }

    pub(crate) fn get<M, V>(&self, key: &[u8], mut mapper: M) -> Result<Option<V>, KeyspaceError>
    where
        M: FnMut(&[u8]) -> V,
    {
        self.kv_storage
            .get_pinned_opt(key, &self.read_options)
            .map(|option| option.map(|value| mapper(value.as_ref())))
            .map_err(|error| KeyspaceError::Get { name: self.name, source: error })
    }

    pub(crate) fn get_prev<M, T>(&self, key: &[u8], mut mapper: M) -> Option<T>
    where
        M: FnMut(&[u8], &[u8]) -> T,
    {
        let mut iterator = self.kv_storage.raw_iterator_opt(self.new_read_options());
        iterator.seek_for_prev(key);
        iterator.item().map(|(k, v)| mapper(k, v))
    }

    pub(crate) fn iterate_range<const PREFIX_INLINE_SIZE: usize>(
        &self,
        iterpool: &IteratorPool,
        range: &KeyRange<Bytes<'_, PREFIX_INLINE_SIZE>>,
        storage_counters: StorageCounters,
    ) -> iterator::KeyspaceRangeIterator {
        iterator::KeyspaceRangeIterator::new(self, iterpool, range, storage_counters)
    }

    pub(crate) fn write(&self, write_batch: WriteBatch) -> Result<(), KeyspaceError> {
        self.kv_storage
            .write_opt(write_batch, &self.write_options)
            .map_err(|error| KeyspaceError::BatchWrite { name: self.name, source: error })
    }

    pub(crate) fn checkpoint(&self, checkpoint_dir: &Path) -> Result<(), KeyspaceCheckpointError> {
        use KeyspaceCheckpointError::{CheckpointExists, CreateRocksDBCheckpoint};

        let checkpoint_dir = checkpoint_dir.join(self.name);
        if checkpoint_dir.exists() {
            return Err(CheckpointExists { name: self.name, dir: checkpoint_dir });
        }

        Checkpoint::new(&self.kv_storage)
            .and_then(|checkpoint| checkpoint.create_checkpoint(&checkpoint_dir))
            .map_err(|error| CreateRocksDBCheckpoint { name: self.name, source: error })?;

        Ok(())
    }

    pub(crate) fn delete(self) -> Result<(), KeyspaceDeleteError> {
        drop(self.background_compaction);
        drop(self.kv_storage);
        fs::remove_dir_all(self.path.clone())
            .map_err(|error| KeyspaceDeleteError::DirectoryRemove { name: self.name, source: Arc::new(error) })?;
        Ok(())
    }

    pub(crate) fn enable_bulk_loading(&self) -> Result<(), KeyspaceError> {
        CompactionConfiguration::BULK_LOAD
            .apply(&self.kv_storage)
            .map_err(|source| KeyspaceError::SetOptions { name: self.name, source })
    }

    pub(crate) fn finish_bulk_loading(&self, finished: mpsc::Sender<()>) -> Result<(), KeyspaceError> {
        self.wait_for_background_compaction();
        let kv_storage = self.kv_storage.clone();
        let name = self.name;
        let thread = thread::Builder::new()
            .name(format!("compaction-{name}"))
            .spawn(move || {
                // dropped when the compaction ends, however it ends
                let _finished = finished;
                if let Err(error) = kv_storage.flush() {
                    warn!("Failed to flush keyspace '{name}' while finishing bulk load: {error}");
                }
                kv_storage.compact_range(None::<&[u8]>, None::<&[u8]>);
                if let Err(error) = CompactionConfiguration::DEFAULT.apply(&kv_storage) {
                    warn!("Failed to restore the options of keyspace '{name}' while finishing bulk load: {error}");
                }
            })
            .map_err(|source| KeyspaceError::CompactionStart { name, source: Arc::new(source) })?;
        *self.background_compaction.lock().unwrap() =
            Some(BackgroundCompaction { kv_storage: self.kv_storage.clone(), thread: Some(thread) });
        Ok(())
    }

    fn wait_for_background_compaction(&self) {
        if let Some(background_compaction) = self.background_compaction.lock().unwrap().take() {
            background_compaction.wait();
        }
    }

    pub(crate) fn reset(&mut self) -> Result<(), KeyspaceError> {
        let iterator = self.kv_storage.iterator(IteratorMode::Start);
        for entry in iterator {
            let (key, _) = entry.map_err(|err| KeyspaceError::Iterate { name: self.name, source: err })?;
            self.kv_storage.delete(key).map_err(|err| KeyspaceError::Iterate { name: self.name, source: err })?;
        }
        Ok(())
    }

    pub fn estimate_size_in_bytes(&self) -> Result<u64, KeyspaceError> {
        let property_name = constants::rocksdb::PROPERTY_ESTIMATE_LIVE_DATA_SIZE;
        self.kv_storage
            .property_int_value(property_name)
            .map_err(|source| KeyspaceError::Property { name: property_name, source })
            .map(|result_opt| result_opt.unwrap_or(0))
    }

    pub fn estimate_key_count(&self) -> Result<u64, KeyspaceError> {
        let property_name = constants::rocksdb::PROPERTY_ESTIMATE_NUM_KEYS;
        self.kv_storage
            .property_int_value(property_name)
            .map_err(|source| KeyspaceError::Property { name: property_name, source })
            .map(|result_opt| result_opt.unwrap_or(0))
    }

    #[cfg(not(debug_assertions))]
    pub(crate) fn benchmark_only_flush(&self) {
        self.kv_storage.flush().expect("This is test only code. We unwrap")
    }
}

struct BackgroundCompaction {
    kv_storage: Arc<DB>,
    thread: Option<JoinHandle<()>>,
}

impl BackgroundCompaction {
    fn wait(mut self) {
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                warn!("The background compaction of a keyspace panicked.");
            }
        }
    }
}

impl Drop for BackgroundCompaction {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            if !thread.is_finished() {
                self.kv_storage.cancel_all_background_work(true);
            }
            let _ = thread.join();
        }
    }
}

#[derive(Debug)]
pub struct CompactionHandle {
    receiver: mpsc::Receiver<()>,
}

impl CompactionHandle {
    pub fn wait(self) {
        let _ = self.receiver.recv();
    }
}

impl fmt::Debug for Keyspace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Keyspace[name={}, path={:?}, id={}]", self.name, self.path, self.id)
    }
}

#[derive(Debug, Clone)]
pub enum KeyspaceOpenError {
    RocksDB { name: &'static str, source: rocksdb::Error },
    Validation { source: KeyspaceValidationError },
}

impl fmt::Display for KeyspaceOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl Error for KeyspaceOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RocksDB { source, .. } => Some(source),
            Self::Validation { source, .. } => Some(source),
        }
    }
}

#[derive(Debug, Clone)]
pub enum KeyspaceCheckpointError {
    CheckpointExists { name: &'static str, dir: PathBuf },
    CreateRocksDBCheckpoint { name: &'static str, source: rocksdb::Error },
}

impl fmt::Display for KeyspaceCheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl Error for KeyspaceCheckpointError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CheckpointExists { .. } => None,
            Self::CreateRocksDBCheckpoint { source, .. } => Some(source),
        }
    }
}

#[derive(Debug, Clone)]
pub enum KeyspaceDeleteError {
    DirectoryRemove { name: &'static str, source: Arc<io::Error> },
}

impl fmt::Display for KeyspaceDeleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl Error for KeyspaceDeleteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self {
            Self::DirectoryRemove { source, .. } => Some(source),
        }
    }
}

#[derive(Clone, Debug)]
pub enum KeyspaceError {
    Get { name: &'static str, source: rocksdb::Error },
    Put { name: &'static str, source: rocksdb::Error },
    BatchWrite { name: &'static str, source: rocksdb::Error },
    Iterate { name: &'static str, source: rocksdb::Error },
    DeleteRange { name: &'static str, source: rocksdb::Error },
    Property { name: &'static str, source: rocksdb::Error },
    SetOptions { name: &'static str, source: rocksdb::Error },
    CompactionStart { name: &'static str, source: Arc<io::Error> },
}

impl fmt::Display for KeyspaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl Error for KeyspaceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self {
            Self::Get { source, .. } => Some(source),
            Self::Put { source, .. } => Some(source),
            Self::BatchWrite { source, .. } => Some(source),
            Self::Iterate { source, .. } => Some(source),
            Self::DeleteRange { source, .. } => Some(source),
            Self::Property { source, .. } => Some(source),
            Self::SetOptions { source, .. } => Some(source),
            Self::CompactionStart { source, .. } => Some(source),
        }
    }
}
