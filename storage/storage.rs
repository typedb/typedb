/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]
#![allow(clippy::module_inception)]

use std::{
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
    thread::sleep,
    time::Duration,
};

use ::error::typedb_error;
use bytes::{byte_array::ByteArray, Bytes};
use isolation_manager::IsolationConflict;
use iterator::MVCCReadError;
use keyspace::KeyspaceDeleteError;
use lending_iterator::LendingIterator;
use logger::{error, result::ResultExt};
use resource::{
    constants::{snapshot::BUFFER_VALUE_INLINE, storage::WATERMARK_WAIT_INTERVAL_MICROSECONDS},
    profile::{CommitProfile, StorageCounters},
};
use tracing::trace;

use crate::{
    durability_client::{DurabilityClient, DurabilityClientError},
    error::{MVCCStorageError, MVCCStorageErrorKind},
    isolation_manager::{CommitRecord, IsolationManager, StatusRecord, ValidatedCommit},
    iterator::MVCCRangeIterator,
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyReference},
    keyspace::{
        iterator::KeyspaceRangeIterator, IteratorPool, Keyspace, KeyspaceError, KeyspaceId, KeyspaceOpenError,
        KeyspaceSet, Keyspaces,
    },
    recovery::{
        checkpoint::{Checkpoint, CheckpointCreateError, CheckpointLoadError},
        commit_recovery::{apply_recovered, load_commit_data_from, StorageRecoveryError},
    },
    sequence_number::SequenceNumber,
    snapshot::{write::Write, CommittableSnapshot, ReadSnapshot, SchemaSnapshot, WriteSnapshot},
};

pub mod durability_client;
pub mod error;
pub mod isolation_manager;
pub mod iterator;
pub mod key_range;
pub mod key_value;
pub mod keyspace;
pub mod recovery;
pub mod sequence_number;
pub mod snapshot;
mod write_batches;

#[derive(Debug)]
pub struct MVCCStorage<Durability> {
    name: Arc<String>,
    path: PathBuf,
    keyspaces: Keyspaces,
    durability_client: Durability,
    isolation_manager: IsolationManager,
}

impl<Durability> MVCCStorage<Durability> {
    pub const STORAGE_DIR_NAME: &'static str = "storage";

    pub fn create<KS: KeyspaceSet>(
        name: impl AsRef<str>,
        path: &Path,
        mut durability_client: Durability,
    ) -> Result<Self, StorageOpenError>
    where
        Durability: DurabilityClient,
    {
        let storage_dir = path.join(Self::STORAGE_DIR_NAME);

        if storage_dir.exists() {
            return Err(StorageOpenError::StorageDirectoryExists { name: name.as_ref().to_owned(), path: storage_dir });
        }
        fs::create_dir_all(&storage_dir).map_err(|error| StorageOpenError::StorageDirectoryCreate {
            name: name.as_ref().to_owned(),
            source: Arc::new(error),
        })?;
        Self::register_durability_record_types(&mut durability_client);
        let keyspaces = Self::create_keyspaces::<KS>(name.as_ref(), &storage_dir)?;

        let isolation_manager = IsolationManager::new(durability_client.current());
        Ok(Self {
            name: Arc::new(name.as_ref().to_owned()),
            path: storage_dir,
            durability_client,
            keyspaces,
            isolation_manager,
        })
    }

    fn create_keyspaces<KS: KeyspaceSet>(
        name: impl AsRef<str>,
        storage_dir: &Path,
    ) -> Result<Keyspaces, StorageOpenError> {
        let keyspaces = Keyspaces::open::<KS>(&storage_dir)
            .map_err(|err| StorageOpenError::KeyspaceOpen { name: name.as_ref().to_owned(), source: err })?;
        Ok(keyspaces)
    }

    pub fn load<KS: KeyspaceSet>(
        name: impl AsRef<str>,
        path: &Path,
        mut durability_client: Durability,
        checkpoint: &Option<Checkpoint>,
    ) -> Result<Self, StorageOpenError>
    where
        Durability: DurabilityClient,
    {
        use StorageOpenError::{RecoverFromCheckpoint, RecoverFromDurability, StorageDirectoryRecreate};

        let name = name.as_ref();
        let storage_dir = path.join(Self::STORAGE_DIR_NAME);

        Self::register_durability_record_types(&mut durability_client);
        let (keyspaces, next_sequence_number) = match checkpoint {
            None => {
                fs::remove_dir_all(&storage_dir)
                    .map_err(|err| StorageDirectoryRecreate { name: name.to_owned(), source: Arc::new(err) })?;
                fs::create_dir_all(&storage_dir)
                    .map_err(|err| StorageDirectoryRecreate { name: name.to_owned(), source: Arc::new(err) })?;
                let keyspaces = Self::create_keyspaces::<KS>(name, &storage_dir)?;
                trace!("No checkpoint found, loading from WAL");
                let commits = load_commit_data_from(SequenceNumber::MIN.next(), &durability_client, usize::MAX)
                    .map_err(|err| RecoverFromDurability { name: name.to_owned(), typedb_source: err })?;
                let next_sequence_number = commits.keys().max().cloned().unwrap_or(SequenceNumber::MIN).next();
                apply_recovered(commits, &durability_client, &keyspaces)
                    .map_err(|err| RecoverFromDurability { name: name.to_owned(), typedb_source: err })?;
                trace!("Finished applying commits from WAL.");
                (keyspaces, next_sequence_number)
            }
            Some(checkpoint) => checkpoint
                .recover_storage::<KS, _>(&storage_dir, &durability_client)
                .map_err(|error| RecoverFromCheckpoint { name: name.to_owned(), typedb_source: error })?,
        };

        let isolation_manager = IsolationManager::new(next_sequence_number);
        Ok(Self { name: Arc::new(name.to_owned()), path: storage_dir, durability_client, keyspaces, isolation_manager })
    }

    fn register_durability_record_types(durability_client: &mut impl DurabilityClient) {
        durability_client.register_record_type::<CommitRecord>();
        durability_client.register_record_type::<StatusRecord>();
    }

    fn name(&self) -> Arc<String> {
        self.name.clone()
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn durability(&self) -> &Durability {
        &self.durability_client
    }

    pub fn durability_mut(&mut self) -> &mut Durability {
        &mut self.durability_client
    }

    pub fn open_snapshot_write(self: Arc<Self>) -> WriteSnapshot<Durability> {
        // guarantee external consistency: we always await the latest snapshots to finish
        let possible_sequence_number = self.isolation_manager.highest_validated_sequence_number();
        let open_sequence_number = self.wait_for_watermark(possible_sequence_number);
        WriteSnapshot::new(self, open_sequence_number)
    }

    pub fn open_snapshot_write_at(self: Arc<Self>, sequence_number: SequenceNumber) -> WriteSnapshot<Durability> {
        // guarantee external consistency: await this sequence number to be behind the watermark
        self.wait_for_watermark(sequence_number);
        WriteSnapshot::new(self, sequence_number)
    }

    pub fn open_snapshot_read(self: Arc<Self>) -> ReadSnapshot<Durability> {
        // guarantee external consistency: we always await the latest snapshots to finish
        let possible_sequence_number = self.isolation_manager.highest_validated_sequence_number();
        let open_sequence_number = self.wait_for_watermark(possible_sequence_number);
        ReadSnapshot::new(self, open_sequence_number)
    }

    pub fn open_snapshot_read_at(self: Arc<Self>, sequence_number: SequenceNumber) -> ReadSnapshot<Durability> {
        self.wait_for_watermark(sequence_number);
        ReadSnapshot::new(self, sequence_number)
    }

    pub fn open_snapshot_schema(self: Arc<Self>) -> SchemaSnapshot<Durability> {
        // guarantee external consistency: we always await the latest snapshots to finish
        let possible_sequence_number = self.isolation_manager.highest_validated_sequence_number();
        let open_sequence_number = self.wait_for_watermark(possible_sequence_number);
        SchemaSnapshot::new(self, open_sequence_number)
    }

    fn wait_for_watermark(&self, target: SequenceNumber) -> SequenceNumber {
        // We can alternatively also block commits from returning until the watermark rises
        // See detailed analysis at https://github.com/typedb/typedb/pull/7254/
        let mut watermark = self.snapshot_watermark();
        while watermark < target {
            sleep(Duration::from_micros(WATERMARK_WAIT_INTERVAL_MICROSECONDS));
            watermark = self.snapshot_watermark();
        }
        watermark
    }

    fn snapshot_commit(
        &self,
        snapshot: impl CommittableSnapshot<Durability>,
        commit_profile: &mut CommitProfile,
    ) -> Result<SequenceNumber, StorageCommitError>
    where
        Durability: DurabilityClient,
    {
        use StorageCommitError::{Durability, Internal, Keyspace, MVCCRead};

        self.set_initial_put_status(&snapshot, commit_profile.storage_counters())
            .map_err(|error| MVCCRead { name: self.name.clone(), source: error })?;
        commit_profile.snapshot_put_statuses_checked();

        let commit_record = snapshot.into_commit_record();
        commit_profile.snapshot_commit_record_created();

        let commit_sequence_number = self
            .durability_client
            .sequenced_write(&commit_record)
            .map_err(|error| Durability { name: self.name.clone(), typedb_source: error })?;
        commit_profile.snapshot_durable_write_data_submitted();

        let sync_notifier = self.durability_client.request_sync();
        let validate_result =
            self.isolation_manager.validate_commit(commit_sequence_number, commit_record, &self.durability_client);
        commit_profile.snapshot_isolation_validated();

        match validate_result {
            Ok(ValidatedCommit::Write(write_batches)) => {
                sync_notifier.recv().unwrap(); // Ensure WAL is persisted before inserting to the KV store
                                               // Write to the k-v store
                commit_profile.snapshot_durable_write_data_confirmed();

                self.keyspaces
                    .write(write_batches)
                    .map_err(|error| Keyspace { name: self.name.clone(), source: Arc::new(error) })?;
                commit_profile.snapshot_storage_written();

                // Inform the isolation manager and increment the watermark
                self.isolation_manager
                    .applied(commit_sequence_number)
                    .map_err(|error| Internal { name: self.name.clone(), source: Arc::new(error) })?;
                commit_profile.snapshot_isolation_manager_notified();

                Self::persist_commit_status(true, commit_sequence_number, &self.durability_client)
                    .map_err(|error| Durability { name: self.name.clone(), typedb_source: error })?;
                commit_profile.snapshot_durable_write_commit_status_submitted();

                Ok(commit_sequence_number)
            }
            Ok(ValidatedCommit::Conflict(conflict)) => {
                sync_notifier.recv().unwrap();
                commit_profile.snapshot_durable_write_data_confirmed();

                Self::persist_commit_status(false, commit_sequence_number, &self.durability_client)
                    .map_err(|error| Durability { name: self.name.clone(), typedb_source: error })?;
                commit_profile.snapshot_durable_write_commit_status_submitted();
                Err(StorageCommitError::Isolation { name: self.name.clone(), conflict })
            }
            Err(error) => {
                sync_notifier.recv().unwrap();
                commit_profile.snapshot_durable_write_data_confirmed();
                Err(Durability { name: self.name.clone(), typedb_source: error })
            }
        }
    }

    fn set_initial_put_status(
        &self,
        snapshot: &impl CommittableSnapshot<Durability>,
        storage_counters: StorageCounters,
    ) -> Result<(), MVCCReadError>
    where
        Durability: DurabilityClient,
    {
        for buffer in snapshot.operations() {
            let writes = buffer.writes();
            let puts = writes.iter().filter_map(|(key, write)| match write {
                Write::Put { value, reinsert, known_to_exist } => Some((key, value, reinsert, *known_to_exist)),
                _ => None,
            });
            for (key, value, reinsert, known_to_exist) in puts {
                let wrapped = StorageKeyReference::new_raw(buffer.keyspace_id, key);
                if known_to_exist {
                    debug_assert!(self
                        .get::<0>(
                            snapshot.iterator_pool(),
                            wrapped,
                            snapshot.open_sequence_number(),
                            storage_counters.clone()
                        )
                        .is_ok_and(|opt| opt.is_some()));
                    reinsert.store(false, Ordering::Release);
                } else {
                    let existing_stored = self
                        .get::<BUFFER_VALUE_INLINE>(
                            snapshot.iterator_pool(),
                            wrapped,
                            snapshot.open_sequence_number(),
                            storage_counters.clone(),
                        )?
                        .is_some_and(|reference| &reference == value);
                    reinsert.store(!existing_stored, Ordering::Release);
                }
            }
        }
        Ok(())
    }

    fn persist_commit_status(
        did_apply: bool,
        commit_sequence_number: SequenceNumber,
        durability_client: &Durability,
    ) -> Result<(), DurabilityClientError>
    where
        Durability: DurabilityClient,
    {
        durability_client.unsequenced_write(&StatusRecord::new(commit_sequence_number, did_apply))?;
        Ok(())
    }

    pub fn closed_snapshot_write(&self, open_sequence_number: SequenceNumber) {
        self.isolation_manager.closed_for_read(open_sequence_number)
    }

    fn get_keyspace(&self, keyspace_id: KeyspaceId) -> &Keyspace {
        self.keyspaces.get(keyspace_id)
    }

    pub fn checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointCreateError> {
        checkpoint.add_storage(&self.keyspaces, self.snapshot_watermark())
    }

    pub fn delete_storage(self) -> Result<(), StorageDeleteError>
    where
        Durability: DurabilityClient,
    {
        use StorageDeleteError::{DirectoryDelete, DurabilityDelete, KeyspaceDelete};

        self.keyspaces.delete().map_err(|errs| KeyspaceDelete { name: self.name.clone(), errors: errs })?;

        self.durability_client
            .delete_durability()
            .map_err(|err| DurabilityDelete { name: self.name.clone(), typedb_source: err })?;

        if self.path.exists() {
            std::fs::remove_dir_all(&self.path).map_err(|error| {
                error!("Failed to delete storage {}, received error: {}", self.name, error);
                DirectoryDelete { name: self.name.clone(), source: Arc::new(error) }
            })?;
        }

        Ok(())
    }

    pub fn get<'a, const INLINE_BYTES: usize>(
        &self,
        iterator_pool: &IteratorPool,
        key: impl Into<StorageKeyReference<'a>>,
        open_sequence_number: SequenceNumber,
        storage_counters: StorageCounters,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, MVCCReadError> {
        self.get_mapped(
            iterator_pool,
            key,
            open_sequence_number,
            |byte_ref| ByteArray::from(byte_ref),
            storage_counters,
        )
    }

    pub fn get_mapped<'a, 'pool, Mapper, V>(
        &self,
        iterator_pool: &IteratorPool,
        key: impl Into<StorageKeyReference<'a>>,
        open_sequence_number: SequenceNumber,
        mapper: Mapper,
        storage_counters: StorageCounters,
    ) -> Result<Option<V>, MVCCReadError>
    where
        Mapper: Fn(&[u8]) -> V,
    {
        let key = key.into();
        let mut iterator = self.iterate_range(
            iterator_pool,
            &KeyRange::new_within(StorageKey::<0>::Reference(key), false),
            open_sequence_number,
            storage_counters,
        );
        loop {
            match iterator.next().transpose()? {
                None => return Ok(None),
                Some((k, v)) if k.bytes() == key.bytes() => return Ok(Some(mapper(v))),
                Some(_) => (),
            }
        }
    }

    pub(crate) fn iterate_range<'this, const PS: usize>(
        &'this self,
        iterpool: &IteratorPool,
        range: &KeyRange<StorageKey<'this, PS>>,
        open_sequence_number: SequenceNumber,
        storage_counters: StorageCounters,
    ) -> MVCCRangeIterator {
        MVCCRangeIterator::new(self, iterpool, range, open_sequence_number, storage_counters)
    }

    pub fn snapshot_watermark(&self) -> SequenceNumber {
        self.isolation_manager.watermark()
    }

    // --- direct access to storage, bypassing MVCC and returning raw key/value pairs ---

    pub fn put_raw(&self, key: StorageKeyReference<'_>, value: &Bytes<'_, BUFFER_VALUE_INLINE>) {
        // TODO: writes should always have to go through a transaction? Otherwise we have to WAL right here in a different path
        self.keyspaces
            .get(key.keyspace_id())
            .put(key.bytes(), value)
            .map_err(|e| MVCCStorageError {
                storage_name: self.name(),
                kind: MVCCStorageErrorKind::KeyspaceError {
                    source: Arc::new(e),
                    keyspace_name: self.keyspaces.get(key.keyspace_id()).name(),
                },
            })
            .unwrap_or_log()
    }

    pub fn get_raw_mapped<M, V>(&self, key: StorageKeyReference<'_>, mut mapper: M) -> Option<V>
    where
        M: FnMut(&[u8]) -> V,
    {
        self.keyspaces
            .get(key.keyspace_id())
            .get(key.bytes(), |value| mapper(value))
            .map_err(|e| MVCCStorageError {
                storage_name: self.name(),
                kind: MVCCStorageErrorKind::KeyspaceError {
                    source: Arc::new(e),
                    keyspace_name: self.keyspaces.get(key.keyspace_id()).name(),
                },
            })
            .unwrap_or_log() // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
    }

    pub fn get_prev_raw<M, T>(&self, key: StorageKeyReference<'_>, mut key_value_mapper: M) -> Option<T>
    where
        M: FnMut(&MVCCKey<'_>, &[u8]) -> T,
    {
        self.keyspaces
            .get(key.keyspace_id())
            .get_prev(key.bytes(), |raw_key, v| key_value_mapper(&MVCCKey::wrap_slice(raw_key), v))
    }

    pub fn iterate_keyspace_range<'this, const PREFIX_INLINE: usize>(
        &'this self,
        iterator_pool: &IteratorPool,
        range: KeyRange<StorageKey<'this, PREFIX_INLINE>>,
        storage_counters: StorageCounters,
    ) -> KeyspaceRangeIterator {
        self.keyspaces.get(range.start().get_value().keyspace_id()).iterate_range(
            iterator_pool,
            &range.map(|k| k.as_bytes(), |fixed| fixed),
            storage_counters,
        )
    }

    pub fn reset(&mut self) -> Result<(), StorageResetError>
    where
        Durability: DurabilityClient,
    {
        self.isolation_manager.reset();
        self.keyspaces
            .reset()
            .map_err(|err| StorageResetError::KeyspaceError { name: self.name.clone(), source: err })?;
        self.durability_client
            .reset()
            .map_err(|err| StorageResetError::Durability { name: self.name.clone(), typedb_source: err })?;
        Ok(())
    }

    pub fn estimate_size_in_bytes(&self) -> Result<u64, StorageOpenError> {
        self.keyspaces.estimate_size_in_bytes().map_err(|source| StorageOpenError::Keyspace { source })
    }

    pub fn estimate_key_count(&self) -> Result<u64, StorageOpenError> {
        self.keyspaces.estimate_key_count().map_err(|source| StorageOpenError::Keyspace { source })
    }
}

typedb_error! {
    pub StorageOpenError(component = "Storage open", prefix = "STO") {
        StorageDirectoryExists(1, "Failed to open database '{name}' in directory '{path:?}'", name: String, path: PathBuf),
        StorageDirectoryCreate(2, "Failed to create database '{name}'.", name: String, source: Arc<io::Error>),
        StorageDirectoryRecreate(3, "Failed to recreate database '{name}'.", name: String, source: Arc<io::Error>),

        DurabilityClientOpen(4, "Failed to open durability client for database '{name}'.", name: String, source: Arc<io::Error>),
        DurabilityClientRead(5, "Failed to read from durability client for database '{name}'.", name: String, typedb_source: DurabilityClientError),
        DurabilityClientWrite(6, "Failed to write to durability client for database '{name}'.", name: String, typedb_source: DurabilityClientError),

        KeyspaceOpen(7, "Failed to open keyspace '{name}'.", name: String, source: KeyspaceOpenError),
        Keyspace(8, "Failed to operate with keyspaces.", source: KeyspaceError),

        CheckpointCreate(9, "Failed to create checkpoint for database '{name}'.", name: String, source: CheckpointCreateError),

        RecoverFromCheckpoint(10, "Failed to recover from checkpoint for database '{name}'.", name: String, typedb_source: CheckpointLoadError),
        RecoverFromDurability(11, "Failed to recover from durability logs for database '{name}'.", name: String, typedb_source: StorageRecoveryError),
    }
}

typedb_error! {
    pub StorageCommitError(component = "Storage commit", prefix = "STC") {
        Internal(1, "Commit in database '{name}' failed with internal error.", name: Arc<String>, source: Arc<dyn Error + Send + Sync + 'static>),
        Isolation(2, "Commit in database '{name}' failed with isolation conflict: {conflict}", name: Arc<String>, conflict: IsolationConflict),
        IO(3, "Commit in database '{name}' failed with I/O error'.", name: Arc<String>, source: Arc<io::Error>),
        MVCCRead(4, "Commit in database '{name}' failed due to failed read from MVCC storage layer.", name: Arc<String>, source: MVCCReadError),
        Keyspace(5, "Commit in database '{name}' failed due to a storage keyspace error.", name: Arc<String>, source: Arc<KeyspaceError>),
        Durability(6, "Commit in database '{name}' failed due to error in durability client.", name: Arc<String>, typedb_source: DurabilityClientError),
    }
}

typedb_error! {
    pub StorageDeleteError(component = "Storage delete", prefix = "STD") {
        DurabilityDelete(1, "Deleting storage of database '{name}' failed partway while deleting durability records.", name: Arc<String>, typedb_source: DurabilityClientError),
        KeyspaceDelete(2, "Deleting storage of database '{name}' failed partway while deleting keyspaces: {errors:?}", name: Arc<String>, errors: Vec<KeyspaceDeleteError>),
        DirectoryDelete(3, "Deleting storage of database '{name}' failed partway while deleting directory.", name: Arc<String>, source: Arc<io::Error>),
    }
}

typedb_error! {
    pub StorageResetError(component = "Storage reset", prefix = "STR") {
        KeyspaceError(1, "Resetting storage of database '{name}' failed partway while resetting keyspace.", name: Arc<String>, source: KeyspaceError),
        Durability(2, "Resetting storage of database '{name}' failed partway while resetting durability records.", name: Arc<String>, typedb_source: DurabilityClientError),
    }
}

/// MVCC keys are made of three parts: the [KEY][SEQ][OP]
pub struct MVCCKey<'bytes> {
    bytes: Bytes<'bytes, MVCC_KEY_INLINE_SIZE>,
}

// byte array inline size can be adjusted to avoid allocation since these key are often short-lived
const MVCC_KEY_INLINE_SIZE: usize = 128;

impl<'bytes> MVCCKey<'bytes> {
    const OPERATION_START_NEGATIVE_OFFSET: usize = StorageOperation::serialised_len();
    const SEQUENCE_NUMBER_START_NEGATIVE_OFFSET: usize =
        Self::OPERATION_START_NEGATIVE_OFFSET + SequenceNumber::serialised_len();

    fn build(key: &[u8], sequence_number: SequenceNumber, storage_operation: StorageOperation) -> Self {
        let length = key.len() + SequenceNumber::serialised_len() + StorageOperation::serialised_len();
        let mut byte_array = ByteArray::zeros(length);
        let bytes = &mut *byte_array;

        let key_end = key.len();
        let sequence_number_end = key_end + SequenceNumber::serialised_len();
        let operation_end = sequence_number_end + StorageOperation::serialised_len();

        bytes[0..key_end].copy_from_slice(key);
        sequence_number.invert().serialise_be_into(&mut bytes[key_end..sequence_number_end]);
        bytes[sequence_number_end..operation_end].copy_from_slice(storage_operation.bytes());

        Self { bytes: Bytes::Array(byte_array) }
    }

    fn wrap_slice(bytes: &'bytes [u8]) -> Self {
        Self { bytes: Bytes::reference(bytes) }
    }

    pub(crate) fn is_visible_to(&self, sequence_number: SequenceNumber) -> bool {
        self.sequence_number() <= sequence_number
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn length(&self) -> usize {
        self.bytes.length()
    }

    pub fn key(&self) -> &[u8] {
        &self.bytes()[0..(self.length() - Self::SEQUENCE_NUMBER_START_NEGATIVE_OFFSET)]
    }

    fn into_key(self) -> Bytes<'bytes, MVCC_KEY_INLINE_SIZE> {
        let end = self.length() - Self::SEQUENCE_NUMBER_START_NEGATIVE_OFFSET;
        let bytes = self.bytes;
        bytes.truncate(end)
    }

    fn sequence_number(&self) -> SequenceNumber {
        let sequence_number_start = self.length() - Self::SEQUENCE_NUMBER_START_NEGATIVE_OFFSET;
        let sequence_number_end = sequence_number_start + SequenceNumber::serialised_len();
        let inverse_sequence_number_bytes = &self.bytes()[sequence_number_start..sequence_number_end];
        debug_assert_eq!(SequenceNumber::serialised_len(), inverse_sequence_number_bytes.len());
        SequenceNumber::from_be_bytes(inverse_sequence_number_bytes).invert()
    }

    fn operation(&self) -> StorageOperation {
        let operation_byte_offset = self.length() - Self::OPERATION_START_NEGATIVE_OFFSET;
        let operation_byte_end = operation_byte_offset + StorageOperation::serialised_len();
        StorageOperation::from(&self.bytes()[operation_byte_offset..operation_byte_end])
    }
}

enum StorageOperation {
    Insert,
    Delete,
}

impl StorageOperation {
    const BYTES: usize = 1;

    const fn bytes(&self) -> &[u8; Self::BYTES] {
        match self {
            Self::Insert => &[0x0],
            Self::Delete => &[0x1],
        }
    }

    const fn from(bytes: &[u8]) -> Self {
        match bytes {
            [0x0] => Self::Insert,
            [0x1] => Self::Delete,
            _ => panic!("Unrecognised storage operation bytes."),
        }
    }

    const fn serialised_len() -> usize {
        Self::BYTES
    }
}

#[cfg(test)]
mod tests {
    use bytes::byte_array::ByteArray;
    use durability::wal::WAL;
    use resource::profile::StorageCounters;
    use test_utils::{create_tmp_dir, init_logging};

    use crate::{
        durability_client::{DurabilityClient, WALClient},
        isolation_manager::{CommitRecord, CommitType},
        key_value::StorageKeyArray,
        keyspace::{IteratorPool, KeyspaceId, KeyspaceSet, Keyspaces},
        snapshot::buffer::OperationsBuffer,
        write_batches::WriteBatches,
        MVCCStorage,
    };

    macro_rules! test_keyspace_set {
        {$($variant:ident => $id:literal : $name: literal),* $(,)?} => {
            #[derive(Clone, Copy)]
            enum TestKeyspaceSet { $($variant),* }
            impl KeyspaceSet for TestKeyspaceSet {
                fn iter() -> impl Iterator<Item = Self> { [$(Self::$variant),*].into_iter() }
                fn id(&self) -> KeyspaceId {
                    match *self { $(Self::$variant => KeyspaceId($id)),* }
                }
                fn name(&self) -> &'static str {
                    match *self { $(Self::$variant => $name),* }
                }
            }
        };
    }

    #[test]
    fn test_recovery_from_partial_write() {
        test_keyspace_set! {
            PersistedKeyspace => 0: "write",
            FailedKeyspace => 1: "failed",
        }

        init_logging();
        let storage_path = create_tmp_dir();
        let key_1 = StorageKeyArray::from((TestKeyspaceSet::PersistedKeyspace, b"hello"));
        let key_2 = StorageKeyArray::from((TestKeyspaceSet::FailedKeyspace, b"world"));

        let seq = {
            let mut full_operations = OperationsBuffer::new();
            full_operations.writes_in_mut(key_1.keyspace_id()).insert(key_1.byte_array().clone(), ByteArray::empty());
            full_operations.writes_in_mut(key_2.keyspace_id()).insert(key_2.byte_array().clone(), ByteArray::empty());

            let mut partial_operations = OperationsBuffer::new();
            partial_operations
                .writes_in_mut(key_1.keyspace_id())
                .insert(key_1.byte_array().clone(), ByteArray::empty());

            let mut durability_client = WALClient::new(WAL::create(storage_path.join(WAL::WAL_DIR_NAME)).unwrap());
            durability_client.register_record_type::<CommitRecord>();
            let seq = durability_client
                .sequenced_write(&CommitRecord::new(full_operations, durability_client.previous(), CommitType::Data))
                .unwrap();

            let partial_commit = WriteBatches::from_operations(seq, &partial_operations);
            let keyspaces =
                Keyspaces::open::<TestKeyspaceSet>(storage_path.join(MVCCStorage::<WALClient>::STORAGE_DIR_NAME))
                    .unwrap();
            keyspaces.write(partial_commit).unwrap();

            /* CRASH */

            seq
        };

        let mut durability_client = WALClient::new(WAL::load(storage_path.join(WAL::WAL_DIR_NAME)).unwrap());
        durability_client.register_record_type::<CommitRecord>();
        let storage =
            MVCCStorage::<WALClient>::load::<TestKeyspaceSet>("storage", &storage_path, durability_client, &None)
                .unwrap();
        assert_eq!(
            storage.get::<0>(&IteratorPool::new(), &key_2, seq, StorageCounters::DISABLED).unwrap().unwrap(),
            ByteArray::empty()
        );
    }
}
