/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]
#![allow(dead_code)]
#![allow(clippy::module_inception)]

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    fs::{self, File},
    io::{self, Write as _},
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use chrono::Utc;
use durability::{DurabilityError, DurabilityRecord, DurabilityService, RawRecord, SequenceNumber};
use isolation_manager::IsolationConflict;
use iterator::MVCCReadError;
use itertools::Itertools;
use logger::{error, result::ResultExt};
use resource::constants::snapshot::BUFFER_VALUE_INLINE;

use crate::{
    error::{MVCCStorageError, MVCCStorageErrorKind},
    isolation_manager::{CommitRecord, IsolationManager, StatusRecord},
    iterator::MVCCRangeIterator,
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyReference},
    keyspace::{
        iterator::KeyspaceRangeIterator, Keyspace, KeyspaceCheckpointError, KeyspaceError, KeyspaceId,
        KeyspaceOpenError, KeyspaceSet, KeyspaceValidationError, Keyspaces,
    },
    snapshot::{
        buffer::OperationsBuffer, write::Write, CommittableSnapshot, ReadSnapshot, SchemaSnapshot, WriteSnapshot,
    },
    write_batches::WriteBatches,
};

pub mod error;
pub mod isolation_manager;
pub mod iterator;
pub mod key_range;
pub mod key_value;
pub mod keyspace;
pub mod snapshot;
mod write_batches;

#[derive(Debug)]
pub struct MVCCStorage<Durability> {
    name: String,
    path: PathBuf,
    keyspaces: Keyspaces,
    durability_service: Durability,
    isolation_manager: IsolationManager,
}

impl<Durability> MVCCStorage<Durability> {
    const WAL_DIR_NAME: &'static str = "wal";
    const STORAGE_DIR_NAME: &'static str = "storage";
    const CHECKPOINT_DIR_NAME: &'static str = "checkpoint";
    const CHECKPOINT_METADATA_FILE_NAME: &'static str = "METADATA";

    pub fn open<KS: KeyspaceSet>(name: impl AsRef<str>, path: &Path) -> Result<Self, StorageOpenError>
    where
        Durability: DurabilityService,
    {
        use StorageOpenError::{Commit, DurabilityServiceWrite, MetadataRead};

        let storage_dir = path.join(Self::STORAGE_DIR_NAME);
        if !storage_dir.exists() {
            fs::create_dir_all(&storage_dir).map_err(|_error| todo!())?;
        }

        // FIXME proper error
        let mut durability_service =
            Durability::open(path.join(Self::WAL_DIR_NAME)).expect("Could not create WAL directory");
        durability_service.register_record_type::<CommitRecord>();
        durability_service.register_record_type::<StatusRecord>();

        let name = name.as_ref();
        let keyspaces = Keyspaces::open::<KS>(&storage_dir)?;

        let checkpoint_dir = path.join(Self::CHECKPOINT_DIR_NAME);

        let checkpoint_sequence_number = {
            if let Some(latest_checkpoint_dir) = find_latest_checkpoint(&checkpoint_dir)? {
                let metadata_file_path = latest_checkpoint_dir.join(Self::CHECKPOINT_METADATA_FILE_NAME);
                let metadata = fs::read_to_string(metadata_file_path)
                    .map_err(|error| MetadataRead { dir: checkpoint_dir.clone(), source: error })?;
                SequenceNumber::new(metadata.parse().unwrap()) // FIXME corrupt METADATA handling
            } else {
                SequenceNumber::new(1)
            }
        };

        let (isolation_manager, pending_commits) =
            recover_isolation(name, checkpoint_sequence_number, &durability_service)
                .map_err(|error| Commit { source: error })?;

        for (commit_sequence_number, commit_record) in pending_commits {
            let conflict = Self::try_apply_commit(
                name,
                commit_sequence_number,
                commit_record,
                &keyspaces,
                &isolation_manager,
                &durability_service,
            )
            .map_err(|error| Commit { source: error })?;

            Self::persist_commit_status(conflict.is_none(), commit_sequence_number, &durability_service)
                .map_err(|error| DurabilityServiceWrite { source: error })?;
        }

        Ok(Self { name: name.to_owned(), path: path.to_owned(), durability_service, keyspaces, isolation_manager })
    }

    fn name(&self) -> &str {
        &self.name
    }

    pub fn open_snapshot_write(self: Arc<Self>) -> WriteSnapshot<Durability> {
        /*
        How to pick a sequence number:

        TXN 1 - open(0) ---> durably write = 10 PENDING ---> validate ---> write ---> committed. RETURN

        Question: does external consistency make sense for single-node machines?
        If user opens TXN 10 in thread 1, thread 2... work. User expects if open TXN after TXN 10 returns, we will see it. Otherwise, no expectations.
        Therefore - we can always use the last committed state safely for happens-before relations.

        For external consistency, we should use the the currently last pending sequence number and wait for it to finish.
         */

        let open_sequence_number = self.isolation_manager.watermark();
        WriteSnapshot::new(self, open_sequence_number)
    }

    pub fn open_snapshot_write_at(
        self: Arc<Self>,
        sequence_number: SequenceNumber,
    ) -> Result<WriteSnapshot<Durability>, WriteSnapshotOpenError> {
        // TODO: Support waiting for watermark to catch up to sequence number when we support causal reading.
        assert!(sequence_number <= self.read_watermark());
        Ok(WriteSnapshot::new(self, sequence_number))
    }

    pub fn open_snapshot_read(self: Arc<Self>) -> ReadSnapshot<Durability> {
        let open_sequence_number = self.isolation_manager.watermark();
        ReadSnapshot::new(self, open_sequence_number)
    }

    pub fn open_snapshot_read_at(
        self: Arc<Self>,
        sequence_number: SequenceNumber,
    ) -> Result<ReadSnapshot<Durability>, ReadSnapshotOpenError> {
        // TODO: Support waiting for watermark to catch up to sequence number when we support causal reading.
        assert!(sequence_number <= self.read_watermark());
        Ok(ReadSnapshot::new(self, sequence_number))
    }

    pub fn open_snapshot_schema(self: Arc<Self>) -> SchemaSnapshot<Durability> {
        // todo!("schema snapshot locking");
        let watermark = self.isolation_manager.watermark();
        SchemaSnapshot::new(self, watermark)
    }

    fn snapshot_commit(&self, snapshot: impl CommittableSnapshot<Durability>) -> Result<(), StorageCommitError>
    where
        Durability: DurabilityService,
    {
        use StorageCommitError::Durability;

        self.set_initial_put_status(&snapshot);
        let commit_record = snapshot.into_commit_record();

        let commit_sequence_number = self
            .durability_service
            .sequenced_write(&commit_record)
            .map_err(|error| Durability { name: self.name.to_string(), source: error })?;

        let conflict = Self::try_apply_commit(
            &self.name,
            commit_sequence_number,
            commit_record,
            &self.keyspaces,
            &self.isolation_manager,
            &self.durability_service,
        )?;

        Self::persist_commit_status(conflict.is_none(), commit_sequence_number, &self.durability_service)
            .map_err(|error| Durability { name: self.name.clone(), source: error })?;

        match conflict {
            Some(conflict) => Err(StorageCommitError::Isolation { name: self.name.clone(), conflict }),
            None => Ok(()),
        }
    }

    fn set_initial_put_status(&self, snapshot: &impl CommittableSnapshot<Durability>)
    where
        Durability: DurabilityService,
    {
        for buffer in snapshot.operations() {
            let writes = buffer.writes().write().unwrap();
            let puts = writes.iter().filter_map(|(key, write)| match write {
                Write::Put { value, reinsert, known_to_exist } => Some((key, value, reinsert, *known_to_exist)),
                _ => None,
            });
            for (key, value, reinsert, known_to_exist) in puts {
                let wrapped = StorageKeyReference::new_raw(buffer.keyspace_id, ByteReference::new(key.bytes()));
                if known_to_exist {
                    reinsert.store(false, Ordering::Release);
                } else {
                    let existing_stored = self
                        .get::<BUFFER_VALUE_INLINE>(wrapped, snapshot.open_sequence_number())
                        .unwrap() // TODO
                        .is_some_and(|reference| reference.bytes() == value.bytes());
                    reinsert.store(!existing_stored, Ordering::Release);
                }
            }
        }
    }

    fn try_apply_commit(
        storage_name: &str,
        commit_sequence_number: SequenceNumber,
        commit_record: CommitRecord,
        keyspaces: &Keyspaces,
        isolation_manager: &IsolationManager,
        durability_service: &Durability,
    ) -> Result<Option<IsolationConflict>, StorageCommitError>
    where
        Durability: DurabilityService,
    {
        use StorageCommitError::{Durability, Keyspace};
        if let Some(conflict) = isolation_manager
            .validate_commit(commit_sequence_number, commit_record, durability_service)
            .map_err(|error| Durability { name: storage_name.to_owned(), source: error })?
        {
            Ok(Some(conflict))
        } else {
            Self::apply_commit(commit_sequence_number, keyspaces, isolation_manager)
                .map_err(|error| Keyspace { name: storage_name.to_owned(), source: Arc::new(error) })?;
            Ok(None)
        }
    }

    fn apply_commit(
        commit_sequence_number: SequenceNumber,
        keyspaces: &Keyspaces,
        isolation_manager: &IsolationManager,
    ) -> Result<(), KeyspaceError> {
        // Write to the k-v storage
        keyspaces.write(WriteBatches::from_operations(
            commit_sequence_number,
            isolation_manager.get_commit_record(commit_sequence_number).operations(),
        ))?;

        // Inform the isolation manager and increment the watermark
        isolation_manager.applied(commit_sequence_number).unwrap();
        Ok(())
    }

    fn persist_commit_status(
        did_apply: bool,
        commit_sequence_number: SequenceNumber,
        durability_service: &Durability,
    ) -> Result<(), DurabilityError>
    where
        Durability: DurabilityService,
    {
        durability_service.unsequenced_write(&StatusRecord::new(commit_sequence_number, did_apply))?;
        Ok(())
    }

    pub fn closed_snapshot_write(&self, open_sequence_number: SequenceNumber) {
        self.isolation_manager.closed_for_read(open_sequence_number)
    }

    fn get_keyspace(&self, keyspace_id: KeyspaceId) -> &Keyspace {
        self.keyspaces.get(keyspace_id)
    }

    pub fn checkpoint(&self) -> Result<(), StorageCheckpointError> {
        use StorageCheckpointError::{
            CreateCheckpointDir, CreateMetadataFile, KeyspaceCheckpoint, ReadCheckpointDir, RemoveOldCheckpoint,
            WriteMetadata,
        };

        let watermark = self.isolation_manager.watermark();

        let checkpoint_dir = self.path.join(Self::CHECKPOINT_DIR_NAME);
        if !checkpoint_dir.exists() {
            fs::create_dir_all(&checkpoint_dir)
                .map_err(|error| CreateCheckpointDir { dir: checkpoint_dir.clone(), source: error })?
        }

        let previous_checkpoints: Vec<_> = fs::read_dir(&checkpoint_dir)
            .and_then(|entries| entries.map_ok(|entry| entry.path()).try_collect())
            .map_err(|error| ReadCheckpointDir { dir: checkpoint_dir.clone(), source: error })?;

        let current_checkpoint_dir = checkpoint_dir.join(format!("{}", Utc::now().timestamp_micros()));
        fs::create_dir_all(&current_checkpoint_dir)
            .map_err(|error| CreateCheckpointDir { dir: checkpoint_dir.clone(), source: error })?;

        self.keyspaces
            .checkpoint(&current_checkpoint_dir)
            .map_err(|error| KeyspaceCheckpoint { dir: current_checkpoint_dir.clone(), source: error })?;

        let metadata_file_path = current_checkpoint_dir.join(Self::CHECKPOINT_METADATA_FILE_NAME);
        let mut metadata_file = File::create(&metadata_file_path)
            .map_err(|error| CreateMetadataFile { file_path: metadata_file_path.clone(), source: error })?;
        metadata_file
            .write_all(watermark.number().to_string().as_bytes())
            .map_err(|error| WriteMetadata { file_path: metadata_file_path.clone(), source: error })?;
        metadata_file
            .sync_all()
            .map_err(|error| WriteMetadata { file_path: metadata_file_path.clone(), source: error })?;

        for previous_checkpoint in previous_checkpoints {
            fs::remove_dir_all(&previous_checkpoint)
                .map_err(|error| RemoveOldCheckpoint { dir: previous_checkpoint, source: error })?
        }

        Ok(())
    }

    pub fn delete_storage(self) -> Result<(), Vec<StorageDeleteError>> {
        use StorageDeleteError::{DirectoryDelete, KeyspaceDelete};

        self.keyspaces.delete().map_err(|errs| {
            errs.into_iter().map(|error| KeyspaceDelete { name: self.name.clone(), source: error }).collect_vec()
        })?;

        if self.path.exists() {
            std::fs::remove_dir_all(&self.path).map_err(|error| {
                error!("Failed to delete storage {}, received error: {}", self.name, error);
                vec![DirectoryDelete { name: self.name.clone(), source: error }]
            })?;
        }

        Ok(())
    }

    fn get<const INLINE_BYTES: usize>(
        &self,
        key: StorageKeyReference<'_>,
        open_sequence_number: SequenceNumber,
    ) -> Result<Option<ByteArray<INLINE_BYTES>>, MVCCReadError> {
        let mut iterator =
            self.iterate_range(KeyRange::new_within(StorageKey::<0>::Reference(key), false), open_sequence_number);
        loop {
            match iterator.next().transpose()? {
                None => return Ok(None),
                Some((k, v)) if k.bytes() == key.bytes() => return Ok(Some(ByteArray::copy(v.bytes()))),
                Some(_) => (),
            }
        }
    }

    pub(crate) fn iterate_range<'this, const PS: usize>(
        &'this self,
        range: KeyRange<StorageKey<'this, PS>>,
        open_sequence_number: SequenceNumber,
    ) -> MVCCRangeIterator<'this, PS> {
        MVCCRangeIterator::new(self, range, open_sequence_number)
    }

    pub fn read_watermark(&self) -> SequenceNumber {
        self.isolation_manager.watermark()
    }

    // --- direct access to storage, bypassing MVCC and returning raw key/value pairs ---

    pub fn put_raw(&self, key: StorageKeyReference<'_>, value: &Bytes<'_, BUFFER_VALUE_INLINE>) {
        // TODO: writes should always have to go through a transaction? Otherwise we have to WAL right here in a different path
        self.keyspaces
            .get(key.keyspace_id())
            .put(key.bytes(), value.bytes())
            .map_err(|e| MVCCStorageError {
                storage_name: self.name().to_owned(),
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
                storage_name: self.name().to_owned(),
                kind: MVCCStorageErrorKind::KeyspaceError {
                    source: Arc::new(e),
                    keyspace_name: self.keyspaces.get(key.keyspace_id()).name(),
                },
                // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
            })
            .unwrap_or_log()
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
        range: KeyRange<StorageKey<'this, PREFIX_INLINE>>,
    ) -> KeyspaceRangeIterator<'this, PREFIX_INLINE> {
        debug_assert!(!range.start().bytes().is_empty());
        self.keyspaces
            .get(range.start().keyspace_id())
            .iterate_range(range.map(|k| k.into_byte_array_or_ref(), |fixed| fixed))
    }
}

fn recover_isolation(
    storage_name: &str,
    checkpoint_sequence_number: SequenceNumber,
    durability_service: &impl DurabilityService,
) -> Result<(IsolationManager, Vec<(SequenceNumber, CommitRecord)>), StorageCommitError> {
    let isolation_manager = IsolationManager::new(checkpoint_sequence_number);

    enum CheckpointCommitStatus {
        Pending(CommitRecord),
        Applied(Option<CommitRecord>),
        Rejected,
    }

    let mut commits = BTreeMap::new();
    for record in durability_service
        .iter_from(checkpoint_sequence_number)
        .map_err(|error| StorageCommitError::Durability { name: storage_name.to_owned(), source: error })?
    {
        let RawRecord { sequence_number, record_type, bytes } = record.unwrap(); // FIXME
        match record_type {
            CommitRecord::RECORD_TYPE => {
                commits.insert(
                    sequence_number,
                    CheckpointCommitStatus::Pending(CommitRecord::deserialise_from(&mut &*bytes).unwrap()),
                );
            }
            StatusRecord::RECORD_TYPE => {
                let StatusRecord { commit_record_sequence_number, was_committed } =
                    StatusRecord::deserialise_from(&mut &*bytes).unwrap();
                if was_committed {
                    let record = commits.remove(&commit_record_sequence_number).map(|status| {
                        let CheckpointCommitStatus::Pending(record) = status else {
                            unreachable!("found second commit status for a record")
                        };
                        record
                    });
                    commits.insert(commit_record_sequence_number, CheckpointCommitStatus::Applied(record));
                } else {
                    commits.insert(commit_record_sequence_number, CheckpointCommitStatus::Rejected);
                }
            }
            _other => (), // skip?
        }
    }

    let mut pending_commits = Vec::new();
    for (commit_sequence_number, commit) in commits {
        match commit {
            CheckpointCommitStatus::Applied(commit_record) => {
                isolation_manager.load_applied(commit_sequence_number, commit_record.unwrap())
            }
            CheckpointCommitStatus::Rejected => isolation_manager.load_aborted(commit_sequence_number),
            CheckpointCommitStatus::Pending(commit_record) => {
                isolation_manager.opened_for_read(commit_record.open_sequence_number());
                pending_commits.push((commit_sequence_number, commit_record));
            }
        }
    }

    Ok((isolation_manager, pending_commits)) // FIXME
}

fn find_latest_checkpoint(checkpoint_dir: &Path) -> Result<Option<PathBuf>, StorageOpenError> {
    if !checkpoint_dir.exists() {
        return Ok(None);
    }

    fs::read_dir(checkpoint_dir)
        .and_then(|mut entries| entries.try_fold(None, |cur, entry| Ok(cur.max(Some(entry?.path())))))
        .map_err(|error| StorageOpenError::CheckpointRead { dir: checkpoint_dir.to_owned(), source: error })
}

#[derive(Debug)]
pub enum ReadSnapshotOpenError {}

impl fmt::Display for ReadSnapshotOpenError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

impl Error for ReadSnapshotOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {}
    }
}

#[derive(Debug)]
pub enum WriteSnapshotOpenError {}

impl fmt::Display for WriteSnapshotOpenError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

impl Error for WriteSnapshotOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {}
    }
}

#[derive(Debug)]
pub enum StorageOpenError {
    KeyspaceValidation { source: KeyspaceValidationError },
    KeyspaceOpen { source: KeyspaceOpenError },
    DurabilityServiceRead { source: DurabilityError },
    DurabilityServiceWrite { source: DurabilityError },
    CheckpointRead { dir: PathBuf, source: io::Error },
    MetadataRead { dir: PathBuf, source: io::Error },
    Commit { source: StorageCommitError },
}

impl fmt::Display for StorageOpenError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for StorageOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::KeyspaceValidation { source, .. } => Some(source),
            Self::KeyspaceOpen { source, .. } => Some(source),
            Self::DurabilityServiceRead { source, .. } => Some(source),
            Self::DurabilityServiceWrite { source, .. } => Some(source),
            Self::CheckpointRead { source, .. } => Some(source),
            Self::MetadataRead { source, .. } => Some(source),
            Self::Commit { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum StorageCheckpointError {
    CreateCheckpointDir { dir: PathBuf, source: io::Error },
    ReadCheckpointDir { dir: PathBuf, source: io::Error },

    KeyspaceCheckpoint { dir: PathBuf, source: KeyspaceCheckpointError },

    CreateMetadataFile { file_path: PathBuf, source: io::Error },
    WriteMetadata { file_path: PathBuf, source: io::Error },

    RemoveOldCheckpoint { dir: PathBuf, source: io::Error },
}

impl fmt::Display for StorageCheckpointError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for StorageCheckpointError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CreateCheckpointDir { source, .. } => Some(source),
            Self::ReadCheckpointDir { source, .. } => Some(source),
            Self::KeyspaceCheckpoint { source, .. } => Some(source),
            Self::CreateMetadataFile { source, .. } => Some(source),
            Self::WriteMetadata { source, .. } => Some(source),
            Self::RemoveOldCheckpoint { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum StorageCommitError {
    Isolation { name: String, conflict: IsolationConflict },
    Keyspace { name: String, source: Arc<KeyspaceError> },
    Durability { name: String, source: DurabilityError },
}

impl fmt::Display for StorageCommitError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for StorageCommitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Isolation { .. } => None,
            Self::Keyspace { source, .. } => Some(source),
            Self::Durability { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum StorageDeleteError {
    KeyspaceDelete { name: String, source: KeyspaceError },
    DirectoryDelete { name: String, source: io::Error },
}

impl fmt::Display for StorageDeleteError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for StorageDeleteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::KeyspaceDelete { source, .. } => Some(source),
            Self::DirectoryDelete { source, .. } => Some(source),
        }
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
        let bytes = byte_array.bytes_mut();

        let key_end = key.len();
        let sequence_number_end = key_end + SequenceNumber::serialised_len();
        let operation_end = sequence_number_end + StorageOperation::serialised_len();

        bytes[0..key_end].copy_from_slice(key);
        sequence_number.invert().serialise_be_into(&mut bytes[key_end..sequence_number_end]);
        bytes[sequence_number_end..operation_end].copy_from_slice(storage_operation.bytes());

        Self { bytes: Bytes::Array(byte_array) }
    }

    fn wrap_slice(bytes: &'bytes [u8]) -> Self {
        Self { bytes: Bytes::Reference(ByteReference::new(bytes)) }
    }

    pub(crate) fn is_visible_to(&self, sequence_number: SequenceNumber) -> bool {
        self.sequence_number() <= sequence_number
    }

    fn bytes(&self) -> &[u8] {
        self.bytes.bytes()
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
    use durability::{wal::WAL, DurabilityService, SequenceNumber, Sequencer};
    use test_utils::{create_tmp_dir, init_logging};

    use super::{
        isolation_manager::CommitRecord,
        key_value::StorageKeyReference,
        keyspace::{KeyspaceId, KeyspaceSet, Keyspaces},
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
    fn recovery_path() {
        test_keyspace_set! {
            Keyspace => 0: "keyspace",
        }

        init_logging();
        let storage_path = create_tmp_dir();
        let watermark_after_one_commit = {
            let storage: MVCCStorage<WAL> = MVCCStorage::open::<TestKeyspaceSet>("storage", &storage_path).unwrap();
            let commit_record = CommitRecord::new(OperationsBuffer::new(), SequenceNumber::MIN);
            let commit_sequence_number = storage.durability_service.sequenced_write(&commit_record).unwrap();

            let conflict = MVCCStorage::try_apply_commit(
                &storage.name,
                commit_sequence_number,
                commit_record,
                &storage.keyspaces,
                &storage.isolation_manager,
                &storage.durability_service,
            )
            .unwrap();
            MVCCStorage::persist_commit_status(conflict.is_none(), commit_sequence_number, &storage.durability_service)
                .unwrap();

            storage.read_watermark()
        };

        let pending_commit_sequence = {
            let storage: MVCCStorage<WAL> = MVCCStorage::open::<TestKeyspaceSet>("storage", &storage_path).unwrap();
            assert_eq!(watermark_after_one_commit, storage.read_watermark());
            storage
                .durability_service
                .sequenced_write::<CommitRecord>(&CommitRecord::new(OperationsBuffer::new(), storage.read_watermark()))
                .unwrap()
            // We don't commit it.
        };
        {
            let storage: MVCCStorage<WAL> = MVCCStorage::open::<TestKeyspaceSet>("storage", &storage_path).unwrap();
            assert_eq!(pending_commit_sequence, storage.read_watermark()); // Recovery will commit the pending one.
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

        let seq = {
            let full_operations = OperationsBuffer::new();
            full_operations
                .writes_in(TestKeyspaceSet::PersistedKeyspace.id())
                .insert(ByteArray::copy(b"hello"), ByteArray::empty());
            full_operations
                .writes_in(TestKeyspaceSet::FailedKeyspace.id())
                .insert(ByteArray::copy(b"world"), ByteArray::empty());

            let partial_operations = OperationsBuffer::new();
            partial_operations
                .writes_in(TestKeyspaceSet::PersistedKeyspace.id())
                .insert(ByteArray::copy(b"hello"), ByteArray::empty());

            let mut durability_service = WAL::open(storage_path.join(<MVCCStorage<WAL>>::WAL_DIR_NAME)).unwrap();
            durability_service.register_record_type::<CommitRecord>();
            let seq = durability_service
                .sequenced_write(&CommitRecord::new(full_operations, durability_service.previous()))
                .unwrap();

            let partial_commit = WriteBatches::from_operations(seq, &partial_operations);
            let keyspaces =
                Keyspaces::open::<TestKeyspaceSet>(storage_path.join(<MVCCStorage<WAL>>::STORAGE_DIR_NAME)).unwrap();
            keyspaces.write(partial_commit).unwrap();

            seq
        };

        let storage: MVCCStorage<WAL> = MVCCStorage::open::<TestKeyspaceSet>("storage", &storage_path).unwrap();
        assert_eq!(
            storage
                .get::<0>(
                    StorageKeyReference::new(TestKeyspaceSet::FailedKeyspace, (&ByteArray::<5>::copy(b"world")).into()),
                    seq
                )
                .unwrap()
                .unwrap(),
            ByteArray::empty()
        )
    }
}
