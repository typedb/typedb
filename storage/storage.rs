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
    error::Error,
    fmt,
    fs::{self, File},
    io::{self, Write as _},
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use chrono::Utc;
use durability::{DurabilityError, DurabilityService, SequenceNumber};
use isolation_manager::IsolationError;
use iterator::MVCCReadError;
use itertools::Itertools;
use keyspace::KeyspaceError;
use logger::{error, result::ResultExt};
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use speedb::{Options, WriteBatch};

use crate::{
    error::{MVCCStorageError, MVCCStorageErrorKind},
    isolation_manager::{CommitRecord, CommitStatus, IsolationManager, StatusRecord},
    iterator::MVCCRangeIterator,
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyReference},
    keyspace::{
        iterator::KeyspaceRangeIterator, Keyspace, KeyspaceCheckpointError, KeyspaceId, KeyspaceOpenError,
        KEYSPACE_ID_MAX, KEYSPACE_ID_RESERVED_UNSET, KEYSPACE_MAXIMUM_COUNT,
    },
    snapshot::{
        buffer::OperationsBuffer, write::Write, CommittableSnapshot, ReadSnapshot, SchemaSnapshot, WriteSnapshot,
    },
};

pub mod error;
pub mod isolation_manager;
pub mod iterator;
pub mod key_range;
pub mod key_value;
pub mod keyspace;
pub mod snapshot;

#[derive(Debug)]
pub struct MVCCStorage<D> {
    name: String,
    path: PathBuf,
    keyspaces: Vec<Keyspace>,
    keyspaces_index: [Option<KeyspaceId>; KEYSPACE_MAXIMUM_COUNT],
    durability_service: D,
    isolation_manager: IsolationManager,
}

fn new_db_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.enable_statistics();
    // TODO optimise per-keyspace
    options
}

pub trait KeyspaceSet: Copy {
    fn iter() -> impl Iterator<Item = Self>;
    fn id(&self) -> KeyspaceId;
    fn name(&self) -> &'static str;
}

fn validate_new_keyspace(
    keyspace_id: impl KeyspaceSet,
    keyspaces: &[Keyspace],
    keyspaces_index: &[Option<KeyspaceId>],
) -> Result<(), KeyspaceValidationError> {
    use KeyspaceValidationError::{IdExists, IdReserved, IdTooLarge, NameExists};

    let name = keyspace_id.name();

    if keyspace_id.id() == KEYSPACE_ID_RESERVED_UNSET {
        return Err(IdReserved { name, id: keyspace_id.id().0 });
    }

    if keyspace_id.id() > KEYSPACE_ID_MAX {
        return Err(IdTooLarge { name, id: keyspace_id.id().0, max_id: KEYSPACE_ID_MAX.0 });
    }

    for (existing_id, existing_keyspace_index) in keyspaces_index.iter().enumerate() {
        if let Some(existing_index) = existing_keyspace_index {
            let keyspace = &keyspaces[existing_index.0 as usize];
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

#[derive(Debug)]
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
            Self::IdReserved { name, id, .. } => write!(f, "reserved keyspace id '{id}' cannot be used for new keyspace '{name}'."),
            Self::IdTooLarge { name, id, max_id, .. } => write!(
                f, "keyspace id '{id}' cannot be used for new keyspace '{name}' since it is larger than maximum keyspace id '{max_id}'.",
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

fn db_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.enable_statistics();
    // TODO optimise per-keyspace
    options
}

fn recover_keyspaces<KS: KeyspaceSet>(
    storage_dir: impl AsRef<Path>,
) -> Result<(Vec<Keyspace>, [Option<KeyspaceId>; KEYSPACE_MAXIMUM_COUNT]), StorageRecoverError> {
    use StorageRecoverError::{KeyspaceOpen, KeyspaceValidation};

    let path = storage_dir.as_ref();
    let mut keyspaces = Vec::new();
    let mut keyspaces_index = core::array::from_fn(|_| None);
    let options = db_options();
    for keyspace_id in KS::iter() {
        validate_new_keyspace(keyspace_id, &keyspaces, &keyspaces_index)
            .map_err(|error| KeyspaceValidation { source: error })?;
        keyspaces.push(Keyspace::open(path, keyspace_id, &options).map_err(|error| KeyspaceOpen { source: error })?);
        keyspaces_index[keyspace_id.id().0 as usize] = Some(KeyspaceId(keyspaces.len() as u8 - 1));
    }
    Ok((keyspaces, keyspaces_index))
}

impl<D> MVCCStorage<D> {
    const WAL_DIR_NAME: &'static str = "wal";
    const STORAGE_DIR_NAME: &'static str = "storage";
    const CHECKPOINT_DIR_NAME: &'static str = "checkpoint";
    const CHECKPOINT_METADATA_FILE_NAME: &'static str = "METADATA";

    pub fn recover<KS: KeyspaceSet>(name: impl AsRef<str>, path: &Path) -> Result<Self, StorageRecoverError>
    where
        D: DurabilityService,
    {
        let storage_dir = path.join(Self::STORAGE_DIR_NAME);
        if !storage_dir.exists() {
            fs::create_dir_all(&storage_dir).map_err(|_error| todo!())?;
        }

        // FIXME proper error
        let mut durability_service = D::recover(path.join(Self::WAL_DIR_NAME)).expect("Could not create WAL directory");
        durability_service.register_record_type::<CommitRecord>();
        durability_service.register_record_type::<StatusRecord>();

        let name = name.as_ref();
        let (keyspaces, keyspaces_index) = recover_keyspaces::<KS>(&storage_dir)?;

        let recovery_starts_from = SequenceNumber::from(1); // TODO!
        let isolation_manager = IsolationManager::new(recovery_starts_from);

        let name = name.to_owned();
        let path = path.to_owned();
        let storage = Self { name, path, keyspaces, keyspaces_index, isolation_manager, durability_service };

        storage.reload()?;

        Ok(storage)
    }

    fn reload(&self) -> Result<(), StorageRecoverError>
    where
        D: DurabilityService,
    {
        use StorageRecoverError::DurabilityServiceRead;

        let mut fresh_commits: Vec<SequenceNumber> = Vec::new();

        let records = IsolationManager::iterate_commit_status_from_disk(
            &self.durability_service,
            SequenceNumber::MIN,
            SequenceNumber::MAX,
        )
        .map_err(|error| DurabilityServiceRead { source: error })?;

        for record in records {
            let (commit_sequence_number, commit_status) =
                record.map_err(|error| DurabilityServiceRead { source: error })?;
            match commit_status {
                CommitStatus::Applied(commit_record) => {
                    self.isolation_manager.load_applied(commit_sequence_number, commit_record.into_owned());
                }
                CommitStatus::Aborted => {
                    self.isolation_manager.load_aborted(commit_sequence_number);
                }
                CommitStatus::Pending(commit_record) => {
                    self.isolation_manager.opened_for_read(commit_record.open_sequence_number()); // try_commit currently decrements reader count.
                    let try_commit_result =
                        self.try_write_commit_record(commit_sequence_number, commit_record.into_owned());
                    match try_commit_result {
                        Ok(()) => fresh_commits.push(commit_sequence_number),
                        Err(StorageCommitError::Isolation { .. }) => (), // Isolation errors are fine.
                        Err(_) => try_commit_result.unwrap(),            // TODO: Other errors are not
                    }
                }
                CommitStatus::Empty | CommitStatus::Validated(_) => unreachable!(),
            }
        }

        for commit_sequence_number in fresh_commits {
            if let Err(_error) =
                self.durability_service.unsequenced_write(&StatusRecord::new(commit_sequence_number, true))
            {
                todo!(); // TODO: What happens if the persist fails? For now, we need to crash the server
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    pub fn open_snapshot_write(self: Arc<Self>) -> WriteSnapshot<D> {
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
    ) -> Result<WriteSnapshot<D>, WriteSnapshotOpenError> {
        // TODO: Support waiting for watermark to catch up to sequence number when we support causal reading.
        assert!(sequence_number <= self.read_watermark());
        Ok(WriteSnapshot::new(self, sequence_number))
    }

    pub fn open_snapshot_read(self: Arc<Self>) -> ReadSnapshot<D> {
        let open_sequence_number = self.isolation_manager.watermark();
        ReadSnapshot::new(self, open_sequence_number)
    }

    pub fn open_snapshot_read_at(
        self: Arc<Self>,
        sequence_number: SequenceNumber,
    ) -> Result<ReadSnapshot<D>, ReadSnapshotOpenError> {
        // TODO: Support waiting for watermark to catch up to sequence number when we support causal reading.
        assert!(sequence_number <= self.read_watermark());
        Ok(ReadSnapshot::new(self, sequence_number))
    }

    pub fn open_snapshot_schema(self: Arc<Self>) -> SchemaSnapshot<D> {
        // todo!("schema snapshot locking");
        let watermark = self.isolation_manager.watermark();
        SchemaSnapshot::new(self, watermark)
    }

    fn snapshot_commit(&self, snapshot: impl CommittableSnapshot<D>) -> Result<(), StorageCommitError>
    where
        D: DurabilityService,
    {
        use StorageCommitError::Durability;

        let commit_record = snapshot.into_commit_record();

        // 0. Assign whether the put operations need to be performed given storage contents at open
        //    sequence number
        for buffer in commit_record.operations() {
            let writes = buffer.writes().write().unwrap();
            let puts = writes.iter().filter_map(|(key, write)| match write {
                Write::Put { value, reinsert } => Some((key, value, reinsert)),
                _ => None,
            });
            for (key, value, reinsert) in puts {
                let wrapped = StorageKeyReference::new_raw(buffer.keyspace_id, ByteReference::new(key.bytes()));
                let existing_stored: Option<Option<ByteArray<BUFFER_VALUE_INLINE>>> = self
                    .get(wrapped, commit_record.open_sequence_number(), |reference| {
                        // Only copy if the value is the same
                        (reference.bytes() == value.bytes()).then(|| ByteArray::from(reference))
                    })
                    .unwrap(); // TODO
                reinsert.store(existing_stored.flatten().is_none(), Ordering::Release);
            }
        }

        //  1. make durable and get sequence number
        let commit_sequence_number = self
            .durability_service
            .sequenced_write(&commit_record)
            .map_err(|error| Durability { name: self.name.to_string(), source: error })?;

        // 2,3,4:
        self.try_write_commit_record(commit_sequence_number, commit_record)?;

        // 5. Persist the commit status
        match self.durability_service.unsequenced_write(&StatusRecord::new(commit_sequence_number, true)) {
            Ok(_) => Ok(()),
            Err(_) => todo!(), // TODO: What happens if the persist fails? For now, we need to crash the server
        }
    }

    fn try_write_commit_record(
        &self,
        commit_sequence_number: SequenceNumber,
        commit_record: CommitRecord,
    ) -> Result<(), StorageCommitError>
    where
        D: DurabilityService,
    {
        use StorageCommitError::{Isolation, Keyspace};

        // 2. validate commit isolation
        let validation_result =
            self.isolation_manager.try_commit(commit_sequence_number, commit_record, &self.durability_service);
        if let Err(error) = validation_result {
            self.durability_service.unsequenced_write(&StatusRecord::new(commit_sequence_number, false)).unwrap();
            return Err(Isolation { name: self.name.clone(), source: error });
        }

        //  3. write to kv-storage
        let write_batches = self.isolation_manager.apply_to_commit_record(commit_sequence_number, |record| {
            self.to_write_batches(commit_sequence_number, record.operations())
        });

        for (index, write_batch) in write_batches.into_iter().enumerate() {
            debug_assert!(index < KEYSPACE_MAXIMUM_COUNT);
            if write_batch.is_some() {
                if let Err(error) = self.get_keyspace(KeyspaceId(index as _)).write(write_batch.unwrap()) {
                    return Err(Keyspace {
                        name: self.name.clone(),
                        source: Arc::new(error),
                        keyspace_name: self.get_keyspace(KeyspaceId(index as _)).name(),
                    });
                }
            }
        }
        // 4. Inform the isolation manager and increment the watermark.
        self.isolation_manager.applied(commit_sequence_number);
        Ok(())
    }

    fn to_write_batches(
        &self,
        seq: SequenceNumber,
        buffers: &OperationsBuffer,
    ) -> [Option<WriteBatch>; KEYSPACE_MAXIMUM_COUNT] {
        let mut write_batches: [Option<WriteBatch>; KEYSPACE_MAXIMUM_COUNT] = core::array::from_fn(|_| None);

        for (index, buffer) in buffers.write_buffers().enumerate() {
            let map = buffer.writes().read().unwrap();
            if !map.is_empty() {
                let mut write_batch = WriteBatch::default();
                for (key, write) in &*map {
                    match write {
                        Write::Insert { value } => write_batch
                            .put(MVCCKey::build(key.bytes(), seq, StorageOperation::Insert).bytes(), value.bytes()),
                        Write::Put { value, reinsert } => {
                            if reinsert.load(Ordering::SeqCst) {
                                write_batch.put(
                                    MVCCKey::build(key.bytes(), seq, StorageOperation::Insert).bytes(),
                                    value.bytes(),
                                )
                            }
                        }
                        Write::Delete => {
                            write_batch.put(MVCCKey::build(key.bytes(), seq, StorageOperation::Delete).bytes(), [])
                        }
                    }
                }
                write_batches[index] = Some(write_batch);
            }
        }
        write_batches
    }

    pub fn closed_snapshot_write(&self, open_sequence_number: SequenceNumber) {
        self.isolation_manager.closed_for_read(open_sequence_number)
    }

    fn get_keyspace(&self, keyspace_id: KeyspaceId) -> &Keyspace {
        let keyspace_index = self.keyspaces_index[keyspace_id.0 as usize].unwrap();
        &self.keyspaces[keyspace_index.0 as usize]
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
            .map_err(|error| ReadCheckpointDir { dir: checkpoint_dir.clone(), source: error })?
            .map_ok(|entry| entry.path())
            .try_collect()
            .map_err(|error| ReadCheckpointDir { dir: checkpoint_dir.clone(), source: error })?;

        let current_checkpoint_dir = checkpoint_dir.join(format!("{}", Utc::now().timestamp_micros()));
        fs::create_dir_all(&current_checkpoint_dir)
            .map_err(|error| CreateCheckpointDir { dir: checkpoint_dir.clone(), source: error })?;

        for keyspace in &self.keyspaces {
            keyspace.checkpoint(&current_checkpoint_dir).map_err(|error| KeyspaceCheckpoint {
                keyspace_name: keyspace.name().to_owned(),
                dir: current_checkpoint_dir.clone(),
                source: error,
            })?;
        }

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

    pub fn delete_storage(self) -> Result<(), Vec<MVCCStorageError>> {
        let errors: Vec<MVCCStorageError> = self
            .keyspaces
            .into_iter()
            .map(|keyspace| keyspace.delete())
            .filter(|result| result.is_err())
            .map(|result| MVCCStorageError {
                storage_name: self.name.clone(),
                kind: MVCCStorageErrorKind::KeyspaceDeleteError { source: result.unwrap_err() },
            })
            .collect();

        if !errors.is_empty() {
            return Err(errors);
        }

        if self.path.exists() {
            std::fs::remove_dir_all(&self.path).map_err(|err| {
                error!("Failed to delete storage {}, received error: {}", self.name, err);
                vec![MVCCStorageError {
                    storage_name: self.name.clone(),
                    kind: MVCCStorageErrorKind::FailedToDeleteStorage { source: err },
                }]
            })?;
        }

        Ok(())
    }

    pub fn get<V>(
        &self,
        key: StorageKeyReference<'_>,
        open_sequence_number: SequenceNumber,
        mut mapper: impl FnMut(ByteReference<'_>) -> V,
    ) -> Result<Option<V>, MVCCReadError> {
        let mut iterator =
            self.iterate_range(KeyRange::new_within(StorageKey::<0>::Reference(key), false), open_sequence_number);
        // TODO: we don't want to panic on unwrap here
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
        self.get_keyspace(key.keyspace_id())
            .put(key.bytes(), value.bytes())
            .map_err(|e| MVCCStorageError {
                storage_name: self.name().to_owned(),
                kind: MVCCStorageErrorKind::KeyspaceError {
                    source: Arc::new(e),
                    keyspace_name: self.get_keyspace(key.keyspace_id()).name(),
                },
            })
            .unwrap_or_log()
    }

    pub fn get_raw<M, V>(&self, key: StorageKeyReference<'_>, mut mapper: M) -> Option<V>
    where
        M: FnMut(&[u8]) -> V,
    {
        self.get_keyspace(key.keyspace_id())
            .get(key.bytes(), |value| mapper(value))
            .map_err(|e| MVCCStorageError {
                storage_name: self.name().to_owned(),
                kind: MVCCStorageErrorKind::KeyspaceError {
                    source: Arc::new(e),
                    keyspace_name: self.get_keyspace(key.keyspace_id()).name(),
                },
                // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
            })
            .unwrap_or_log()
    }

    pub fn get_prev_raw<M, T>(&self, key: StorageKeyReference<'_>, mut key_value_mapper: M) -> Option<T>
    where
        M: FnMut(&MVCCKey<'_>, &[u8]) -> T,
    {
        self.get_keyspace(key.keyspace_id())
            .get_prev(key.bytes(), |raw_key, v| key_value_mapper(&MVCCKey::wrap_slice(raw_key), v))
    }

    pub fn iterate_keyspace_range<'this, const PREFIX_INLINE: usize>(
        &'this self,
        range: KeyRange<StorageKey<'this, PREFIX_INLINE>>,
    ) -> KeyspaceRangeIterator<'this, PREFIX_INLINE> {
        debug_assert!(!range.start().bytes().is_empty());
        self.get_keyspace(range.start().keyspace_id())
            .iterate_range(range.map(|k| k.into_byte_array_or_ref(), |fixed| fixed))
    }
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
pub enum StorageRecoverError {
    KeyspaceValidation { source: KeyspaceValidationError },
    KeyspaceOpen { source: KeyspaceOpenError },
    DurabilityServiceRead { source: DurabilityError },
}

impl fmt::Display for StorageRecoverError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for StorageRecoverError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::KeyspaceValidation { source, .. } => Some(source),
            Self::KeyspaceOpen { source, .. } => Some(source),
            Self::DurabilityServiceRead { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum StorageCheckpointError {
    CreateCheckpointDir { dir: PathBuf, source: io::Error },
    ReadCheckpointDir { dir: PathBuf, source: io::Error },

    KeyspaceCheckpoint { keyspace_name: String, dir: PathBuf, source: KeyspaceCheckpointError },

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
    Isolation { name: String, source: IsolationError },
    Keyspace { name: String, keyspace_name: &'static str, source: Arc<KeyspaceError> },
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
            Self::Isolation { source, .. } => Some(source),
            Self::Keyspace { source, .. } => Some(source),
            Self::Durability { source, .. } => Some(source),
        }
    }
}

///
/// MVCC keys are made of three parts: the [KEY][SEQ][OP]
///
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

    const fn bytes(&self) -> &[u8; StorageOperation::BYTES] {
        match self {
            StorageOperation::Insert => &[0x0],
            StorageOperation::Delete => &[0x1],
        }
    }

    const fn from(bytes: &[u8]) -> StorageOperation {
        match bytes {
            [0x0] => StorageOperation::Insert,
            [0x1] => StorageOperation::Delete,
            _ => panic!("Unrecognised storage operation bytes."),
        }
    }

    const fn serialised_len() -> usize {
        StorageOperation::BYTES
    }
}

#[cfg(test)]
mod tests {
    use durability::{wal::WAL, DurabilityService, SequenceNumber};
    use test_utils::{create_tmp_dir, init_logging};

    use crate::{
        isolation_manager::CommitRecord, snapshot::buffer::OperationsBuffer, KeyspaceId, KeyspaceSet, MVCCStorage,
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

    test_keyspace_set! {
        Keyspace => 0: "keyspace",
    }
    #[test]
    fn recovery_path() {
        init_logging();
        let storage_path = create_tmp_dir();
        let watermark_after_one_commit = {
            let storage: MVCCStorage<WAL> = MVCCStorage::recover::<TestKeyspaceSet>("storage", &storage_path).unwrap();
            let commit_record = CommitRecord::new(OperationsBuffer::new(), SequenceNumber::MIN);
            let commit_sequence_number = storage.durability_service.sequenced_write(&commit_record).unwrap();
            storage.try_write_commit_record(SequenceNumber::from(1), commit_record).unwrap();
            storage.read_watermark()
        };

        let pending_commit_sequence = {
            let storage: MVCCStorage<WAL> = MVCCStorage::recover::<TestKeyspaceSet>("storage", &storage_path).unwrap();
            assert_eq!(watermark_after_one_commit, storage.read_watermark());
            storage
                .durability_service
                .sequenced_write::<CommitRecord>(&CommitRecord::new(OperationsBuffer::new(), storage.read_watermark()))
                .unwrap()
            // We don't commit it.
        };
        {
            let storage: MVCCStorage<WAL> = MVCCStorage::recover::<TestKeyspaceSet>("storage", &storage_path).unwrap();
            assert_eq!(pending_commit_sequence, storage.read_watermark()); // Recovery will commit the pending one.
        };
    }
}
