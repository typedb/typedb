/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::{
    path::PathBuf,
    rc::Rc,
    sync::{atomic::Ordering, Arc},
};

use bytes::{byte_array::ByteArray, byte_array_or_ref::ByteArrayOrRef, byte_reference::ByteReference};
use durability::{wal::WAL, DurabilityService, SequenceNumber, Sequencer};
use iterator::State;
use logger::{error, result::ResultExt};
use primitive::u80::U80;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use snapshot::write::Write;
use speedb::{Options, WriteBatch};

use crate::{
    error::{MVCCStorageError, MVCCStorageErrorKind},
    isolation_manager::{CommitRecord, IsolationManager},
    key_value::{StorageKey, StorageKeyArray, StorageKeyReference},
    keyspace::keyspace::{
        Keyspace, KeyspaceError, KeyspaceId, KeyspacePrefixIterator, KEYSPACE_ID_MAX, KEYSPACE_ID_RESERVED_UNSET,
        KEYSPACE_MAXIMUM_COUNT,
    },
    snapshot::{
        buffer::KeyspaceBuffers,
        snapshot::{ReadSnapshot, WriteSnapshot},
    },
};

pub mod error;
pub mod isolation_manager;
pub mod key_value;
pub mod keyspace;
pub mod snapshot;

#[derive(Debug)]
pub struct MVCCStorage {
    owner_name: Rc<str>,
    path: PathBuf,
    keyspaces: Vec<Keyspace>,
    keyspaces_index: [Option<KeyspaceId>; KEYSPACE_MAXIMUM_COUNT],
    // TODO: inject either a remote or local service
    durability_service: WAL,
    isolation_manager: IsolationManager,
}

impl MVCCStorage {
    const STORAGE_DIR_NAME: &'static str = "storage";

    pub fn new(owner_name: Rc<str>, path: &PathBuf) -> Result<Self, MVCCStorageError> {
        let storage_dir = path.with_extension(MVCCStorage::STORAGE_DIR_NAME);
        let mut durability_service = WAL::open("/tmp/wal").expect("Could not create WAL directory");
        durability_service.register_record_type::<CommitRecord>();
        Ok(MVCCStorage {
            owner_name: owner_name.clone(),
            path: storage_dir,
            keyspaces: Vec::new(),
            keyspaces_index: core::array::from_fn(|_| None),
            isolation_manager: IsolationManager::new(durability_service.current()),
            durability_service,
        })
    }

    // TODO: we want to be able to pass new options, since Rocks can handle rebooting with new options
    fn load_from_checkpoint(
        _path: &PathBuf,
        _durability_service: impl DurabilityService,
    ) -> Result<Self, MVCCStorageError> {
        todo!("Booting from checkpoint not yet implemented")

        // Steps:
        //   Load each keyspace from their latest checkpoint
        //   Get the last known committed sequence number from each keyspace (if we want to do this at all... could probably just replay)
        //   Iterate over records from Durability Service from the earliest sequence number
        //   For each record, commit the records. Some keyspaces will write duplicates, but all writes are idempotent so this is OK.

        // TODO: we have to be careful when we resume from a checkpoint and reapply the Records.Write.InsertPreexisting, to re-check if previous commits have deleted these keys
    }

    pub fn create_keyspace(
        &mut self,
        name: &str,
        keyspace_id: KeyspaceId,
        options: &Options,
    ) -> Result<(), MVCCStorageError> {
        let mut keyspace_path = self.path.clone();
        keyspace_path.push(name);
        self.validate_new_keyspace(name, keyspace_id)?;

        self.keyspaces.push(Keyspace::new(keyspace_path, options, keyspace_id).map_err(|err| MVCCStorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: MVCCStorageErrorKind::KeyspaceError { source: Arc::new(err), keyspace: name.to_owned() },
        })?);
        self.keyspaces_index[keyspace_id as usize] = Some(self.keyspaces.len() as KeyspaceId - 1);
        Ok(())
    }

    pub fn new_db_options() -> Options {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.enable_statistics();
        // TODO optimise per-keyspace
        options
    }

    fn validate_new_keyspace(&self, name: &str, keyspace_id: KeyspaceId) -> Result<(), MVCCStorageError> {
        if keyspace_id == KEYSPACE_ID_RESERVED_UNSET {
            return Err(MVCCStorageError {
                storage_name: self.owner_name.as_ref().to_owned(),
                kind: MVCCStorageErrorKind::KeyspaceIdReserved { keyspace: name.to_owned(), keyspace_id },
            });
        } else if keyspace_id > KEYSPACE_ID_MAX {
            return Err(MVCCStorageError {
                storage_name: self.owner_name.as_ref().to_owned(),
                kind: MVCCStorageErrorKind::KeyspaceIdTooLarge {
                    keyspace: name.to_owned(),
                    keyspace_id,
                    max_keyspace_id: KEYSPACE_ID_MAX,
                },
            });
        }
        for existing_keyspace_id in 0..self.keyspaces_index.len() {
            if let Some(existing_keyspace_index) = self.keyspaces_index[existing_keyspace_id] {
                let keyspace = &self.keyspaces[existing_keyspace_index as usize];
                if keyspace.name() == name {
                    return Err(MVCCStorageError {
                        storage_name: self.owner_name.as_ref().to_owned(),
                        kind: MVCCStorageErrorKind::KeyspaceNameExists { keyspace: name.to_owned() },
                    });
                } else if existing_keyspace_id as KeyspaceId == keyspace_id {
                    return Err(MVCCStorageError {
                        storage_name: self.owner_name.as_ref().to_owned(),
                        kind: MVCCStorageErrorKind::KeyspaceIdExists {
                            new_keyspace: name.to_owned(),
                            keyspace_id,
                            existing_keyspace: keyspace.name().to_owned(),
                        },
                    });
                }
            }
        }
        Ok(())
    }

    pub fn open_snapshot_write(&self) -> WriteSnapshot<'_> {
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

    pub fn open_snapshot_read(&self) -> ReadSnapshot<'_> {
        let open_sequence_number = self.isolation_manager.watermark();
        ReadSnapshot::new(self, open_sequence_number)
    }

    pub fn open_snapshot_read_at(&self, sequence_number: SequenceNumber) -> ReadSnapshot<'_> {
        // TODO: validate sequence number is before or equal to watermark
        ReadSnapshot::new(self, sequence_number)
    }

    pub fn snapshot_commit<'storage>(
        &'storage self,
        snapshot: WriteSnapshot<'storage>,
    ) -> Result<(), MVCCStorageError> {
        let commit_record = snapshot.into_commit_record();

        //  1. make durable and get sequence number
        let commit_sequence_number =
            self.durability_service.sequenced_write(&commit_record).map_err(|err| MVCCStorageError {
                storage_name: self.owner_name.to_string(),
                kind: MVCCStorageErrorKind::DurabilityError { source: err },
            })?;

        // 2. validate commit isolation
        self.isolation_manager.try_commit(commit_sequence_number, commit_record).map_err(|err| MVCCStorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: MVCCStorageErrorKind::IsolationError { source: err },
        })?;

        //  3. write to kv-storage
        let write_batches = self.isolation_manager.apply_to_commit_record(&commit_sequence_number, |record| {
            self.to_write_batches(&commit_sequence_number, record.buffers())
        });

        for (index, write_batch) in write_batches.into_iter().enumerate() {
            debug_assert!(index < KEYSPACE_MAXIMUM_COUNT);
            if write_batch.is_some() {
                self.get_keyspace(index as KeyspaceId).write(write_batch.unwrap()).map_err(|error| {
                    MVCCStorageError {
                        storage_name: self.owner_name.as_ref().to_owned(),
                        kind: MVCCStorageErrorKind::KeyspaceError {
                            source: Arc::new(error),
                            keyspace: self.get_keyspace(index as KeyspaceId).name().to_owned(),
                        },
                    }
                })?;
            }
        }
        Ok(())
    }

    fn to_write_batches(
        &self,
        commit_sequence_number: &SequenceNumber,
        buffers: &KeyspaceBuffers,
    ) -> [Option<WriteBatch>; KEYSPACE_MAXIMUM_COUNT] {
        let mut write_batches: [Option<WriteBatch>; KEYSPACE_MAXIMUM_COUNT] = core::array::from_fn(|_| None);

        buffers.iter().enumerate().for_each(|(index, buffer)| {
            let map = buffer.map().read().unwrap();
            if map.is_empty() {
                write_batches[index] = None
            } else {
                let mut write_batch = WriteBatch::default();
                map.iter().for_each(|(key, write)| match write {
                    Write::Insert(value) => write_batch.put(
                        MVCCKey::<'static>::build(key.bytes(), commit_sequence_number, StorageOperation::Insert)
                            .bytes(),
                        value.bytes(),
                    ),
                    Write::InsertPreexisting(value, reinsert) => {
                        if reinsert.load(Ordering::SeqCst) {
                            write_batch.put(
                                MVCCKey::<'static>::build(
                                    key.bytes(),
                                    commit_sequence_number,
                                    StorageOperation::Insert,
                                )
                                .bytes(),
                                value.bytes(),
                            )
                        }
                    }
                    Write::RequireExists(_) => {}
                    Write::Delete => write_batch.put(
                        MVCCKey::<'static>::build(key.bytes(), commit_sequence_number, StorageOperation::Delete)
                            .bytes(),
                        [],
                    ),
                });
                write_batches[index] = Some(write_batch);
            }
        });
        write_batches
    }

    pub fn closed_snapshot_write(&self, open_sequence_number: &SequenceNumber) {
        self.isolation_manager.closed(open_sequence_number)
    }

    fn get_keyspace(&self, keyspace_id: KeyspaceId) -> &Keyspace {
        let keyspace_index = self.keyspaces_index[keyspace_id as usize].unwrap();
        self.keyspaces.get(keyspace_index as usize).unwrap()
    }

    fn checkpoint(&self) {
        todo!("Checkpointing not yet implemented")
        // Steps:
        //  Each keyspace should checkpoint
    }

    pub fn delete_storage(self) -> Result<(), Vec<MVCCStorageError>> {
        let errors: Vec<MVCCStorageError> = self
            .keyspaces
            .into_iter()
            .map(|keyspace| keyspace.delete())
            .filter(|result| result.is_err())
            .map(|result| MVCCStorageError {
                storage_name: self.owner_name.as_ref().to_owned(),
                kind: MVCCStorageErrorKind::KeyspaceDeleteError { source: result.unwrap_err() },
            })
            .collect();
        if errors.is_empty() {
            if !self.path.exists() {
                Ok(())
            } else {
                match std::fs::remove_dir_all(self.path.clone()) {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("Failed to delete storage {}, received error: {}", self.owner_name, e);
                        Err(vec![MVCCStorageError {
                            storage_name: self.owner_name.as_ref().to_owned(),
                            kind: MVCCStorageErrorKind::FailedToDeleteStorage { source: e },
                        }])
                    }
                }
            }
        } else {
            Err(errors)
        }
    }

    pub fn get<M, V>(
        &self,
        key: StorageKeyReference<'_>,
        open_sequence_number: &SequenceNumber,
        mut mapper: M,
    ) -> Option<V>
    where
        M: FnMut(ByteReference<'_>) -> V,
    {
        let mut iterator = self.iterate_prefix(StorageKey::<8>::Reference(key), open_sequence_number);
        // TODO: we don't want to panic on unwrap here
        iterator.next().transpose().unwrap_or_log().map(|(_, value)| mapper(value))
    }

    pub fn iterate_prefix<'this, const PS: usize>(
        &'this self,
        prefix: StorageKey<'this, PS>,
        open_sequence_number: &SequenceNumber,
    ) -> MVCCPrefixIterator<'this, PS> {
        MVCCPrefixIterator::new(self, prefix, open_sequence_number)
    }

    // --- direct access to storage, bypassing MVCC and returning raw key/value pairs ---

    pub fn put_raw(&self, key: StorageKeyReference<'_>, value: &ByteArrayOrRef<'_, BUFFER_VALUE_INLINE>) {
        // TODO: writes should always have to go through a transaction? Otherwise we have to WAL right here in a different path
        self.get_keyspace(key.keyspace_id())
            .put(key.bytes(), value.bytes())
            .map_err(|e| MVCCStorageError {
                storage_name: self.owner_name.as_ref().to_owned(),
                kind: MVCCStorageErrorKind::KeyspaceError {
                    source: Arc::new(e),
                    keyspace: self.get_keyspace(key.keyspace_id()).name().to_owned(),
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
                storage_name: self.owner_name.as_ref().to_owned(),
                kind: MVCCStorageErrorKind::KeyspaceError {
                    source: Arc::new(e),
                    keyspace: self.get_keyspace(key.keyspace_id()).name().to_owned(),
                },
                // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
            })
            .unwrap_or_log()
    }

    pub fn get_prev_raw<M, T>(&self, key: StorageKeyReference<'_>, mut key_value_mapper: M) -> Option<T>
    where
        M: FnMut(&[u8], &[u8]) -> T,
    {
        self.get_keyspace(key.keyspace_id()).get_prev(key.bytes(), |k, v| key_value_mapper(k, v))
    }

    pub fn iterate_keyspace_prefix<'this, const PREFIX_INLINE: usize>(
        &'this self,
        prefix: StorageKey<'this, PREFIX_INLINE>,
    ) -> KeyspacePrefixIterator<'this, PREFIX_INLINE> {
        debug_assert!(!prefix.bytes().is_empty());
        self.get_keyspace(prefix.keyspace_id()).iterate_prefix(prefix.into_byte_array_or_ref())
    }
}

pub struct MVCCPrefixIterator<'a, const PS: usize> {
    storage: &'a MVCCStorage,
    keyspace: &'a Keyspace,
    iterator: KeyspacePrefixIterator<'a, PS>,
    open_sequence_number: SequenceNumber,
    last_visible_key: Option<ByteArray<MVCC_KEY_INLINE_SIZE>>,
    state: State<Arc<KeyspaceError>>,
}

impl<'s, const P: usize> MVCCPrefixIterator<'s, P> {
    //
    // TODO: optimisation for fixed-width keyspaces: we can skip to key[len(key) - 1] = key[len(key) - 1] + 1 once we find a successful key, to skip all 'older' versions of the key
    //
    fn new(storage: &'s MVCCStorage, prefix: StorageKey<'s, P>, open_sequence_number: &SequenceNumber) -> Self {
        debug_assert!(!prefix.bytes().is_empty());
        let keyspace = storage.get_keyspace(prefix.keyspace_id());
        let iterator = keyspace.iterate_prefix(prefix.into_byte_array_or_ref());
        MVCCPrefixIterator {
            storage,
            keyspace,
            iterator,
            open_sequence_number: *open_sequence_number,
            last_visible_key: None,
            state: State::Init,
        }
    }

    fn peek(&mut self) -> Option<Result<(StorageKeyReference, ByteReference), MVCCStorageError>> {
        match &self.state {
            State::Init => {
                self.find_next_state();
                self.peek()
            }
            State::ItemReady => {
                let (key, value) = self.iterator.peek().unwrap().unwrap();
                let mvcc_key = MVCCKey::wrap_slice(key);
                Some(Ok((
                    StorageKeyReference::new(self.keyspace.id(), mvcc_key.into_key().unwrap_reference()),
                    ByteReference::new(value),
                )))
            }
            State::ItemUsed => {
                self.advance_and_find_next_state();
                self.peek()
            }
            State::Error(error) => Some(Err(MVCCStorageError {
                storage_name: self.storage.owner_name.to_string(),
                kind: MVCCStorageErrorKind::KeyspaceError {
                    source: error.clone(),
                    keyspace: self.keyspace.name().to_owned(),
                },
            })),
            State::Done => None,
        }
    }

    fn next(&mut self) -> Option<Result<(StorageKeyReference, ByteReference), MVCCStorageError>> {
        match &self.state {
            State::Init => {
                self.find_next_state();
                self.next()
            }
            State::ItemReady => {
                let (key, value) = self.iterator.peek().unwrap().unwrap();
                let mvcc_key = MVCCKey::wrap_slice(key);
                let item = Some(Ok((
                    StorageKeyReference::new(self.keyspace.id(), mvcc_key.into_key().unwrap_reference()),
                    ByteReference::new(value),
                )));
                self.state = State::ItemUsed;
                item
            }
            State::ItemUsed => {
                self.advance_and_find_next_state();
                self.next()
            }
            State::Error(error) => Some(Err(MVCCStorageError {
                storage_name: self.storage.owner_name.to_string(),
                kind: MVCCStorageErrorKind::KeyspaceError {
                    source: error.clone(),
                    keyspace: self.keyspace.name().to_owned(),
                },
            })),
            State::Done => None,
        }
    }

    fn seek(&mut self) {
        todo!()
    }

    fn find_next_state(&mut self) {
        assert!(matches!(&self.state, &State::Init) || matches!(&self.state, &State::ItemUsed));
        while matches!(&self.state, &State::Init) || matches!(&self.state, &State::ItemUsed) {
            let peek = self.iterator.peek();
            match peek {
                None => self.state = State::Done,
                Some(Ok((key, _))) => {
                    let mvcc_key = MVCCKey::wrap_slice(key);
                    let is_visible =
                        Self::is_visible_key(&self.open_sequence_number, &self.last_visible_key, &mvcc_key);
                    if is_visible {
                        self.last_visible_key = Some(ByteArray::copy(mvcc_key.key()));
                        match mvcc_key.operation() {
                            StorageOperation::Insert => self.state = State::ItemReady,
                            StorageOperation::Delete => {}
                        }
                    } else {
                        self.advance()
                    }
                }
                Some(Err(error)) => self.state = State::Error(Arc::new(error)),
            }
        }
    }

    fn is_visible_key(
        open_sequence_number: &SequenceNumber,
        last_visible_key: &Option<ByteArray<128>>,
        mvcc_key: &MVCCKey,
    ) -> bool {
        (last_visible_key.is_none() || last_visible_key.as_ref().unwrap().bytes() != mvcc_key.key())
            && mvcc_key.is_visible_to(open_sequence_number)
    }

    fn advance_and_find_next_state(&mut self) {
        assert!(matches!(self.state, State::ItemUsed));
        self.advance();
        self.find_next_state();
    }

    fn advance(&mut self) {
        let _ = self.iterator.next();
    }

    pub fn collect_cloned<const INLINE_KEY: usize, const INLINE_VALUE: usize>(
        mut self,
    ) -> Result<Vec<(StorageKeyArray<INLINE_KEY>, ByteArray<INLINE_VALUE>)>, MVCCStorageError> {
        let mut vec = Vec::new();
        loop {
            let item = self.next();
            match item {
                None => {
                    break;
                }
                Some(Err(err)) => {
                    return Err(err);
                }
                Some(Ok((key_ref, value_ref))) => {
                    vec.push((StorageKeyArray::from(key_ref), ByteArray::from(value_ref)))
                }
            }
        }
        Ok(vec)
    }
}

///
/// MVCC keys are made of three parts: the [KEY][SEQ][OP]
///
struct MVCCKey<'bytes> {
    bytes: ByteArrayOrRef<'bytes, MVCC_KEY_INLINE_SIZE>,
}

// byte array inline size can be adjusted to avoid allocation since these key are often short-lived
const MVCC_KEY_INLINE_SIZE: usize = 128;

impl<'bytes> MVCCKey<'bytes> {
    const OPERATION_START_NEGATIVE_OFFSET: usize = StorageOperation::serialised_len();
    const SEQUENCE_NUMBER_START_NEGATIVE_OFFSET: usize =
        MVCCKey::OPERATION_START_NEGATIVE_OFFSET + SequenceNumber::serialised_len();

    fn build(key: &[u8], sequence_number: &SequenceNumber, storage_operation: StorageOperation) -> MVCCKey<'bytes> {
        let length = key.len() + SequenceNumber::serialised_len() + StorageOperation::serialised_len();
        let mut byte_array = ByteArray::zeros(length);
        let bytes = byte_array.bytes_mut();

        let key_end = key.len();
        let sequence_number_end = key_end + SequenceNumber::serialised_len();
        let operation_end = sequence_number_end + StorageOperation::serialised_len();

        bytes[0..key_end].copy_from_slice(key);
        sequence_number.invert().serialise_be_into(&mut bytes[key_end..sequence_number_end]);
        bytes[sequence_number_end..operation_end].copy_from_slice(storage_operation.bytes());

        MVCCKey { bytes: ByteArrayOrRef::Array(byte_array) }
    }

    fn wrap_slice(bytes: &'bytes [u8]) -> MVCCKey<'bytes> {
        MVCCKey { bytes: ByteArrayOrRef::Reference(ByteReference::new(bytes)) }
    }

    pub(crate) fn is_visible_to(&self, sequence_number: &SequenceNumber) -> bool {
        self.sequence_number() <= *sequence_number
    }

    fn bytes(&'bytes self) -> &'bytes [u8] {
        self.bytes.bytes()
    }

    fn length(&self) -> usize {
        self.bytes.length()
    }

    fn key(&'bytes self) -> &'bytes [u8] {
        &self.bytes()[0..(self.length() - MVCCKey::SEQUENCE_NUMBER_START_NEGATIVE_OFFSET)]
    }

    fn into_key(self) -> ByteArrayOrRef<'bytes, MVCC_KEY_INLINE_SIZE> {
        let end = self.length() - MVCCKey::SEQUENCE_NUMBER_START_NEGATIVE_OFFSET;
        let bytes = self.bytes;
        bytes.truncate(end)
    }

    fn sequence_number(&self) -> SequenceNumber {
        let sequence_number_start = self.length() - MVCCKey::SEQUENCE_NUMBER_START_NEGATIVE_OFFSET;
        let sequence_number_end = sequence_number_start + SequenceNumber::serialised_len();
        let inverse_sequence_number_bytes = &self.bytes()[sequence_number_start..sequence_number_end];
        debug_assert_eq!(SequenceNumber::serialised_len(), inverse_sequence_number_bytes.len());
        SequenceNumber::new(U80::from_be_bytes(inverse_sequence_number_bytes)).invert()
    }

    fn operation(&self) -> StorageOperation {
        let operation_byte_offset = self.length() - MVCCKey::OPERATION_START_NEGATIVE_OFFSET;
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
