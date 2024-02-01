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

use std::error::Error;
use std::fmt::Display;
use std::io::Read;
use std::iter::empty;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::Ordering;

use itertools::Itertools;
use speedb::{Options, WriteBatch};

use durability::{DurabilityRecord, DurabilityService, SequenceNumber, Sequencer, wal::WAL};
use logger::error;
use logger::result::ResultExt;
use primitive::U80;

use crate::error::{MVCCStorageError, MVCCStorageErrorKind};
use crate::error::MVCCStorageErrorKind::KeyspaceError;
use crate::isolation_manager::{CommitRecord, IsolationManager};
use crate::key_value::{KEYSPACE_ID_MAX, KeyspaceId, KeyspaceKey, Value};
use crate::kv::kv_storage::KVStorage;
use crate::snapshot::{ReadSnapshot, Write, WriteData, WriteSnapshot};

pub mod error;
pub mod key_value;
pub mod snapshot;
mod isolation_manager;
mod kv;

pub struct MVCCStorage {
    owner_name: Rc<str>,
    path: PathBuf,
    keyspaces: Vec<KVStorage>,
    keyspaces_index: [KeyspaceId; KEYSPACE_ID_MAX],
    // TODO: inject either a remote or local service
    durability_service: WAL,
    isolation_manager: IsolationManager,
}

impl MVCCStorage {
    const STORAGE_DIR_NAME: &'static str = "storage";

    pub fn new(owner_name: Rc<str>, path: &PathBuf) -> Result<Self, MVCCStorageError> {
        let kv_storage_dir = path.with_extension(MVCCStorage::STORAGE_DIR_NAME);
        let mut durability_service = WAL::new();
        durability_service.register_record_type(CommitRecord::DURABILITY_RECORD_TYPE, CommitRecord::DURABILITY_RECORD_NAME);
        Ok(MVCCStorage {
            owner_name: owner_name.clone(),
            path: kv_storage_dir,
            keyspaces: Vec::new(),
            keyspaces_index: [0; KEYSPACE_ID_MAX],
            isolation_manager: IsolationManager::new(durability_service.poll_next()),
            durability_service: durability_service,
        })
    }

    // TODO: we want to be able to pass new options, since Rocks can handle rebooting with new options
    fn new_from_checkpoint(path: &PathBuf, durability_service: impl DurabilityService) -> Result<Self, MVCCStorageError> {
        todo!("Booting from checkpoint not yet implemented")

        // Steps:
        //   Load each keyspace from their latest checkpoint
        //   Get the last known committed sequence number from each keyspace
        //   Iterate over records from Durability Service from the earliest sequence number
        //   For each record, commit the records. Some keyspaces will write duplicates, but all writes are idempotent so this is OK.

        // TODO: we have to be careful when we resume from a checkpoint and reapply the Records.Write.InsertPreexisting, to re-check if previous commits have deleted these keys
    }

    pub fn create_keyspace(&mut self, name: &str, keyspace_id: KeyspaceId, options: &Options) -> Result<(), MVCCStorageError> {
        let keyspace_path = self.path.with_extension(name);
        self.validate_new_keyspace(name, keyspace_id)?;
        self.keyspaces.push(KVStorage::new(keyspace_path, options).map_err(|err| MVCCStorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: MVCCStorageErrorKind::KeyspaceError { source: err, keyspace: name.to_owned() },
        })?);
        self.keyspaces_index[keyspace_id as usize] = self.keyspaces.len() as KeyspaceId - 1;
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
        for id in self.keyspaces_index {
            let keyspace = self.get_keyspace(id);
            if keyspace.name() == name {
                return Err(MVCCStorageError {
                    storage_name: self.owner_name.as_ref().to_owned(),
                    kind: MVCCStorageErrorKind::KeyspaceNameExists {
                        keyspace: name.to_owned(),
                    },
                });
            } else if id == keyspace_id {
                return Err(MVCCStorageError {
                    storage_name: self.owner_name.as_ref().to_owned(),
                    kind: MVCCStorageErrorKind::KeyspaceIdExists {
                        new_keyspace: name.to_owned(),
                        keyspace_id: keyspace_id,
                        existing_keyspace: keyspace.name().to_owned(),
                    },
                });
            }
        }
        Ok(())
    }

    pub fn snapshot_write<'storage>(&'storage self) -> WriteSnapshot<'storage> {
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

    pub fn snapshot_read<'storage>(&'storage self) -> ReadSnapshot<'storage> {
        let open_sequence_number = self.isolation_manager.watermark();
        ReadSnapshot::new(self, open_sequence_number)
    }

    pub fn snapshot_commit<'storage>(&'storage self, snapshot: WriteSnapshot<'storage>) -> Result<(), MVCCStorageError> {
        let commit_record = snapshot.into_commit_record();

        //  1. make durable and get sequence number
        let commit_sequence_number = self.durability_service.sequenced_write(
            &commit_record, CommitRecord::DURABILITY_RECORD_NAME,
        ).map_err(|err| MVCCStorageError {
            storage_name: self.owner_name.to_string(),
            kind: MVCCStorageErrorKind::DurabilityError { source: err },
        })?;

        // 2. validate commit isolation
        let commit_record = self.isolation_manager.try_commit(commit_sequence_number, commit_record)
            .map_err(|err| MVCCStorageError {
                storage_name: self.owner_name.as_ref().to_owned(),
                kind: MVCCStorageErrorKind::IsolationError { source: err },
            })?;

        //  3. write to kv-storage
        let write_batches = self.to_write_batches(&commit_sequence_number, &commit_record.writes());

        for (index, write_batch) in write_batches.into_iter().enumerate() {
            debug_assert!(index < KEYSPACE_ID_MAX);
            if write_batch.is_some() {
                self.get_keyspace(index as KeyspaceId).write(write_batch.unwrap())
                    .map_err(|error| MVCCStorageError {
                        storage_name: self.owner_name.as_ref().to_owned(),
                        kind: MVCCStorageErrorKind::KeyspaceError {
                            source: error,
                            keyspace: self.get_keyspace(index as KeyspaceId).name().to_owned(),
                        },
                    })?;
            }
        }
        Ok(())
    }

    fn to_write_batches(&self, commit_sequence_number: &SequenceNumber, writes: &WriteData) -> [Option<WriteBatch>; KEYSPACE_ID_MAX] {
        let mut write_batches: [Option<WriteBatch>; KEYSPACE_ID_MAX] = core::array::from_fn(|_| None);

        writes.iter().for_each(|(key, write)| {
            let keyspace_id = key.keyspace_id() as usize;
            if write_batches[keyspace_id].is_none() {
                write_batches[keyspace_id] = Some(WriteBatch::default());
            }
            match write {
                Write::Insert(value) => {
                    write_batches[keyspace_id].as_mut().unwrap().put(
                        MVCCKey::<'static>::build(key.bytes(), commit_sequence_number, StorageOperation::Insert).bytes(),
                        value.bytes(),
                    )
                }
                Write::InsertPreexisting(value, reinsert) => if reinsert.load(Ordering::SeqCst) {
                    write_batches[keyspace_id].as_mut().unwrap().put(
                        MVCCKey::<'static>::build(key.bytes(), commit_sequence_number, StorageOperation::Insert).bytes(),
                        value.bytes(),
                    )
                },
                Write::RequireExists(_) => {}
                Write::Delete => {
                    write_batches[keyspace_id].as_mut().unwrap().delete(
                        MVCCKey::build(key.bytes(), commit_sequence_number, StorageOperation::Delete).bytes()
                    )
                }
            }
        });
        write_batches
    }

    fn get_keyspace(&self, keyspace_id: KeyspaceId) -> &KVStorage {
        let keyspace_index = self.keyspaces_index[keyspace_id as usize];
        self.keyspaces.get(keyspace_index as usize).unwrap()
    }

    fn checkpoint(&self) {
        todo!("Checkpointing not yet implemented")
        // Steps:
        //  Each keyspace should checkpoint
    }

    pub fn delete_storage(mut self) -> Result<(), Vec<MVCCStorageError>> {
        let errors: Vec<MVCCStorageError> = self.keyspaces.into_iter()
            .map(|keyspace| keyspace.delete())
            .filter(|result| result.is_err())
            .map(|result| MVCCStorageError {
                storage_name: self.owner_name.as_ref().to_owned(),
                kind: MVCCStorageErrorKind::KeyspaceDeleteError { source: result.unwrap_err() },
            }).collect();
        if errors.is_empty() {
            if !self.path.exists() {
                return Ok(());
            } else {
                match std::fs::remove_dir_all(self.path.clone()) {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("Failed to delete storage {}, received error: {}", self.owner_name, e);
                        Err(vec!(MVCCStorageError {
                            storage_name: self.owner_name.as_ref().to_owned(),
                            kind: MVCCStorageErrorKind::FailedToDeleteStorage { source: e },
                        }))
                    }
                }
            }
        } else {
            Err(errors)
        }
    }

    pub fn get<R, V>(&self, key: &KeyspaceKey, open_sequence_number: &SequenceNumber, mut reader: R) -> Option<V>
        where R: FnMut(&[u8]) -> V {
        let result = self.get_keyspace(key.keyspace_id())
            .iterate_prefix(
                key.bytes(),
                |sequenced_key, value| {
                    MVCCStorage::mvcc_visibility((sequenced_key, value), open_sequence_number)
                        .map(|(k, v)| ((), reader(v)))
                },
            ).next();
        match result {
            None => None,
            Some(Err(error)) => Err(MVCCStorageError {
                storage_name: self.owner_name.to_string(),
                kind: KeyspaceError {
                    keyspace: self.get_keyspace(key.keyspace_id()).name().to_owned(),
                    source: error,
                },
            }).unwrap_or_log(),
            Some(Ok((k, v))) => Some(v)
        }
    }

    // --- direct access to storage, bypassing MVCC and returning raw key/value pairs ---

    pub fn put_direct(&self, key: &KeyspaceKey, value: &Value) {
        // TODO: writes should always have to go through a transaction? Otherwise we have to WAL right here in a different path
        self.get_keyspace(key.keyspace_id()).put(key.bytes(), value.bytes()).map_err(|e| MVCCStorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: MVCCStorageErrorKind::KeyspaceError { source: e, keyspace: self.get_keyspace(key.keyspace_id()).name().to_owned() },
        }).unwrap_or_log()
    }

    pub fn get_direct(&self, key: &KeyspaceKey) -> Option<Value> {
        self.get_keyspace(key.keyspace_id()).get(
            key.bytes(),
            |value| Value::Value(Box::from(value)),
        ).map_err(|e| MVCCStorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: MVCCStorageErrorKind::KeyspaceError { source: e, keyspace: self.get_keyspace(key.keyspace_id()).name().to_owned() },
            // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
        }).unwrap_or_log()
    }

    pub fn get_prev_direct(&self, key: &KeyspaceKey) -> Option<(Box<[u8]>, Value)> {
        self.get_keyspace(key.keyspace_id()).get_prev(
            key.bytes(),
                |k, v| (Box::from(k), Value::Value(Box::from(v)))
        )
    }

    pub fn iterate_prefix_direct<'s>(&'s self, prefix: &KeyspaceKey) -> impl Iterator<Item=(Box<[u8]>, Value)> + 's {
        // debug_assert!(prefix.bytes().len() > 1);
        // self.get_keyspace(prefix.keyspace_id()).iterate_prefix(prefix.bytes())
        //     .map(|res| {
        //         match res {
        //             Ok((k, v)) => Ok((k, Value::from(v))),
        //             Err(error) => Err(StorageError {
        //                 storage_name: self.owner_name.as_ref().to_owned(),
        //                 kind: StorageErrorKind::SectionError { source: error },
        //             })
        //             // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
        //         }.unwrap_or_log()
        //     })
        empty()
    }

    fn mvcc_visibility<'bytes>((key, value): (&'bytes [u8], &'bytes [u8]), open_sequence_number: &SequenceNumber) -> Visibility<'bytes> {
        let mvcc_key = MVCCKey::<'bytes>::wrap(key);
        let sequence_number = mvcc_key.sequence_number();

        if open_sequence_number <= &sequence_number {
            Some((mvcc_key.into_key(), value))
        } else {
            None
        }
    }
}

enum Visibility<'bytes> {
    Ignore,
    Visible(&'bytes [u8], &'bytes [u8]),
    Deleted,
}

///
/// MVCC keys are made of three parts: the [KEY][SEQ][OP]
///
/// Small keys are designed to be able to kept on the stack and live in a fixed size array
/// Large keys are heap stored and point to the heap, with the same structure
///
enum MVCCKey<'bytes> {
    Small(MVCCKeySmall),
    Large(MVCCKeyLarge),
    Ref(MVCCRef<'bytes>),
}

impl<'bytes> MVCCKey<'bytes> {
    const OPERATION_NEGATIVE_OFFSET: usize = StorageOperation::serialised_len();
    const SEQUENCE_NUMBER_NEGATIVE_OFFSET: usize = MVCCKey::OPERATION_NEGATIVE_OFFSET + StorageOperation::serialised_len();
    const KEY_NEGATIVE_OFFSET: usize = MVCCKey::SEQUENCE_NUMBER_NEGATIVE_OFFSET + SequenceNumber::serialised_len();

    fn build(bytes: &[u8], sequence_number: &SequenceNumber, storage_operation: StorageOperation) -> MVCCKey<'bytes> {
        let size = bytes.len() + SequenceNumber::serialised_len() + StorageOperation::serialised_len();

        if size < MVCCKeySmall::BYTES {
            MVCCKey::Small(MVCCKeySmall::new(bytes, sequence_number, storage_operation))
        } else {
            MVCCKey::Large(MVCCKeyLarge::new(bytes, sequence_number, storage_operation))
        }
    }

    fn wrap(bytes: &'bytes [u8]) -> MVCCKey<'bytes> {
        MVCCKey::Ref(MVCCRef::new(bytes))
    }

    fn bytes(&'bytes self) -> &'bytes [u8] {
        match self {
            MVCCKey::Small(key) => key.bytes(),
            MVCCKey::Large(key) => key.bytes(),
            MVCCKey::Ref(mvcc_ref) => mvcc_ref.bytes(),
        }
    }

    // TODO: this really means we should refactor into separate data structures
    fn into_bytes(self) -> &'bytes [u8] {
        match self {
            MVCCKey::Small(_) => unreachable!("Illegal state"),
            MVCCKey::Large(_) => unreachable!("Illegal state"),
            MVCCKey::Ref(mvcc_ref) => mvcc_ref.into_bytes(),
        }
    }

    fn length(&self) -> usize {
        match self {
            MVCCKey::Small(key) => key.length,
            MVCCKey::Large(key) => key.bytes().len(),
            MVCCKey::Ref(mvcc_ref) => mvcc_ref.bytes().len(),
        }
    }

    fn key(&'bytes self) -> &'bytes [u8] {
        &self.bytes()[0..(self.length() - MVCCKey::KEY_NEGATIVE_OFFSET)]
    }

    fn into_key(self) -> &'bytes [u8] {
        let length = self.length();
        &self.into_bytes()[0..(length - MVCCKey::KEY_NEGATIVE_OFFSET)]
    }

    fn sequence_number(&self) -> SequenceNumber {
        let sequence_number_offset = self.length() - MVCCKey::KEY_NEGATIVE_OFFSET;
        let sequence_number_end = self.length() - MVCCKey::SEQUENCE_NUMBER_NEGATIVE_OFFSET;
        let inverse_sequence_number_bytes = &self.bytes()[sequence_number_offset..sequence_number_end];
        debug_assert_eq!(SequenceNumber::serialised_len(), inverse_sequence_number_bytes.len());
        SequenceNumber::new(U80::from_be_bytes(inverse_sequence_number_bytes))
    }

    fn operation(&self) -> StorageOperation {
        let operation_byte_offset = self.length() - MVCCKey::OPERATION_NEGATIVE_OFFSET;
        let operation_byte_end = self.length();
        StorageOperation::from(&self.bytes()[operation_byte_offset..operation_byte_end])
    }
}

struct MVCCKeySmall {
    length: usize,
    bytes: [u8; MVCCKeySmall::BYTES],
}

impl MVCCKeySmall {
    const BYTES: usize = 64;

    fn new(bytes: &[u8], sequence_number: &SequenceNumber, storage_operation: StorageOperation) -> MVCCKeySmall {
        let length = bytes.len() + SequenceNumber::serialised_len() + StorageOperation::serialised_len();
        debug_assert!(length <= MVCCKeySmall::BYTES);
        let bytes_end = bytes.len();
        let sequence_number_end = bytes_end + SequenceNumber::serialised_len();
        let operation_end = sequence_number_end + StorageOperation::serialised_len();

        let mut storage_key_bytes = [0; MVCCKeySmall::BYTES];
        storage_key_bytes[0..bytes_end].copy_from_slice(bytes);
        sequence_number.invert().serialise_be_into(&mut storage_key_bytes[bytes_end..sequence_number_end]);
        storage_key_bytes[sequence_number_end..operation_end].copy_from_slice(storage_operation.bytes());
        MVCCKeySmall {
            length: length,
            bytes: storage_key_bytes,
        }
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes[0..self.length]
    }
}

struct MVCCKeyLarge {
    bytes: Box<[u8]>,
}

impl MVCCKeyLarge {
    fn new(bytes: &[u8], sequence_number: &SequenceNumber, storage_operation: StorageOperation) -> MVCCKeyLarge {
        let mut vec = Vec::with_capacity(bytes.len() + SequenceNumber::serialised_len() + StorageOperation::serialised_len());
        vec.extend_from_slice(bytes);
        vec.extend_from_slice(&sequence_number.invert().serialise_be());
        vec.extend_from_slice(storage_operation.bytes());

        MVCCKeyLarge {
            bytes: vec.into_boxed_slice()
        }
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

struct MVCCRef<'bytes> {
    bytes: &'bytes [u8],
}

impl<'bytes> MVCCRef<'bytes> {
    fn new(bytes: &'bytes [u8]) -> MVCCRef<'bytes> {
        MVCCRef {
            bytes: bytes,
        }
    }

    fn bytes(&self) -> &'bytes [u8] {
        self.bytes
    }

    fn into_bytes(self) -> &'bytes [u8] {
        self.bytes
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
            _ => panic!("Unrecognised storage operation bytes.")
        }
    }

    const fn serialised_len() -> usize {
        StorageOperation::BYTES
    }
}
