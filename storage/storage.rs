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
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use itertools::Itertools;
use speedb::{Options, WriteBatch};
use tracing::Value;

use bytes::byte_array::ByteArray;
use bytes::byte_reference::ByteReference;
use bytes::ByteArrayOrRef;
use durability::{DurabilityRecord, DurabilityService, SequenceNumber, Sequencer, wal::WAL};
use iterator::State;
use logger::error;
use logger::result::ResultExt;
use primitive::U80;
use snapshot::write::Write;

use crate::error::{MVCCStorageError, MVCCStorageErrorKind};
use crate::isolation_manager::{CommitRecord, IsolationManager};
use crate::key_value::{StorageKey, StorageKeyReference, StorageValue, StorageValueReference};
use crate::keyspace::keyspace::{Keyspace, KEYSPACE_ID_MAX, KEYSPACE_ID_RESERVED_UNSET, KeyspaceError, KeyspaceId, KeyspacePrefixIterator};
use crate::snapshot::buffer::{BUFFER_INLINE_VALUE, KeyspaceBuffers};
use crate::snapshot::snapshot::{ReadSnapshot, WriteSnapshot};

pub mod error;
pub mod key_value;
mod isolation_manager;
pub mod keyspace;
pub mod snapshot;

pub struct MVCCStorage {
    owner_name: Rc<str>,
    path: PathBuf,
    keyspaces: Vec<Keyspace>,
    keyspaces_index: [Option<KeyspaceId>; KEYSPACE_ID_MAX],
    // TODO: inject either a remote or local service
    durability_service: WAL,
    isolation_manager: IsolationManager,
}

impl MVCCStorage {
    const STORAGE_DIR_NAME: &'static str = "storage";
    pub(crate) const ITERATOR_INLINE_BYTES: usize = 128;

    pub fn new(owner_name: Rc<str>, path: &PathBuf) -> Result<Self, MVCCStorageError> {
        let storage_dir = path.with_extension(MVCCStorage::STORAGE_DIR_NAME);
        let mut durability_service = WAL::new();
        durability_service.register_record_type(CommitRecord::DURABILITY_RECORD_TYPE, CommitRecord::DURABILITY_RECORD_NAME);
        Ok(MVCCStorage {
            owner_name: owner_name.clone(),
            path: storage_dir,
            keyspaces: Vec::new(),
            keyspaces_index: core::array::from_fn(|_| None),
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
                kind: MVCCStorageErrorKind::KeyspaceIdReserved {
                    keyspace: name.to_owned(),
                    keyspace_id: keyspace_id,
                },
            });
        }
        for existing_keyspace_id in (0..self.keyspaces_index.len()) {
            if let Some(existing_keyspace_index) = self.keyspaces_index[existing_keyspace_id] {
                let keyspace = &self.keyspaces[existing_keyspace_index as usize];
                if keyspace.name() == name {
                    return Err(MVCCStorageError {
                        storage_name: self.owner_name.as_ref().to_owned(),
                        kind: MVCCStorageErrorKind::KeyspaceNameExists {
                            keyspace: name.to_owned(),
                        },
                    });
                } else if existing_keyspace_id as KeyspaceId == keyspace_id {
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
        let write_batches = self.to_write_batches(&commit_sequence_number, &commit_record.buffers());

        for (index, write_batch) in write_batches.into_iter().enumerate() {
            debug_assert!(index < KEYSPACE_ID_MAX);
            if write_batch.is_some() {
                self.get_keyspace(index as KeyspaceId).write(write_batch.unwrap())
                    .map_err(|error| MVCCStorageError {
                        storage_name: self.owner_name.as_ref().to_owned(),
                        kind: MVCCStorageErrorKind::KeyspaceError {
                            source: Arc::new(error),
                            keyspace: self.get_keyspace(index as KeyspaceId).name().to_owned(),
                        },
                    })?;
            }
        }
        Ok(())
    }

    fn to_write_batches(&self, commit_sequence_number: &SequenceNumber, buffers: &KeyspaceBuffers) -> [Option<WriteBatch>; KEYSPACE_ID_MAX] {
        let mut write_batches: [Option<WriteBatch>; KEYSPACE_ID_MAX] = core::array::from_fn(|_| None);

        buffers.iter().enumerate().for_each(|(index, buffer)| {
            let mut map = buffer.map().read().unwrap();
            if map.is_empty() {
                write_batches[index] = None
            } else {
                let mut write_batch = WriteBatch::default();
                map.iter().for_each(|(key, write)| {
                    match write {
                        Write::Insert(value) => {
                            write_batch.put(
                                MVCCKey::<'static>::build(key.bytes(), commit_sequence_number, StorageOperation::Insert).bytes(),
                                value.bytes(),
                            )
                        }
                        Write::InsertPreexisting(value, reinsert) => if reinsert.load(Ordering::SeqCst) {
                            write_batch.put(
                                MVCCKey::<'static>::build(key.bytes(), commit_sequence_number, StorageOperation::Insert).bytes(),
                                value.bytes(),
                            )
                        },
                        Write::RequireExists(_) => {}
                        Write::Delete => {
                            write_batch.put(
                                MVCCKey::<'static>::build(key.bytes(), commit_sequence_number, StorageOperation::Delete).bytes(),
                                [],
                            )
                        }
                    }
                });
                write_batches[index] = Some(write_batch);
            }
        });
        write_batches
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

    pub fn get<M, V, const S: usize>(&self, key: &StorageKey<'_, S>, open_sequence_number: &SequenceNumber, mut mapper: M) -> Option<V>
        where M: FnMut(StorageValueReference<'_>) -> V {
        let mut iterator = self.iterate_prefix(key, open_sequence_number);
        // TODO: we don't want to panic on unwrap here
        iterator.next().transpose().unwrap_or_log().map(|(_, value)|
            mapper(value)
        )
    }

    pub fn iterate_prefix<'this, const S: usize>(&'this self, prefix: &'this StorageKey<'this, S>, open_sequence_number: &SequenceNumber)
                                                 -> MVCCPrefixIterator {
        MVCCPrefixIterator::new(self, prefix, open_sequence_number)
    }

    // --- direct access to storage, bypassing MVCC and returning raw key/value pairs ---

    pub fn put_raw(&self, key: StorageKeyReference<'_>, value: &StorageValue<'_, BUFFER_INLINE_VALUE>) {
        // TODO: writes should always have to go through a transaction? Otherwise we have to WAL right here in a different path
        self.get_keyspace(key.keyspace_id()).put(key.byte_ref().bytes(), value.bytes()).map_err(|e| MVCCStorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: MVCCStorageErrorKind::KeyspaceError { source: Arc::new(e), keyspace: self.get_keyspace(key.keyspace_id()).name().to_owned() },
        }).unwrap_or_log()
    }

    pub fn get_raw<M, V>(&self, key: StorageKeyReference<'_>, mut mapper: M) -> Option<V>
        where M: FnMut(&[u8]) -> V {
        self.get_keyspace(key.keyspace_id()).get(
            key.byte_ref().bytes(),
            |value| mapper(value),
        ).map_err(|e| MVCCStorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: MVCCStorageErrorKind::KeyspaceError { source: Arc::new(e), keyspace: self.get_keyspace(key.keyspace_id()).name().to_owned() },
            // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
        }).unwrap_or_log()
    }

    pub fn get_prev_raw<KM, VM, K, V>(&self, key: StorageKeyReference<'_>, mut key_mapper: KM, mut value_mapper: VM) -> Option<(K, V)>
        where KM: FnMut(&[u8]) -> K, VM: FnMut(&[u8]) -> V {
        self.get_keyspace(key.keyspace_id()).get_prev(
            key.byte_ref().bytes(),
            |k, v| (key_mapper(k), value_mapper(v)),
        )
    }

    pub fn iterate_keyspace_prefix<'this>(&'this self, prefix: StorageKeyReference<'this>) -> KeyspacePrefixIterator<'this> {
        debug_assert!(prefix.bytes().len() > 0);
        self.get_keyspace(prefix.keyspace_id()).iterate_prefix(prefix.into_byte_ref().into_bytes())
    }
}

pub struct MVCCPrefixIterator<'a> {
    storage: &'a MVCCStorage,
    keyspace: &'a Keyspace,
    iterator: KeyspacePrefixIterator<'a>,
    open_sequence_number: SequenceNumber,
    last_visible_key: Option<ByteArray<MVCC_KEY_INLINE_SIZE>>,
    state: State<Arc<KeyspaceError>>,
}

impl<'s> MVCCPrefixIterator<'s> {
    //
    // TODO: optimisations for fixed-width keyspaces
    //
    fn new<const S: usize>(storage: &'s MVCCStorage, prefix: &'s StorageKey<'s, S>, open_sequence_number: &SequenceNumber) -> MVCCPrefixIterator<'s> {
        debug_assert!(prefix.bytes().len() > 0);
        let keyspace = storage.get_keyspace(prefix.keyspace_id());
        let iterator = keyspace.iterate_prefix(prefix.bytes());
        MVCCPrefixIterator {
            storage: storage,
            keyspace: keyspace,
            iterator: iterator,
            open_sequence_number: *open_sequence_number,
            last_visible_key: None,
            state: State::Init,
        }
    }

    fn peek(&mut self) -> Option<Result<(StorageKeyReference, StorageValueReference), MVCCStorageError>> {
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
                    StorageValueReference::new(ByteReference::new(value)),
                )))
            }
            State::ItemUsed => {
                self.advance_and_find_next_state();
                self.peek()
            }
            State::Error(error) => Some(Err(MVCCStorageError {
                storage_name: self.storage.owner_name.to_string(),
                kind: MVCCStorageErrorKind::KeyspaceError { source: error.clone(), keyspace: self.keyspace.name().to_owned() },
            })),
            State::Done => None
        }
    }

    fn next(&mut self) -> Option<Result<(StorageKeyReference, StorageValueReference), MVCCStorageError>> {
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
                    StorageValueReference::new(ByteReference::new(value)),
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
                kind: MVCCStorageErrorKind::KeyspaceError { source: error.clone(), keyspace: self.keyspace.name().to_owned() },
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
                Some(Ok((key, value))) => {
                    let mvcc_key = MVCCKey::wrap_slice(key);
                    let is_visible = (self.last_visible_key.is_none() || self.last_visible_key.as_ref().unwrap().bytes() != mvcc_key.key()) && mvcc_key.is_visible_to(&self.open_sequence_number);
                    if is_visible {
                        self.last_visible_key = Some(ByteArray::from(mvcc_key.key()));
                        match mvcc_key.operation() {
                            StorageOperation::Insert => self.state = State::ItemReady,
                            StorageOperation::Delete => {}
                        }
                    }
                }
                Some(Err(error)) => self.state = State::Error(Arc::new(error)),
            }
        }
    }

    fn advance_and_find_next_state(&mut self) {
        assert!(matches!(self.state, State::ItemUsed));
        let _ = self.iterator.next();
        self.find_next_state();
    }
}

mod visibility {
    // pub(crate) struct VariableWidthIteratorController<M, K, V> where M: FnMut(&[u8], &[u8]) -> (K, V) {
    //     last_visible_key: Option<ByteArray<{ MVCCStorage::ITERATOR_INLINE_BYTES }>>,
    //     open_sequence_number: SequenceNumber,
    //     mapper: M,
    // }
    //
    // impl<M, K, V> VariableWidthIteratorController<M, K, V> where M: FnMut(&[u8], &[u8]) -> (K, V) {
    //     pub(crate) fn new(open_sequence_number: &SequenceNumber, mapper: M) -> VariableWidthIteratorController<M, K, V> {
    //         VariableWidthIteratorController {
    //             last_visible_key: None,
    //             open_sequence_number: *open_sequence_number,
    //             mapper: mapper,
    //         }
    //     }
    // }
    //
    // impl<M, K, V> IteratorController<K, V> for VariableWidthIteratorController<M, K, V> where M: FnMut(&[u8], &[u8]) -> (K, V) {
    //     fn control<'a>(&mut self, sequenced_key: &'a [u8], value: &'a [u8]) -> IterControl<'a, K, V> {
    //         let mvcc_key = MVCCKey::wrap_slice(sequenced_key);
    //
    //         if self.is_new_visible_key(&mvcc_key) {
    //             self.last_visible_key = Some(ByteArray::from(mvcc_key.key()));
    //             match mvcc_key.operation() {
    //                 StorageOperation::Insert => {
    //                     IterControl::Accept(mvcc_key.into_key().unwrap_reference(), value)
    //                 }
    //                 StorageOperation::Delete => {
    //                     IterControl::IgnoreSingle
    //                 }
    //             }
    //         } else {
    //             IterControl::IgnoreSingle
    //         }
    //     }
    // }
    //
    // pub(crate) struct FixedWidthIteratorController {
    //     last_visible_key: Option<ByteArray<{ MVCCStorage::ITERATOR_INLINE_BYTES }>>,
    //     next_possible_key: Option<ByteArray<{ MVCCStorage::ITERATOR_INLINE_BYTES }>>,
    //     open_sequence_number: SequenceNumber,
    // }
    //
    // impl FixedWidthIteratorController {
    //     pub(crate) fn new(open_sequence_number: &SequenceNumber) -> FixedWidthIteratorController {
    //         FixedWidthIteratorController {
    //             last_visible_key: None,
    //             next_possible_key: None,
    //             open_sequence_number: *open_sequence_number,
    //         }
    //     }
    //
    //     pub(crate) fn control<'a>(&mut self, sequenced_key: &'a [u8], value: &'a [u8]) -> IterControl<'a, &'a [u8], &'a [u8]> {
    //         let mvcc_key = MVCCKey::wrap_slice(sequenced_key);
    //
    //         if self.last_visible_key.is_none() {
    //             if mvcc_key.is_visible_to(&self.open_sequence_number) {
    //                 self.control_new_visible_key(mvcc_key, value)
    //             } else {
    //                 IterControl::IgnoreSingle
    //             }
    //         } else {
    //             if self.last_visible_key.as_ref().unwrap().bytes() == mvcc_key.key() {
    //                 // skip over old versions of a previously accepted key
    //                 IterControl::IgnoreUntil(self.next_possible_key.as_ref().unwrap().bytes())
    //             } else if mvcc_key.is_visible_to(&self.open_sequence_number) {
    //                 self.control_new_visible_key(mvcc_key, value)
    //             } else {
    //                 IterControl::IgnoreSingle
    //             }
    //         }
    //     }
    //
    //     fn control_new_visible_key<'a>(&mut self, mvcc_key: MVCCKey, value: &'a [u8]) -> IterControl<'a, &[u8], &[u8]> {
    //         let last_key = ByteArray::from(mvcc_key.key());
    //
    //         // TODO: we should be able to append the sequence number + the first operation to this, to skip through keys and new versions of the expected key
    //         //       we should verify that this is going to work with prefix iterators when the prefix extractor is set to a shorter length?
    //         let mut next_key = last_key.clone();
    //         next_key.increment().unwrap_or_log();
    //         self.last_visible_key = Some(last_key);
    //         self.next_possible_key = Some(next_key);
    //         match mvcc_key.operation() {
    //             StorageOperation::Insert => {
    //                 IterControl::Accept(mvcc_key.into_key().unwrap_reference(), value)
    //             }
    //             StorageOperation::Delete => {
    //                 IterControl::IgnoreUntil(&self.next_possible_key.as_ref().unwrap().bytes())
    //             }
    //         }
    //     }
    //
    //     fn is_new_visible_key(&self, mvcc_key: &MVCCKey) -> bool {
    //         (self.last_visible_key.is_none() || self.last_visible_key.as_ref().unwrap().bytes() != mvcc_key.key())
    //             && mvcc_key.is_visible_to(&self.open_sequence_number)
    //     }
    // }

// pub(crate) fn read_mvcc<'bytes>((key, value): (&'bytes [u8], &'bytes [u8]), open_sequence_number: &SequenceNumber) -> MVCCRead<'bytes> {
//     let mvcc_key = MVCCKey::<'bytes>::wrap(key);
//     let operation = mvcc_key.operation();
//
//     if mvcc_key.is_visible_to(&open_sequence_number) {
//         match operation {
//             StorageOperation::Insert => MVCCRead::Visible(mvcc_key.into_key(), value),
//             StorageOperation::Delete => MVCCRead::Deleted,
//         }
//     } else {
//         MVCCRead::Hidden
//     }
// }
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
    const OPERATION_NEGATIVE_OFFSET: usize = StorageOperation::serialised_len();
    const SEQUENCE_NUMBER_NEGATIVE_OFFSET: usize = MVCCKey::OPERATION_NEGATIVE_OFFSET + StorageOperation::serialised_len();
    const KEY_NEGATIVE_OFFSET: usize = MVCCKey::SEQUENCE_NUMBER_NEGATIVE_OFFSET + SequenceNumber::serialised_len();

    fn build(key: &[u8], sequence_number: &SequenceNumber, storage_operation: StorageOperation) -> MVCCKey<'bytes> {
        let length = key.len() + SequenceNumber::serialised_len() + StorageOperation::serialised_len();
        let mut byte_array = ByteArray::zeros(length);
        let mut bytes = byte_array.bytes_mut();

        let key_end = key.len();
        let sequence_number_end = key_end + SequenceNumber::serialised_len();
        let operation_end = sequence_number_end + StorageOperation::serialised_len();

        bytes[0..key_end].copy_from_slice(key);
        sequence_number.invert().serialise_be_into(&mut bytes[key_end..sequence_number_end]);
        bytes[sequence_number_end..operation_end].copy_from_slice(storage_operation.bytes());

        MVCCKey {
            bytes: ByteArrayOrRef::Array(byte_array),
        }
    }

    fn wrap_owned(bytes: ByteArray<MVCC_KEY_INLINE_SIZE>) -> MVCCKey<'bytes> {
        MVCCKey {
            bytes: ByteArrayOrRef::Array(bytes),
        }
    }

    fn wrap_slice(bytes: &'bytes [u8]) -> MVCCKey<'bytes> {
        MVCCKey {
            bytes: ByteArrayOrRef::Reference(ByteReference::new(bytes))
        }
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
        &self.bytes()[0..(self.length() - MVCCKey::KEY_NEGATIVE_OFFSET)]
    }

    fn into_key(self) -> ByteArrayOrRef<'bytes, MVCC_KEY_INLINE_SIZE> {
        let end = self.length() - MVCCKey::KEY_NEGATIVE_OFFSET;
        let mut bytes = self.bytes;
        bytes.truncate(end)
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
