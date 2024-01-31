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
use std::fmt::{Display, Formatter};
use std::io::Read;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::Ordering;

use itertools::Itertools;
use speedb::{DB, Options, ReadOptions, WriteBatch, WriteOptions};

use durability::{DurabilityRecord, DurabilityService, SequenceNumber, Sequencer, wal::WAL};
use logger::error;
use logger::result::ResultExt;
use primitive::U80;

use crate::error::{StorageError, StorageErrorKind};
use crate::isolation_manager::{CommitRecord, IsolationManager};
use crate::key_value::{Key, Value};
use crate::snapshot::{ReadSnapshot, Write, WriteData, WriteSnapshot};

pub mod error;
pub mod key_value;
pub mod snapshot;
mod isolation_manager;

pub type SectionId = u8;

const SECTION_ID_MAX: usize = SectionId::MAX as usize;

pub struct Storage {
    owner_name: Rc<str>,
    path: PathBuf,
    sections: Vec<Section>,
    section_index: [SectionId; SECTION_ID_MAX],
    // TODO: inject either a remote or local service
    durability_service: WAL,
    isolation_manager: IsolationManager,
}

impl Storage {
    const STORAGE_DIR_NAME: &'static str = "storage";

    pub fn new(owner_name: Rc<str>, path: &PathBuf) -> Result<Self, StorageError> {
        let kv_storage_dir = path.with_extension(Storage::STORAGE_DIR_NAME);
        let mut durability_service = WAL::new();
        durability_service.register_record_type(CommitRecord::DURABILITY_RECORD_TYPE, CommitRecord::DURABILITY_RECORD_NAME);
        Ok(Storage {
            owner_name: owner_name.clone(),
            path: kv_storage_dir,
            sections: Vec::new(),
            section_index: [0; SECTION_ID_MAX],
            isolation_manager: IsolationManager::new(durability_service.poll_next()),
            durability_service: durability_service,
        })
    }

    // TODO: we want to be able to pass new options, since Rocks can handle rebooting with new options
    fn new_from_checkpoint(path: &PathBuf, durability_service: impl DurabilityService) -> Result<Self, StorageError> {
        todo!("Booting from checkpoint not yet implemented")

        // Steps:
        //   Load each section from their latest checkpoint
        //   Get the last known committed sequence number from each section
        //   Iterate over records from Durability Service from the earliest sequence number
        //   For each record, commit the records. Some sections will write duplicates, but all writes are idempotent so this is OK.

        // TODO: we have to be careful when we resume from a checkpoint and reapply the Records.Write.InsertPreexisting, to re-check if previous commits have deleted these keys
    }

    pub fn create_section(&mut self, name: &str, section_id: SectionId, options: &Options) -> Result<(), StorageError> {
        let section_path = self.path.with_extension(name);
        self.validate_new_section(name, section_id)?;
        self.sections.push(Section::new(name, section_path, section_id, options).map_err(|err| StorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: StorageErrorKind::SectionError { source: err },
        })?);
        self.section_index[section_id as usize] = self.sections.len() as SectionId - 1;
        Ok(())
    }

    fn validate_new_section(&self, name: &str, section_id: SectionId) -> Result<(), StorageError> {
        for section in &self.sections {
            if section.name == name {
                return Err(StorageError {
                    storage_name: self.owner_name.as_ref().to_owned(),
                    kind: StorageErrorKind::SectionError {
                        source: SectionError {
                            section_name: name.to_owned(),
                            kind: SectionErrorKind::FailedToCreateSectionNameExists {},
                        }
                    },
                });
            } else if section.section_id == section_id {
                return Err(StorageError {
                    storage_name: self.owner_name.as_ref().to_owned(),
                    kind: StorageErrorKind::SectionError {
                        source: SectionError {
                            section_name: name.to_owned(),
                            kind: SectionErrorKind::FailedToCreateSectionIDExists {
                                id: section_id,
                                existing_section: section.name.to_owned(),
                            },
                        }
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

    pub fn snapshot_commit<'storage>(&'storage self, snapshot: WriteSnapshot<'storage>) -> Result<(), StorageError> {
        let commit_record = snapshot.into_commit_record();

        //  1. make durable and get sequence number
        let commit_sequence_number = self.durability_service.sequenced_write(
            &commit_record, CommitRecord::DURABILITY_RECORD_NAME,
        ).map_err(|err| StorageError {
            storage_name: self.owner_name.to_string(),
            kind: StorageErrorKind::DurabilityError { source: err },
        })?;

        // 2. validate commit isolation
        let commit_record = self.isolation_manager.try_commit(commit_sequence_number, commit_record)
            .map_err(|err| StorageError {
                storage_name: self.owner_name.as_ref().to_owned(),
                kind: StorageErrorKind::IsolationError { source: err },
            })?;

        //  3. write to kv-storage
        let write_batches = self.to_write_batches(&commit_sequence_number, &commit_record.writes());

        for (index, write_batch) in write_batches.into_iter().enumerate() {
            debug_assert!(index < SECTION_ID_MAX);
            if write_batch.is_some() {
                self.get_section(index as SectionId).write(write_batch.unwrap())
                    .map_err(|error| StorageError {
                        storage_name: self.owner_name.as_ref().to_owned(),
                        kind: StorageErrorKind::SectionError { source: error },
                    })?;
            }
        }
        Ok(())
    }

    fn to_write_batches(&self, commit_sequence_number: &SequenceNumber, writes: &WriteData) -> [Option<WriteBatch>; SECTION_ID_MAX] {
        let mut write_batches: [Option<WriteBatch>; SECTION_ID_MAX] = core::array::from_fn(|_| None);

        writes.iter().for_each(|(key, write)| {
            let section_id = key.section_id() as usize;
            if write_batches[section_id].is_none() {
                write_batches[section_id] = Some(WriteBatch::default());
            }
            match write {
                Write::Insert(value) => {
                    write_batches[section_id].as_mut().unwrap().put(
                        StorageKey::new(key.bytes(), commit_sequence_number, StorageOperation::Insert).bytes(),
                        value.bytes(),
                    )
                }
                Write::InsertPreexisting(value, reinsert) => if reinsert.load(Ordering::SeqCst) {
                    write_batches[section_id].as_mut().unwrap().put(
                        StorageKey::new(key.bytes(), commit_sequence_number, StorageOperation::Insert).bytes(),
                        value.bytes(),
                    )
                },
                Write::RequireExists(_) => {}
                Write::Delete => {
                    write_batches[section_id].as_mut().unwrap().delete(
                        StorageKey::new(key.bytes(), commit_sequence_number, StorageOperation::Delete).bytes()
                    )
                }
            }
        });
        write_batches
    }

    pub fn put(&self, key: &Key, value: &Value) {
        // TODO: writes should always have to go through a transaction? Otherwise we have to WAL right here in a different path
        self.get_section(key.section_id()).put(key.bytes(), value.bytes()).map_err(|e| StorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: StorageErrorKind::SectionError { source: e },
        }).unwrap_or_log()
    }

    pub fn get(&self, key: &Key) -> Option<Value> {
        self.get_section(key.section_id()).get(key.bytes()).map_err(|e| StorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: StorageErrorKind::SectionError { source: e },
            // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
        }).unwrap_or_log()
    }

    pub fn get_prev(&self, key: &Key) -> Option<Value> {
        self.get_section(key.section_id()).get_prev(key.bytes())
    }

    pub fn iterate_prefix<'s>(&'s self, prefix: &Key) -> impl Iterator<Item=(Box<[u8]>, Value)> + 's {
        debug_assert!(prefix.bytes().len() > 1);
        self.get_section(prefix.section_id()).iterate_prefix(prefix.bytes())
            .map(|res| {
                match res {
                    Ok((k, v)) => Ok((k, Value::from(v))),
                    Err(error) => Err(StorageError {
                        storage_name: self.owner_name.as_ref().to_owned(),
                        kind: StorageErrorKind::SectionError { source: error },
                    })
                    // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
                }.unwrap_or_log()
            })
    }

    fn get_section(&self, section_id: SectionId) -> &Section {
        let section_index = self.section_index[section_id as usize];
        self.sections.get(section_index as usize).unwrap()
    }

    fn checkpoint(&self) {
        todo!("Checkpointing not yet implemented")
        // Steps:
        //  Each section should checkpoint
    }

    pub fn delete_storage(mut self) -> Result<(), Vec<StorageError>> {
        let errors: Vec<StorageError> = self.sections.into_iter().map(|section| section.delete_section())
            .filter(|result| result.is_err())
            .map(|result| StorageError {
                storage_name: self.owner_name.as_ref().to_owned(),
                kind: StorageErrorKind::SectionError { source: result.unwrap_err() },
            }).collect();
        if errors.is_empty() {
            if !self.path.exists() {
                return Ok(());
            } else {
                match std::fs::remove_dir_all(self.path.clone()) {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("Failed to delete storage {}, received error: {}", self.owner_name, e);
                        Err(vec!(StorageError {
                            storage_name: self.owner_name.as_ref().to_owned(),
                            kind: StorageErrorKind::FailedToDeleteStorage { source: e },
                        }))
                    }
                }
            }
        } else {
            Err(errors)
        }
    }
}


enum StorageKey {
    Small(StorageKeySmall),
    Large(StorageKeyLarge),
}

impl StorageKey {
    const OPERATION_NEGATIVE_OFFSET: usize = StorageOperation::serialised_len();
    const SEQUENCE_NUMBER_NEGATIVE_OFFSET: usize = StorageKey::OPERATION_NEGATIVE_OFFSET + StorageOperation::serialised_len();
    const KEY_NEGATIVE_OFFSET: usize = StorageKey::SEQUENCE_NUMBER_NEGATIVE_OFFSET + SequenceNumber::serialised_len();

    fn new(bytes: &[u8], sequence_number: &SequenceNumber, storage_operation: StorageOperation) -> StorageKey {
        let size = bytes.len() + SequenceNumber::serialised_len() + StorageOperation::serialised_len();

        if size < StorageKeySmall::BYTES {
            StorageKey::Small(StorageKeySmall::new(bytes, sequence_number, storage_operation))
        } else {
            StorageKey::Large(StorageKeyLarge::new(bytes, sequence_number, storage_operation))
        }
    }

    fn bytes(&self) -> &[u8] {
        match self {
            StorageKey::Small(key) => key.bytes(),
            StorageKey::Large(key) => key.bytes(),
        }
    }

    fn length(&self) -> usize {
        match self {
            StorageKey::Small(key) => key.length,
            StorageKey::Large(key) => key.bytes().len(),
        }
    }

    fn key(&self) -> &[u8] {
        &self.bytes()[0..(self.length() - StorageKey::KEY_NEGATIVE_OFFSET)]
    }

    fn sequence_number(&self) -> SequenceNumber {
        let sequence_number_offset = self.length() - StorageKey::KEY_NEGATIVE_OFFSET;
        let sequence_number_end = self.length() - StorageKey::SEQUENCE_NUMBER_NEGATIVE_OFFSET;
        let inverse_sequence_number_bytes = &self.bytes()[sequence_number_offset..sequence_number_end];
        debug_assert_eq!(SequenceNumber::serialised_len(), inverse_sequence_number_bytes.len());
        SequenceNumber::new(U80::from_be_bytes(inverse_sequence_number_bytes))
    }

    fn operation(&self) -> StorageOperation {
        let operation_byte_offset = self.length() - StorageKey::OPERATION_NEGATIVE_OFFSET;
        let operation_byte_end = self.length();
        StorageOperation::from(&self.bytes()[operation_byte_offset..operation_byte_end])
    }
}

struct StorageKeySmall {
    length: usize,
    bytes: [u8; StorageKeySmall::BYTES],
}

impl StorageKeySmall {
    const BYTES: usize = 64;

    fn new(bytes: &[u8], sequence_number: &SequenceNumber, storage_operation: StorageOperation) -> StorageKeySmall {
        let length = bytes.len() + SequenceNumber::serialised_len() + StorageOperation::serialised_len();
        debug_assert!(length <= StorageKeySmall::BYTES);
        let bytes_end = bytes.len();
        let sequence_number_end = bytes_end + SequenceNumber::serialised_len();
        let operation_end = sequence_number_end + StorageOperation::serialised_len();

        let mut storage_key_bytes = [0; StorageKeySmall::BYTES];
        storage_key_bytes[0..bytes_end].copy_from_slice(bytes);
        sequence_number.invert().serialise_be_into(&mut storage_key_bytes[bytes_end..sequence_number_end]);
        storage_key_bytes[sequence_number_end..operation_end].copy_from_slice(storage_operation.bytes());
        StorageKeySmall {
            length: length,
            bytes: storage_key_bytes,
        }
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes[0..self.length]
    }
}

struct StorageKeyLarge {
    bytes: Box<[u8]>,
}

impl StorageKeyLarge {
    fn new(bytes: &[u8], sequence_number: &SequenceNumber, storage_operation: StorageOperation) -> StorageKeyLarge {
        let mut vec = Vec::with_capacity(bytes.len() + SequenceNumber::serialised_len() + StorageOperation::serialised_len());
        vec.extend_from_slice(bytes);
        vec.extend_from_slice(&sequence_number.invert().serialise_be());
        vec.extend_from_slice(storage_operation.bytes());

        StorageKeyLarge {
            bytes: vec.into_boxed_slice()
        }
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes
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

pub struct Section {
    name: String,
    path: PathBuf,
    section_id: SectionId,
    kv_storage: DB,
    next_checkpoint_id: u64,
    read_options: ReadOptions,
    write_options: WriteOptions,
}

impl Section {
    fn new(name: &str, path: PathBuf, section_id: SectionId, options: &Options) -> Result<Self, SectionError> {
        let kv_storage = DB::open(&options, &path)
            .map_err(|e| SectionError {
                section_name: name.to_owned(),
                kind: SectionErrorKind::FailedToCreateSectionError { source: e },
            })?;
        Ok(Section {
            name: name.to_owned(),
            path: path,
            section_id: section_id,
            kv_storage: kv_storage,
            next_checkpoint_id: 0,
            read_options: Section::new_read_options(),
            write_options: Section::new_write_options(),
        })
    }

    // TODO: we want to be able to pass new options, since Rocks can handle rebooting with new options
    fn new_from_checkpoint(path: PathBuf) {
        todo!()
        // Steps:
        //  WARNING: this is intended to be DESTRUCTIVE since we may wipe anything partially written in the active directory
        //  Locate the directory with the latest number - say 'checkpoint_n'
        //  Delete 'active' directory.
        //  Rename directory called 'active' to 'checkpoint_x' -- TODO: do we need to delete 'active' or will re-checkpointing to it be clever enough to delete corrupt files?
        //  Rename 'checkpoint_x' to 'active'
        //  open DB at 'active'
    }

    pub fn new_db_options() -> Options {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.enable_statistics();
        options.set_max_background_jobs(4);
        options
    }

    fn new_read_options() -> ReadOptions {
        ReadOptions::default()
    }

    fn new_write_options() -> WriteOptions {
        let mut options = WriteOptions::default();
        // TODO; verify that disabling WAL is the final decision
        options.disable_wal(true);
        options
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), SectionError> {
        // TODO: this should WAL if it is going to be exposed
        self.kv_storage.put_opt(key, value, &self.write_options)
            .map_err(|e| SectionError {
                section_name: self.name.clone(),
                kind: SectionErrorKind::FailedPut { source: e },
            })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Value>, SectionError> {
        // TODO: if we can let Value decide whether to read Value into stack or heap automatically, we can avoid one heap allocation using GetPinnableSlice
        // TODO: even better if we pass in a transform function that maps &[u8] to T we're not restricted to Values
        self.kv_storage.get_opt(key, &self.read_options).map_err(|e| SectionError {
            section_name: self.name.clone(),
            kind: SectionErrorKind::FailedGet { source: e },
        }).map(
            |value| value.map(
                |v| if v.is_empty() { Value::Empty } else { Value::Value(v.into_boxed_slice()) })
        )
    }

    fn get_prev(&self, key: &[u8]) -> Option<Value> {
        let mut iterator = self.kv_storage.raw_iterator_opt(Section::new_read_options());
        iterator.seek_for_prev(key);
        iterator.key().map(|array_ref| Value::Value(array_ref.into()))
    }

    // TODO: we should benchmark using iterator pools
    fn iterate_prefix<'s>(&'s self, prefix: &[u8]) -> impl Iterator<Item=Result<(Box<[u8]>, Value), SectionError>> + 's {
        // TODO:
        self.kv_storage.prefix_iterator(prefix).map(|result| {
            match result {
                Ok((key, value)) => Ok((key, if value.is_empty() { Value::Empty } else { Value::Value(value) })),
                Err(error) => Err(SectionError {
                    section_name: self.name.clone(),
                    kind: SectionErrorKind::FailedIterate { source: error },
                })
            }
        })
    }

    fn write(&self, write_batch: WriteBatch) -> Result<(), SectionError> {
        self.kv_storage.write_opt(write_batch, &self.write_options).map_err(|error| {
            SectionError {
                section_name: self.name.clone(),
                kind: SectionErrorKind::FailedBatchWrite { source: error },
            }
        })
    }

    fn checkpoint(&self) -> Result<(), SectionError> {
        todo!()
        // Steps:
        //  Create new checkpoint directory at 'checkpoint_{next_checkpoint_id}'
        //  Take the last sequence number watermark
        //  Take a storage checkpoint into directory (may end up containing some more commits, which is OK)
        //  Write properties file: timestamp and last sequence number watermark
    }

    fn delete_section(self) -> Result<(), SectionError> {
        match std::fs::remove_dir_all(self.path.clone()) {
            Ok(_) => Ok(()),
            Err(e) => {
                Err(SectionError {
                    section_name: self.name.clone(),
                    kind: SectionErrorKind::FailedToDeleteSection { source: e },
                })
            }
        }
    }
}

#[derive(Debug)]
pub struct SectionError {
    pub section_name: String,
    pub kind: SectionErrorKind,
}

#[derive(Debug)]
pub enum SectionErrorKind {
    FailedToCreateSectionError { source: speedb::Error },
    FailedToCreateSectionNameExists {},
    FailedToCreateSectionIDExists { id: SectionId, existing_section: String },
    FailedToGetSectionHandle {},
    FailedToDeleteSection { source: std::io::Error },
    FailedGet { source: speedb::Error },
    FailedPut { source: speedb::Error },
    FailedBatchWrite { source: speedb::Error },
    FailedIterate { source: speedb::Error },
}

impl Display for SectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for SectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            SectionErrorKind::FailedToCreateSectionError { source, .. } => Some(source),
            SectionErrorKind::FailedToCreateSectionNameExists { .. } => None,
            SectionErrorKind::FailedToCreateSectionIDExists { .. } => None,
            SectionErrorKind::FailedToGetSectionHandle { .. } => None,
            SectionErrorKind::FailedToDeleteSection { source, .. } => Some(source),
            SectionErrorKind::FailedGet { source, .. } => Some(source),
            SectionErrorKind::FailedPut { source, .. } => Some(source),
            SectionErrorKind::FailedBatchWrite { source, .. } => Some(source),
            SectionErrorKind::FailedIterate { source, .. } => Some(source),
        }
    }
}


