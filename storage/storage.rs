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

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;

use logger::error;
use logger::result::ResultExt;
use speedb::{DB, Options, ReadOptions, WriteBatch, WriteOptions};
use durability::{Record, SequenceNumber, DurabilityService, wal::WAL, Sequencer};

use crate::error::{StorageError, StorageErrorKind};
use crate::isolation_manager::IsolationManager;
use crate::key_value::{Key, Value};
use crate::snapshot::{ReadSnapshot, WriteData, WriteSnapshot};

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
    durability_service: WAL,
    // TODO: want to inject either a remote or local service
    isolation_manager: IsolationManager,
}

impl Storage {
    const STORAGE_DIR_NAME: &'static str = "storage";

    pub fn new(owner_name: Rc<str>, path: &PathBuf) -> Result<Self, StorageError> {
        let kv_storage_dir = path.with_extension(Storage::STORAGE_DIR_NAME);
        let durability_service = WAL::new();
        Ok(Storage {
            owner_name: owner_name.clone(),
            path: kv_storage_dir,
            sections: Vec::new(),
            section_index: [0; SECTION_ID_MAX],
            durability_service: durability_service,
            isolation_manager: IsolationManager::new(durability_service.poll_next()),
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

        let open_sequence_number = self.isolation_manager.last_committed();
        WriteSnapshot::new(self, open_sequence_number)
    }

    pub fn snapshot_read<'storage>(&'storage self) -> ReadSnapshot<'storage> {
        let open_sequence_number = self.isolation_manager.last_committed();
        ReadSnapshot::new(self, open_sequence_number)
    }

    pub fn snapshot_commit<'storage>(&'storage self, snapshot: WriteSnapshot<'storage>) -> Result<(), StorageError> {
        let commit_record = snapshot.into_commit_record();

        //  1. make durable and get sequence number
        let write_batches = self.to_write_batches(commit_record.writes());
        let commit_sequence_number = self.durability_service.sequenced_write(WALRecordCommit::new(&write_batches));

        //  2. notify committed to isolation manager
        self.isolation_manager.notify_commit_pending(open_sequence_number, commit_record);

        //  3. validate against concurrent transactions in the given order.
        // TODO decide if we should block until all predecessors finish, allow out of order (non-Calvin model/applicable for non-distributed), or validate against all predecessors even if they are validating and fail eagerly.
        for predecessor_commit_record in self.isolation_manager.iterate_commits_between(open_sequence_number, commit_sequence_number) {
            self.isolation_manager.validate_isolation(predecessor_commit_record, &commit_record);
        }

        //  4. write to kv-storage
        for (index, write_batch) in write_batches.into_iter().enumerate() {
            debug_assert!(index < SECTION_ID_MAX);
            if write_batch.is_some() {
                self.get_section(index as SectionId).write(write_batch.unwrap())
                    .map_err(|error| StorageError {
                        storage_name: self.owner_name.as_ref().to_owned(),
                        kind: StorageErrorKind::SectionError { source: error }
                    })?;
            }
        }
        Ok(())
    }

    fn to_write_batches(&self, writes: &WriteData) -> [Option<WriteBatch>; SECTION_ID_MAX] {
        const EMPTY_WRITE_BATCH: Option<WriteBatch> = None;
        let mut write_batches: [Option<WriteBatch>; SECTION_ID_MAX] = [EMPTY_WRITE_BATCH; SECTION_ID_MAX];

        // let mut write_batches: Box<[Option<WriteBatch>]> = (0..SECTIONS_MAX).map(|_| None).collect::<Vec<Option<WriteBatch>>>().into_boxed_slice();
        writes.iter().for_each(|(key, value)| {
            let section_id = key.section_id() as usize;
            if write_batches[section_id].is_none() {
                write_batches[section_id] = Some(WriteBatch::default());
            }
            write_batches[section_id].as_mut().unwrap().put(key.bytes(), value.bytes())
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

struct WALRecordCommit {
    data: Box<[u8]>,
}

impl WALRecordCommit {
    const WB_SIZE_BYTES: usize = std::mem::size_of::<u64>();

    // TODO: should this serialise the BTree writes directly?
    fn new(commit_data: &[Option<WriteBatch>]) -> WALRecordCommit {
        let data_size = commit_data.iter()
            .map(|wb| wb.as_ref().map_or(0, |batch| WALRecordCommit::WB_SIZE_BYTES + batch.size_in_bytes()))
            .sum();

        let mut data: Box<[u8]> = vec![0; data_size].into_boxed_slice();
        let mut index: usize = 0;

        for write_batch in commit_data.iter().flatten() {
            let length_start = index;
            let length_end = length_start + std::mem::size_of::<u64>();
            let data_end = length_end + write_batch.size_in_bytes();
            index = data_end;

            data[length_start..length_end].copy_from_slice(&write_batch.size_in_bytes().to_be_bytes());
            data[length_end..data_end].copy_from_slice(write_batch.data());
        }
        WALRecordCommit {
            data: data
        }
    }
}

impl Record for WALRecordCommit {
    fn as_bytes(&self) -> &[u8] {
        todo!()
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


