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
use speedb::{DB, Options, WriteBatch, WriteBatchWithTransaction};
use wal::{Record, SequenceNumber, WAL};

use crate::durability_service::DurabilityService;
use crate::error::{StorageError, StorageErrorKind};
use crate::isolation_manager::IsolationManager;
use crate::key::{Key};
use crate::snapshot::{ReadSnapshot, WriteSnapshot};

pub mod error;
pub mod key;
pub mod snapshot;
mod durability_service;
mod isolation_manager;

pub struct Storage {
    owner_name: Rc<str>,
    path: PathBuf,
    sections: Vec<Section>,
    section_index: [u8; 256],
    durability_service: WAL,
    // TODO: want to inject either a remote or local service
    isolation_manager: IsolationManager,


    // TODO eliminate
    empty: Box<[u8]>
}

impl Storage {
    const STORAGE_DIR_NAME: &'static str = "storage";

    pub fn new(owner_name: Rc<str>, path: &PathBuf) -> Result<Self, StorageError> {
        let kv_storage_dir = path.with_extension(Storage::STORAGE_DIR_NAME);
        Ok(Storage {
            owner_name: owner_name.clone(),
            path: kv_storage_dir,
            sections: Vec::new(),
            section_index: [0; 256],
            durability_service: WAL::new(),
            isolation_manager: IsolationManager::new(),
            empty: Box::new([0; 0]),
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

    pub fn create_section(&mut self, name: &str, prefix: u8, options: &Options) -> Result<(), StorageError> {
        let section_path = self.path.with_extension(name);
        self.validate_new_section(name, prefix)?;
        self.sections.push(Section::new(name, section_path, prefix, options).map_err(|err| StorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: StorageErrorKind::SectionError { source: err },
        })?);
        self.section_index[prefix as usize] = self.sections.len() as u8 - 1;
        Ok(())
    }

    fn validate_new_section(&self, name: &str, prefix: u8) -> Result<(), StorageError> {
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
            } else if section.section_id == prefix {
                return Err(StorageError {
                    storage_name: self.owner_name.as_ref().to_owned(),
                    kind: StorageErrorKind::SectionError {
                        source: SectionError {
                            section_name: name.to_owned(),
                            kind: SectionErrorKind::FailedToCreateSectionIDExists {
                                id: prefix,
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
        WriteSnapshot::new(self, SequenceNumber { number: 0 })
    }

    pub fn snapshot_read<'storage>(&'storage self) -> ReadSnapshot<'storage> {
        ReadSnapshot::new(self, SequenceNumber { number: 0 })
    }

    pub fn snapshot_commit<'storage>(&'storage self, snapshot: WriteSnapshot<'storage>) {
        let (writes, open_sequence_number) = snapshot.into_data();
        //  1. make durable and get sequence number
        // TODO: create a set of serialised WriteBatch, one per Section, which are serialised to WAL record directly
        let commit_sequence_number = self.durability_service.sequenced_write(WALRecordCommit::new(&writes));
        //  2. notify committed to isolation manager (must be immediate so waiting txn's don't spin long)
        let writes_shared = Rc::new(writes);
        self.isolation_manager.notify_commit(open_sequence_number, commit_sequence_number, writes_shared.clone());
        // steps:
        //  3. validate against concurrent transactions in the given order.
        // TODO decide if we should block until all predecessors finish, allow out of order (non-Calvin model), or validate against all predecessors even if they are validating and fail eagerly.

        // TODO implement

        //  4. write to kv-storage
        // TODO - commit write batches directly to avoid re-creating structures
        writes_shared.iter().for_each(|(key, value)| self.put(key, value));
    }

    pub fn put(&self, key: &Key, value: &Option<Box<[u8]>>) {
        // TODO: writes should always have to go through a transaction? Otherwise we have to WAL right here in a different path
        let value_bytes = value.as_ref().map_or_else(|| &self.empty, |v| v);
        self.get_section(key.section_id()).put(key.bytes(), value_bytes).map_err(|e| StorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: StorageErrorKind::SectionError { source: e },
        }).unwrap_or_log()
    }

    pub fn get(&self, key: &Key) -> Option<Box<[u8]>> {
        self.get_section(key.section_id()).get(key.bytes()).map_err(|e| StorageError {
            storage_name: self.owner_name.as_ref().to_owned(),
            kind: StorageErrorKind::SectionError { source: e },
            // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
        }).unwrap_or_log()
    }

    pub fn get_prev(&self, key: &Key) -> Option<Box<[u8]>> {
        self.get_section(key.section_id()).get_prev(key.bytes())
    }

    pub fn iterate_prefix<'s>(&'s self, prefix: &Key) -> impl Iterator<Item=(Box<[u8]>, Option<Box<[u8]>>)> + 's {
        debug_assert!(prefix.bytes().len() > 1);
        self.get_section(prefix.section_id()).iterate_prefix(prefix.bytes())
            .map(|res| {
                match res {
                    Ok(v) => Ok(v),
                    Err(error) => Err(StorageError {
                        storage_name: self.owner_name.as_ref().to_owned(),
                        kind: StorageErrorKind::SectionError { source: error },
                    })
                    // TODO: unwrap_or_log may be incorrect: this could trigger if the DB is deleted for example?
                }.unwrap_or_log()
            })
    }

    fn get_section(&self, section_id: u8) -> &Section {
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
    section_id: u8,
    kv_storage: DB,
    next_checkpoint_id: u64,
}

impl Section {
    fn new(name: &str, path: PathBuf, section_id: u8, options: &Options) -> Result<Self, SectionError> {
        let kv_storage = DB::open(&options, &path)
            .map_err(|e| SectionError {
                section_name: name.to_owned(),
                kind: SectionErrorKind::FailedToCreateSectionError { source: e },
            })?;
        Ok(Section {
            name: name.to_owned(),
            path: path,
            section_id,
            kv_storage: kv_storage,
            next_checkpoint_id: 0,
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

    pub fn new_options() -> Options {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.enable_statistics();
        options.set_max_background_jobs(4);
        options
    }

    fn put(&self, key: &[u8], value: &Box<[u8]>) -> Result<(), SectionError> {
        // TODO: this should WAL if it is going to be exposed
        self.kv_storage.put(key, value)
            .map_err(|e| SectionError {
                section_name: self.name.clone(),
                kind: SectionErrorKind::FailedPut { source: e },
            })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Box<[u8]>>, SectionError> {
        self.kv_storage.get(key).map_err(|e| SectionError {
            section_name: self.name.clone(),
            kind: SectionErrorKind::FailedGet { source: e },
        }).map(|value| value.map(|v| v.into_boxed_slice()))
    }

    fn get_prev(&self, key: &[u8]) -> Option<Box<[u8]>> {
        let mut iterator = self.kv_storage.raw_iterator();
        iterator.seek_for_prev(key);
        iterator.key().map(|array_ref| array_ref.into())
    }

    // TODO: we should benchmark using iterator pools
    fn iterate_prefix<'s>(&'s self, prefix: &[u8]) -> impl Iterator<Item=Result<(Box<[u8]>, Option<Box<[u8]>>), SectionError>> + 's {
        self.kv_storage.prefix_iterator(prefix).map(|result| {
            match result {
                Ok((key, value)) => Ok((key, if value.is_empty() { None } else { Some(value) })),
                Err(error) => Err(SectionError {
                    section_name: self.name.clone(),
                    kind: SectionErrorKind::FailedIterate { source: error },
                })
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
    // TODO: replace with proper interface for serialised data
    data: Box<[(Box<[u8]>, Option<Box<[u8]>>)]>,
}

impl WALRecordCommit {
    fn new(commit_data: &BTreeMap<Key, Option<Box<[u8]>>>) -> WALRecordCommit {
        let data = commit_data
            .iter()
            .map(|(key, value)| {
                (key.bytes().to_vec().into_boxed_slice(), value.as_ref().map(|v| v.to_vec().into_boxed_slice()))
            }).collect::<Vec<_>>().into_boxed_slice();
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
    FailedToCreateSectionIDExists { id: u8, existing_section: String },
    FailedToGetSectionHandle {},
    FailedToDeleteSection { source: std::io::Error },
    FailedGet { source: speedb::Error },
    FailedPut { source: speedb::Error },
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
            SectionErrorKind::FailedIterate { source, .. } => Some(source),
        }
    }
}


