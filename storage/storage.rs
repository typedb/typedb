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
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use speedb::{DB, DBCommon, DBIteratorWithThreadMode, Options, SingleThreaded};
use logger::{error, trace};
use crate::error::{StorageError, StorageErrorKind};
use crate::key::Key;

mod snapshot;
pub mod error;
pub mod key;

pub struct Storage {
    name: Rc<str>,
    path: PathBuf,
    sections: Vec<Section>,
    section_index: [u8; 256],
}

impl Storage {
    const BYTES_EMPTY: [u8; 0] = [];

    pub fn new(name: &str, path: &PathBuf) -> Result<Self, StorageError> {
        let kv_storage_dir = path.with_extension(name);
        Ok(Storage {
            name: Rc::from(name.to_owned()),
            path: kv_storage_dir,
            sections: Vec::new(),
            section_index: [0; 256],
        })
    }

    pub fn create_section(&mut self, name: &str, prefix: u8, options: &Options) -> Result<(), StorageError> {
        let section_path = self.path.with_extension(name);
        self.validate_new_section(name, prefix)?;
        self.sections.push(Section::new(name, section_path, prefix, options).map_err(|err| StorageError {
            storage_name: self.name.as_ref().to_owned(),
            kind: StorageErrorKind::SectionError { source: err },
        })?);
        self.section_index[prefix as usize] = self.sections.len()  as u8 - 1;
        Ok(())
    }

    fn validate_new_section(&self, name: &str, prefix: u8) -> Result<(), StorageError> {
        for section in &self.sections {
            if section.name == name {
                return Err(StorageError {
                    storage_name: self.name.as_ref().to_owned(),
                    kind: StorageErrorKind::SectionError {
                        source: SectionError {
                            section_name: name.to_owned(),
                            kind: SectionErrorKind::FailedToCreateSectionNameExists {},
                        }
                    },
                })
            } else if section.prefix == prefix {
                return Err(StorageError {
                    storage_name: self.name.as_ref().to_owned(),
                    kind: StorageErrorKind::SectionError {
                        source: SectionError {
                            section_name: name.to_owned(),
                            kind: SectionErrorKind::FailedToCreateSectionPrefixExists {
                                prefix: prefix,
                                existing_section: section.name.to_owned(),
                            },
                        }
                    },
                })
            }
        }
        Ok(())
    }

    pub fn put(&self, key: &Key) -> Result<(), SectionError> {
        self.get_section(key.data[0]).put(&key.data)
    }

    pub fn get(&self, key: &Key) -> Result<Option<Vec<u8>>, SectionError> {
        self.get_section(key.data[0]).get(&key.data)
    }

    pub fn iterate_prefix<'s>(&'s self, prefix: Vec<u8>) -> impl Iterator<Item=Result<(Box<[u8]>, Box<[u8]>), speedb::Error>> + 's {
        debug_assert!(prefix.len() > 1);
        self.get_section(*prefix.get(0).unwrap()).iterate_prefix(prefix)
    }

    fn get_section(&self, prefix: u8) -> &Section {
        let section_index = self.section_index[prefix as usize];
        self.sections.get(section_index as usize).unwrap()
    }

    pub fn delete_storage(mut self) -> Result<(), Vec<StorageError>> {
        let errors: Vec<StorageError> = self.sections.into_iter().map(|section| section.delete_section())
            .filter(|result| result.is_err())
            .map(|result| StorageError {
                storage_name: self.name.as_ref().to_owned(),
                kind: StorageErrorKind::SectionError { source: result.unwrap_err() },
            }).collect();
        if errors.is_empty() {
            match std::fs::remove_dir_all(self.path.clone()) {
                Ok(_) => {
                    trace!("Storage {} deleted.", self.name);
                    Ok(())
                }
                Err(e) => {
                    error!("Failed to delete storage {}, received error: {}", self.name, e);
                    Err(vec!(StorageError {
                        storage_name: self.name.as_ref().to_owned(),
                        kind: StorageErrorKind::FailedToDeleteStorage { source: e },
                    }))
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
    prefix: u8,
    kv_storage: DB,
}

impl Section {
    fn new(name: &str, path: PathBuf, prefix: u8, options: &Options) -> Result<Self, SectionError> {
        let kv_storage = DB::open(&options, &path)
            .map_err(|e| SectionError {
                section_name: name.to_owned(),
                kind: SectionErrorKind::FailedToCreateSectionKVError { source: e },
            })?;
        Ok(Section {
            name: name.to_owned(),
            path: path,
            prefix: prefix,
            kv_storage: kv_storage,
        })
    }

    pub fn new_options() -> Options {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.enable_statistics();
        options.set_max_background_jobs(4);
        options
    }

    fn put(&self, bytes: &Vec<u8>) -> Result<(), SectionError> {
        self.kv_storage.put(bytes, Storage::BYTES_EMPTY)
            .map_err(|e| SectionError {
                // TODO: is into_string() correct?
                section_name: self.name.to_owned(),
                kind: SectionErrorKind::FailedSectionPut { source: e },
            })
    }

    // TODO: look into using get_pin if we're going to use the result
    fn get(&self, bytes: &Vec<u8>) -> Result<Option<Vec<u8>>, SectionError> {
        self.kv_storage.get(bytes).map_err(|e| SectionError {
            section_name: self.name.clone(),
            kind: SectionErrorKind::FailedSectionGet { source: e },
        })
    }

    // TODO: we should benchmark using iterator pools
    fn iterate_prefix<'s>(&'s self, prefix: Vec<u8>) -> impl Iterator<Item=Result<(Box<[u8]>, Box<[u8]>), speedb::Error>> + 's {
        self.kv_storage.prefix_iterator(prefix)
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
    FailedToCreateSectionKVError { source: speedb::Error },
    FailedToCreateSectionNameExists {},
    FailedToCreateSectionPrefixExists { prefix: u8, existing_section: String },
    FailedToGetSectionHandle {},
    FailedToDeleteSection { source: std::io::Error },
    FailedSectionGet { source: speedb::Error },
    FailedSectionPut { source: speedb::Error },
}

impl Display for SectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for SectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            SectionErrorKind::FailedToCreateSectionKVError { source, .. } => Some(source),
            SectionErrorKind::FailedToCreateSectionNameExists { .. } => None,
            SectionErrorKind::FailedToCreateSectionPrefixExists { .. } => None,
            SectionErrorKind::FailedToGetSectionHandle { .. } => None,
            SectionErrorKind::FailedToDeleteSection { source, .. } => Some(source),
            SectionErrorKind::FailedSectionGet { source, .. } => Some(source),
            SectionErrorKind::FailedSectionPut { source, .. } => Some(source),
        }
    }
}


