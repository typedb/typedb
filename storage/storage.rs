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
use std::path::PathBuf;
use speedb::{DB, Options};
use logger::{error, trace};
use crate::error::{StorageError, StorageErrorKind};

mod snapshot;
mod error;

pub struct Storage {
    name: String,
    path: PathBuf,
    sections: Vec<Section>,
    section_index: [u8; 256],
}

impl Storage {
    pub fn new(name: &str, path: PathBuf) -> Result<Self, StorageError> {
        let kv_storage_dir = path.with_extension(name);
        Ok(Storage {
            name: name.to_owned(),
            path: kv_storage_dir,
            sections: Vec::new(),
            section_index: [0; 256],
        })
    }

    fn add_prefixed_section(&mut self, name: &str, prefix: u8, options: &Options) -> Result<(), StorageError> {
        let section_path = self.path.with_extension(name);
        self.sections.push(Section::new(name, section_path, prefix)?);
        self.section_index[prefix as usize] = self.sections.len() as u8;
        Ok(())
    }

    fn new_options() -> Options {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.enable_statistics();
        options.set_max_background_jobs(4);
        options
    }

    pub fn delete(self) -> Result<(), Self> {
        self.sections.for_each(|section| section.delete());
        match std::fs::remove_dir_all(self.path.clone()) {
            Ok(_) => {
                trace!("Database {} deleted.", self.name);
                Ok(())
            }
            Err(e) => {
                error!("Failed to delete {}, received error: {}", self.name, e);
                Err(self)
            }
        }
    }
}

struct Section<'storage> {
    storage_name: &'storage str,
    section_name: String,
    path: PathBuf,
    prefix: u8,
    kv_storage: DB,
}

impl<'storage> Section<'storage> {
    fn new(storage_name: &'storage str, section_name: &str, path: PathBuf, prefix: u8) -> Result<Self, StorageError> {
        let kv_storage = DB::open(&options, &path)
            .map_err(|e| StorageError {
                storage_name: section_name.to_owned(),
                kind: StorageErrorKind::FailedToCreateSection { section_name: section_name.to_owned(), source: e },
            })?;
        Ok(Section {
            storage_name: storage_name,
            section_name: section_name.to_owned(),
            path: path,
            prefix: prefix,
            kv_storage: kv_storage,
        })
    }

    fn delete(self) -> Result<(), StorageError> {
        match std::fs::remove_dir_all(self.path.clone()) {
            Ok(_) => {
                trace!("Database section {} deleted.", self.section_name);
                Ok(())
            }
            Err(e) => {
                error!("Failed to delete {}, received error: {}", self.section_name, e);
                StorageErrorKind::FailedToDeleteSection { section_name: section_name.to_owned(), source: e },
            }
        }
    }
}

