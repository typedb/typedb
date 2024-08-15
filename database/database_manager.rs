/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;

use storage::durability_client::WALClient;

use crate::{Database, DatabaseDeleteError, DatabaseOpenError, DatabaseResetError};

#[derive(Debug)]
pub struct DatabaseManager {
    data_directory: PathBuf,
    databases: HashMap<String, Arc<Database<WALClient>>>,
}

impl DatabaseManager {
    pub fn new(data_directory: &Path) -> Result<Self, DatabaseOpenError> {
        let databases = fs::read_dir(data_directory)
            .map_err(|error| DatabaseOpenError::CouldNotReadDataDirectory { path: data_directory.to_owned(), source: error })?
            .map(|entry| {
                let entry = entry
                    .map_err(|error| DatabaseOpenError::CouldNotReadDataDirectory { path: data_directory.to_owned(), source: error })?;
                let database = Database::<WALClient>::open(&entry.path())?;
                Ok((database.name().to_owned(), Arc::new(database)))
            })
            .try_collect()?;

        Ok(Self { data_directory: data_directory.to_owned(), databases })
    }

    pub fn create_database(&mut self, name: impl AsRef<str>) {
        let name = name.as_ref();
        self.databases
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(Database::<WALClient>::open(&self.data_directory.join(name)).unwrap()));
    }

    pub fn delete_database(&mut self, name: impl AsRef<str>) -> Result<(), DatabaseDeleteError> {
        // TODO: this is a partial implementation, only single threaded and without cooperative transaction shutdown
        // remove from map to make DB unavailable
        let db = self.databases.remove(name.as_ref());
        if let Some(db) = db {
            match Arc::try_unwrap(db) {
                Ok(unwrapped) => unwrapped.delete()?,
                Err(arc) => {
                    // failed to delete since it's in use - let's re-insert for now instead of losing the reference
                    self.databases.insert(name.as_ref().to_owned(), arc);
                    return Err(DatabaseDeleteError::InUse {});
                }
            }
        }
        Ok(())
    }

    pub fn reset_else_recreate_database(&mut self, name: impl AsRef<str>) -> Result<(), DatabaseDeleteError> {
        // TODO: this is a partial implementation, only single threaded and without cooperative transaction shutdown
        // remove from map to make DB unavailable
        let db = self.databases.remove(name.as_ref());
        let result = if let Some(db) = db {
            match Arc::try_unwrap(db) {
                Ok(mut unwrapped) => {
                    let reset_result = unwrapped.reset();
                    self.databases.insert(name.as_ref().to_owned(), Arc::new(unwrapped));
                    reset_result
                }
                Err(arc) => {
                    // failed to reset since it's in use - let's re-insert for now instead of losing the reference
                    self.databases.insert(name.as_ref().to_owned(), arc);
                    Err(DatabaseResetError::InUse {})
                }
            }
        } else {
            self.create_database(name);
            return Ok(());
        };

        Ok(match result {
            Ok(_) => (),
            Err(_) => {
                self.delete_database(name.as_ref())?;
                self.create_database(name)
            }
        })
    }

    pub fn database(&self, name: &str) -> Option<&Database<WALClient>> {
        self.databases.get(name).map(|arc| &**arc)
    }

}