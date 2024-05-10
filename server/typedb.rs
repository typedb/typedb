/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    error::Error,
    ffi::OsString,
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use database::{Database, DatabaseOpenError};
use durability::wal::WAL;
use itertools::Itertools;

#[derive(Debug)]
pub struct Server {
    data_directory: PathBuf,
    databases: HashMap<String, Arc<Database<WAL>>>,
}

impl Server {
    pub fn open(data_directory: impl AsRef<Path>) -> Result<Self, ServerOpenError> {
        use ServerOpenError::{
            CouldNotCreateDataDirectory, CouldNotReadDataDirectory, DatabaseOpen, NotADirectory,
        };
        let data_directory = data_directory.as_ref();

        if !data_directory.exists() {
            fs::create_dir_all(data_directory)
                .map_err(|error| CouldNotCreateDataDirectory { path: data_directory.to_owned(), source: error })?;
        } else if !data_directory.is_dir() {
            return Err(NotADirectory { path: data_directory.to_owned() });
        }

        let databases = fs::read_dir(data_directory)
            .map_err(|error| CouldNotReadDataDirectory { path: data_directory.to_owned(), source: error })?
            .map(|entry| {
                let entry = entry
                    .map_err(|error| CouldNotReadDataDirectory { path: data_directory.to_owned(), source: error })?;
                let database = Database::<WAL>::open(&entry.path()).map_err(|error| DatabaseOpen { source: error })?;
                Ok((database.name().to_owned(), Arc::new(database)))
            })
            .try_collect()?;
        let data_directory = data_directory.to_owned();
        Ok(Self { data_directory, databases })
    }

    pub fn create_database(&mut self, name: impl AsRef<str>) {
        let name = name.as_ref();
        self.databases
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(Database::<WAL>::open(&self.data_directory.join(name)).unwrap()));
    }

    pub fn database(&self, name: &str) -> Option<&Database<WAL>> {
        self.databases.get(name).map(|arc| &**arc)
    }

    pub fn databases(&self) -> &HashMap<String, Arc<Database<WAL>>> {
        &self.databases
    }

    pub fn serve(self) {
        todo!()
    }
}

#[derive(Debug)]
pub enum ServerOpenError {
    NotADirectory { path: PathBuf },
    CouldNotCreateDataDirectory { path: PathBuf, source: io::Error },
    CouldNotReadDataDirectory { path: PathBuf, source: io::Error },
    DatabaseOpen { source: DatabaseOpenError },
}

impl Error for ServerOpenError {}

impl fmt::Display for ServerOpenError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
