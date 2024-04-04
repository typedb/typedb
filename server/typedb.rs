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
};

use database::{Database, DatabaseRecoverError};
use durability::wal::WAL;
use itertools::Itertools;

#[derive(Debug)]
pub struct Server {
    data_directory: PathBuf,
    databases: HashMap<String, Database<WAL>>,
}

impl Server {
    pub fn recover(data_directory: impl AsRef<Path>) -> Result<Self, ServerRecoverError> {
        use ServerRecoverError::*;
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
                let database_name = entry.file_name().into_string().map_err(|name| InvalidUnicodeName { name })?;
                let database = Database::recover(&entry.path(), &database_name)
                    .map_err(|error| DatabaseRecover { source: error })?;
                Ok((database_name, database))
            })
            .try_collect()?;
        let data_directory = data_directory.to_owned();
        Ok(Self { data_directory, databases })
    }

    pub fn create_database(&mut self, name: impl AsRef<str>) {
        let name = name.as_ref();
        self.databases
            .entry(name.to_owned())
            .or_insert_with(|| Database::recover(&self.data_directory.join(name), name).unwrap());
    }

    pub fn databases(&self) -> &HashMap<String, Database<WAL>> {
        &self.databases
    }

    pub fn serve(mut self) {
        self.create_database("test");
        dbg!(self.databases);
    }
}

#[derive(Debug)]
pub enum ServerRecoverError {
    NotADirectory { path: PathBuf },
    CouldNotCreateDataDirectory { path: PathBuf, source: io::Error },
    CouldNotReadDataDirectory { path: PathBuf, source: io::Error },
    InvalidUnicodeName { name: OsString },
    DatabaseRecover { source: DatabaseRecoverError },
}

impl Error for ServerRecoverError {}

impl fmt::Display for ServerRecoverError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
