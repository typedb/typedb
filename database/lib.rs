/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{fs::File, io::Read, path::Path, sync::Arc};

use durability::properties::WALProperties;
use primitive::either::Either;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use storage::properties::StorageProperties;
use versioning::{ExtendableVersion, ServerSupportedVersion};

pub use self::database::{Database, DatabaseDeleteError, DatabaseOpenError, DatabaseResetError};

pub mod database;
pub mod database_manager;
mod database_registry;
pub mod migration;
pub mod query;
pub mod transaction;

#[derive(Debug, Serialize, Deserialize)]
struct DatabaseProperties {
    properties_file_version: ExtendableVersion,
    database_id: u64,
    wal: WALProperties,
    storage: StorageProperties,
}

impl DatabaseProperties {
    pub const FILENAME: &'static str = "properties.yml";
    pub const SERVER_PROPERTIES_FILE_VERSION: ServerSupportedVersion =
        ServerSupportedVersion(ExtendableVersion { version: 1, extension: 1 });

    pub(crate) fn values_for_prehistoric_database() -> Self {
        let database_id = rand::thread_rng().next_u64();
        let properties_file_version = Self::SERVER_PROPERTIES_FILE_VERSION.0.clone();
        let wal = WALProperties::values_for_prehistoric_database();
        let storage = StorageProperties::values_for_prehistoric_database();
        Self { database_id, properties_file_version, wal, storage }
    }

    pub(crate) fn create_for_new_database(wal: WALProperties, storage: StorageProperties) -> Self {
        let database_id = rand::thread_rng().next_u64();
        let properties_file_version = Self::SERVER_PROPERTIES_FILE_VERSION.0.clone();
        Self { database_id, properties_file_version, wal, storage }
    }

    pub(crate) fn validate_compatible(&self) -> Result<(), DatabaseOpenError> {
        WALProperties::validate_compatibility(&self.wal).map_err(|source| DatabaseOpenError::WALOpen { source })?;
        StorageProperties::validate_compatibility(&self.storage)
            .map_err(|typedb_source| DatabaseOpenError::StorageOpen { typedb_source })?;
        Ok(())
    }

    fn try_read(dir: &Path, db_name: &str) -> Result<Option<Self>, Box<DatabaseOpenError>> {
        let file_path = dir.join(Self::FILENAME);
        let exists = !std::fs::exists(&file_path).map_err(|io_error| {
            Box::new(DatabaseOpenError::PropertiesRead { name: db_name.to_owned(), source: Arc::new(io_error) })
        })?;
        if !exists {
            return Ok(None);
        }
        let mut raw_yaml = String::new();
        File::open(&file_path).and_then(|mut file| file.read_to_string(&mut raw_yaml)).map_err(|io_error| {
            Box::new(DatabaseOpenError::PropertiesRead { name: db_name.to_owned(), source: Arc::new(io_error) })
        })?;
        let properties = serde_yaml2::from_str::<Self>(raw_yaml.as_str()).map_err(|source| {
            Box::new(DatabaseOpenError::PropertiesParse { name: db_name.to_owned(), source: Arc::new(source) })
        })?;
        Ok(Some(properties))
    }

    fn write(&self, dir: &Path, db_name: &str) -> Result<(), DatabaseOpenError> {
        self.write_at_dir(dir).map_err(|err| match err {
            Either::First(io_error) => {
                DatabaseOpenError::PropertiesWrite { name: db_name.to_owned(), source: Arc::new(io_error) }
            }
            Either::Second(serde_error) => {
                DatabaseOpenError::PropertiesSerialize { name: db_name.to_owned(), source: Arc::new(serde_error) }
            }
        })
    }

    fn write_at_dir(&self, dir: &Path) -> Result<(), Either<std::io::Error, serde_yaml2::ser::Errors>> {
        let yaml = serde_yaml2::to_string(self).map_err(|e| Either::Second(e))?;

        // Write to a temp file, then rename, so a crash never leaves a half-written file.
        let final_path = dir.join(Self::FILENAME);
        let tmp_path = dir.join(format!("{}.tmp", Self::FILENAME));
        std::fs::write(&tmp_path, yaml).map_err(|e| Either::First(e))?;
        std::fs::rename(&tmp_path, &final_path).map_err(|e| Either::First(e))?;
        Ok(())
    }
}
