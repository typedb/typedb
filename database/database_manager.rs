/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, LockResult, RwLock, RwLockReadGuard},
};

use logger::debug;
use resource::constants::{database::INTERNAL_DATABASE_PREFIX, server::SYSTEM_FILE_PREFIX};
use storage::durability_client::WALClient;
use tracing::{event, Level};

use crate::{database::DatabaseCreateError, Database, DatabaseDeleteError, DatabaseOpenError, DatabaseResetError};

#[derive(Debug)]
pub struct DatabaseManager {
    data_directory: PathBuf,
    databases: RwLock<HashMap<String, Arc<Database<WALClient>>>>,
    imported_databases: RwLock<HashSet<String>>,
}

impl DatabaseManager {
    pub fn new(data_directory: &Path) -> Result<Arc<Self>, DatabaseOpenError> {
        let entries = fs::read_dir(data_directory).map_err(|error| DatabaseOpenError::DirectoryRead {
            path: data_directory.to_owned(),
            source: Arc::new(error),
        })?;

        let mut databases: HashMap<String, Arc<Database<WALClient>>> = HashMap::new();

        for entry in entries {
            let entry_path = entry
                .map_err(|error| DatabaseOpenError::DirectoryRead {
                    path: data_directory.to_owned(),
                    source: Arc::new(error),
                })?
                .path();

            if !entry_path.is_dir() {
                debug!("Not attempting to load database @ {:?}: not a directory", entry_path);
                continue;
            }

            let database_name = entry_path.file_name().unwrap().to_string_lossy();
            if database_name.starts_with(SYSTEM_FILE_PREFIX) {
                continue;
            }

            match Database::<WALClient>::open(&entry_path) {
                Ok(database) => {
                    assert!(!databases.contains_key(database.name()));
                    databases.insert(database.name().to_owned(), Arc::new(database));
                }
                Err(DatabaseOpenError::IncompleteDatabaseImport {}) => {
                    event!(Level::WARN, "Database {database_name} is in an incomplete state after an interrupted import operation. It will be deleted.");
                    fs::remove_dir_all(&entry_path).map_err(|source| DatabaseOpenError::DirectoryDelete {
                        path: entry_path.to_owned(),
                        source: Arc::new(source),
                    })?;
                }
                Err(other) => return Err(other),
            }
        }

        Ok(Arc::new(Self {
            data_directory: data_directory.to_owned(),
            databases: RwLock::new(databases),
            imported_databases: RwLock::new(HashSet::new()),
        }))
    }

    pub fn create_database(&self, name: impl AsRef<str>) -> Result<(), DatabaseCreateError> {
        let name = name.as_ref();
        Self::validate_database_name(name.as_ref())?;
        self.create_database_unrestricted(name)
    }

    pub fn create_database_unrestricted(&self, name: impl AsRef<str>) -> Result<(), DatabaseCreateError> {
        // Allows databases to already exist
        let name = name.as_ref();
        let mut databases = self.databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        if !databases.contains_key(name) {
            let database = Database::<WALClient>::open(&self.data_directory.join(name))
                .map_err(|typedb_source| DatabaseCreateError::DatabaseOpen { typedb_source })?;
            databases.insert(name.to_string(), Arc::new(database));
        }
        Ok(())
    }

    pub fn delete_database(&self, name: impl AsRef<str>) -> Result<(), DatabaseDeleteError> {
        let name = name.as_ref();
        if Self::is_internal_database(name) {
            return Err(DatabaseDeleteError::InternalDatabaseDeletionProhibited {});
        }

        // TODO: this is a partial implementation, only single threaded and without cooperative transaction shutdown
        // remove from map to make DB unavailable
        let mut databases = self.databases.write().map_err(|_| DatabaseDeleteError::WriteAccessDenied {})?;
        let db = databases.remove(name);
        match db {
            None => return Err(DatabaseDeleteError::DoesNotExist {}),
            Some(db) => {
                match Arc::try_unwrap(db) {
                    Ok(unwrapped) => unwrapped.delete()?,
                    Err(arc) => {
                        // failed to delete since it's in use - let's re-insert for now instead of losing the reference
                        databases.insert(name.to_owned(), arc);
                        return Err(DatabaseDeleteError::InUse {});
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn prepare_imported_database(&self, name: String) -> Result<Database<WALClient>, DatabaseCreateError> {
        let mut imported_databases =
            self.imported_databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        if imported_databases.contains(&name) {
            return Err(DatabaseCreateError::AlreadyExists { name: name.to_string() });
        }

        Self::validate_database_name(&name)?;
        if self.database(&name).is_some() {
            return Err(DatabaseCreateError::AlreadyExists { name: name.to_string() });
        }

        let database = Database::<WALClient>::open(&self.data_directory.join(name.clone()))
            .map_err(|typedb_source| DatabaseCreateError::DatabaseOpen { typedb_source })?;
        imported_databases.insert(name);
        database.mark_imported()?;
        Ok(database)
    }

    pub(crate) fn finalise_imported_database(&self, database: Database<WALClient>) -> Result<(), DatabaseCreateError> {
        let name = database.name().to_string();

        let mut imported_databases =
            self.imported_databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        if !imported_databases.contains(&name) {
            return Err(DatabaseCreateError::DatabaseIsNotBeingImported { name });
        }
        imported_databases.remove(&name);

        let mut databases =
            self.databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        if databases.contains_key(&name) {
            return Err(DatabaseCreateError::AlreadyExists { name });
        } else {
            database.unmark_imported()?;
            databases.insert(name, Arc::new(database));
            Ok(())
        }
    }

    pub(crate) fn cancel_database_import(&self, database: Database<WALClient>) -> Result<(), DatabaseDeleteError> {
        let name = database.name().to_string();

        let mut imported_databases =
            self.imported_databases.write().map_err(|_| DatabaseDeleteError::WriteAccessDenied {})?;
        if !imported_databases.contains(&name) {
            return Err(DatabaseDeleteError::DatabaseIsNotBeingImported { name });
        }

        database.delete()?;
        imported_databases.remove(&name);
        Ok(())
    }

    pub fn reset_else_recreate_database(&self, name: impl AsRef<str>) -> Result<(), DatabaseResetError> {
        // TODO: this is a partial implementation, only single threaded and without cooperative transaction shutdown
        // remove from map to make DB unavailable
        let mut databases = self.databases.write().unwrap();
        let db = databases.remove(name.as_ref());
        let result = if let Some(db) = db {
            match Arc::try_unwrap(db) {
                Ok(mut unwrapped) => {
                    let reset_result = unwrapped.reset();
                    databases.insert(name.as_ref().to_owned(), Arc::new(unwrapped));
                    reset_result
                }
                Err(arc) => {
                    // failed to reset since it's in use - let's re-insert for now instead of losing the reference
                    databases.insert(name.as_ref().to_owned(), arc);
                    Err(DatabaseResetError::InUse {})
                }
            }
        } else {
            drop(databases);
            self.create_database(name).map_err(|typedb_source| DatabaseResetError::DatabaseCreate { typedb_source })?;
            return Ok(());
        };

        drop(databases);
        match result {
            Ok(_) => (),
            Err(_) => {
                self.delete_database(name.as_ref())
                    .map_err(|typedb_source| DatabaseResetError::DatabaseDelete { typedb_source })?;
                self.create_database(name)
                    .map_err(|typedb_source| DatabaseResetError::DatabaseCreate { typedb_source })?
            }
        };
        Ok(())
    }

    pub fn database(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        if Self::is_internal_database(name) {
            return None;
        }
        self.database_unrestricted(name)
    }

    pub fn database_unrestricted(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        self.databases.read().unwrap().get(name).cloned()
    }

    pub fn database_names(&self) -> Vec<String> {
        self.databases.read().unwrap().keys().cloned().filter(|db| Self::is_user_database(db)).collect()
    }

    pub fn databases(&self) -> RwLockReadGuard<'_, HashMap<String, Arc<Database<WALClient>>>> {
        self.databases.read().unwrap()
    }

    pub fn is_user_database(name: &str) -> bool {
        !Self::is_internal_database(name)
    }

    pub fn is_internal_database(name: &str) -> bool {
        name.starts_with(INTERNAL_DATABASE_PREFIX)
    }

    fn validate_database_name(name: &str) -> Result<(), DatabaseCreateError> {
        if Self::is_internal_database(name) {
            return Err(DatabaseCreateError::InternalDatabaseCreationProhibited {});
        }
        if !typeql::common::identifier::is_valid_identifier(name) {
            return Err(DatabaseCreateError::InvalidName { name: name.to_string() });
        }
        Ok(())
    }
}
