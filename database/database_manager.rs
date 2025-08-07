/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use cache::CACHE_DB_NAME_PREFIX;
use itertools::Itertools;
use resource::{constants::database::INTERNAL_DATABASE_PREFIX, internal_database_prefix};
use storage::durability_client::WALClient;
use tracing::{event, Level};

use crate::{database::DatabaseCreateError, Database, DatabaseDeleteError, DatabaseOpenError, DatabaseResetError};

type DatabasesMap = HashMap<String, Arc<Database<WALClient>>>;
type Databases = RwLock<DatabasesMap>;
type DatabasesReadLock<'a> = RwLockReadGuard<'a, DatabasesMap>;
type DatabasesWriteLock<'a> = RwLockWriteGuard<'a, DatabasesMap>;

#[derive(Debug)]
pub struct DatabaseManager {
    data_directory: PathBuf,
    import_directory: PathBuf,
    databases: Databases,
}

impl DatabaseManager {
    const IMPORT_DIRECTORY_NAME: &'static str = concat!(internal_database_prefix!(), "import");

    pub fn new(data_directory: impl AsRef<Path>) -> Result<Arc<Self>, DatabaseOpenError> {
        let data_directory = data_directory.as_ref().to_owned();
        let import_directory = data_directory.join(Self::IMPORT_DIRECTORY_NAME);

        let databases = RwLock::new(Self::initialise_databases(&data_directory, &import_directory)?);
        Self::cleanup_import_directory(&import_directory)?;

        Ok(Arc::new(Self { data_directory, import_directory, databases }))
    }

    fn initialise_databases(
        data_directory: &PathBuf,
        import_directory: &PathBuf,
    ) -> Result<DatabasesMap, DatabaseOpenError> {
        let entries = fs::read_dir(data_directory).map_err(|error| DatabaseOpenError::DirectoryRead {
            name: Self::file_name_lossy(data_directory),
            source: Arc::new(error),
        })?;

        let mut databases = DatabasesMap::new();

        for entry in entries {
            let entry_path = entry
                .map_err(|error| DatabaseOpenError::DirectoryRead {
                    name: Self::file_name_lossy(data_directory),
                    source: Arc::new(error),
                })?
                .path();

            if !entry_path.is_dir() {
                event!(Level::DEBUG, "Not attempting to load database @ {:?}: not a directory", entry_path);
                continue;
            }

            // TODO: Can be extended to "is in ignored/system/private directories"
            if &entry_path == import_directory {
                continue;
            }

            let database_name = entry_path.file_name().unwrap().to_string_lossy();
            if Self::is_internal_database(&database_name) {
                continue;
            }

            let database = Database::<WALClient>::open(&entry_path)?;
            assert!(!databases.contains_key(database.name()));
            databases.insert(database.name().to_owned(), Arc::new(database));
        }

        Ok(databases)
    }

    fn cleanup_import_directory(import_directory: &PathBuf) -> Result<(), DatabaseOpenError> {
        if !import_directory.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(import_directory).map_err(|error| DatabaseOpenError::DirectoryRead {
            name: Self::file_name_lossy(import_directory),
            source: Arc::new(error),
        })?;

        for entry in entries {
            let entry_path = entry
                .map_err(|error| DatabaseOpenError::DirectoryRead {
                    name: Self::file_name_lossy(import_directory),
                    source: Arc::new(error),
                })?
                .path();

            match entry_path.is_dir() {
                true => {
                    let name = entry_path.file_name().unwrap_or("".as_ref()).to_string_lossy();
                    if name.starts_with(CACHE_DB_NAME_PREFIX) {
                        event!(
                            Level::DEBUG,
                            "Cache '{name}' was not removed after an interrupted import operation. It will be deleted."
                        );
                    } else {
                        event!(Level::WARN, "Database '{name}' is in an incomplete state after an interrupted import operation. It will be deleted.");
                    }
                    fs::remove_dir_all(&entry_path).map_err(|source| DatabaseOpenError::DirectoryDelete {
                        name: Self::file_name_lossy(&entry_path),
                        source: Arc::new(source),
                    })?;
                }
                false => {
                    event!(Level::DEBUG, "Removing import file @ {:?}: expected to be temporary", entry_path);
                    fs::remove_file(&entry_path).map_err(|source| DatabaseOpenError::FileDelete {
                        name: Self::file_name_lossy(&entry_path),
                        source: Arc::new(source),
                    })?;
                }
            }
        }

        Ok(())
    }

    pub fn put_database(&self, name: impl AsRef<str>) -> Result<(), DatabaseCreateError> {
        Self::validate_database_name(name.as_ref())?;
        self.put_database_unrestricted(name)
    }

    pub fn put_database_unrestricted(&self, name: impl AsRef<str>) -> Result<(), DatabaseCreateError> {
        let name = name.as_ref();
        let mut databases = self.databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        if self.exists_import(&databases, name) {
            return Err(DatabaseCreateError::IsBeingImported { name: name.to_string() });
        }
        if !databases.contains_key(name) {
            let database = self.new_public_database(name)?;
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
        if !self.import_directory.exists() {
            fs::create_dir(&self.import_directory).map_err(|source| DatabaseCreateError::DirectoryWrite {
                name: name.clone(),
                source: Arc::new(source),
            })?;
        }

        Self::validate_database_name(&name)?;

        let databases = self.databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        if self.exists_public(&databases, &name) {
            return Err(DatabaseCreateError::AlreadyExists { name });
        }
        if self.exists_import(&databases, &name) {
            return Err(DatabaseCreateError::IsBeingImported { name });
        }

        self.new_imported_database(&name)
    }

    pub(crate) fn finalise_imported_database(&self, database: Database<WALClient>) -> Result<(), DatabaseCreateError> {
        let mut databases = self.databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        let name = database.name().to_string();
        let database_path = database.path.clone();

        assert!(self.exists_import(&databases, &name), "Imported database is not in the import folder");
        if self.exists_public(&databases, &name) {
            database.delete().map_err(|typedb_source| DatabaseCreateError::AlreadyExistsAndCleanupBlocked {
                name: name.clone(),
                typedb_source,
            })?;
            Err(DatabaseCreateError::AlreadyExists { name })
        } else {
            drop(database);
            self.move_directory_to_data(&name, &database_path)?;
            let database = self.new_public_database(&name)?;
            databases.insert(name, Arc::new(database));
            Ok(())
        }
    }

    pub(crate) fn cancel_database_import(&self, database: Database<WALClient>) -> Result<(), DatabaseDeleteError> {
        let databases = self.databases.write().map_err(|_| DatabaseDeleteError::WriteAccessDenied {})?;
        let name = database.name().to_string();
        if !self.exists_import(&databases, &name) {
            return Err(DatabaseDeleteError::DatabaseIsNotBeingImported { name });
        }
        database.delete()
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
            self.put_database(name).map_err(|typedb_source| DatabaseResetError::DatabaseCreate { typedb_source })?;
            return Ok(());
        };

        drop(databases);
        match result {
            Ok(_) => (),
            Err(_) => {
                self.delete_database(name.as_ref())
                    .map_err(|typedb_source| DatabaseResetError::DatabaseDelete { typedb_source })?;
                self.put_database(name).map_err(|typedb_source| DatabaseResetError::DatabaseCreate { typedb_source })?
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

    pub(crate) fn import_directory(&self) -> &PathBuf {
        &self.import_directory
    }

    fn new_public_database(&self, name: &str) -> Result<Database<WALClient>, DatabaseCreateError> {
        Database::<WALClient>::open(&self.data_directory.join(name))
            .map_err(|typedb_source| DatabaseCreateError::DatabaseOpen { typedb_source })
    }

    fn new_imported_database(&self, name: &str) -> Result<Database<WALClient>, DatabaseCreateError> {
        Database::<WALClient>::open(&self.import_directory.join(name))
            .map_err(|typedb_source| DatabaseCreateError::DatabaseOpen { typedb_source })
    }

    fn exists_public<'a>(&'a self, databases: &'a DatabasesWriteLock<'a>, name: &str) -> bool {
        let exists_public = self.data_directory.join(name).is_dir();
        assert_eq!(
            exists_public,
            databases.contains_key(name),
            "Public databases should be in the public database list: {name}"
        );
        exists_public
    }

    fn exists_import<'a>(&'a self, databases: &'a DatabasesWriteLock<'a>, name: &str) -> bool {
        let exists_import = self.import_directory.join(name).is_dir();
        assert!(
            !exists_import || !databases.contains_key(name),
            "Imported databases cannot be in the public database list: {name}"
        );
        exists_import
    }

    fn move_directory_to_data(&self, name: &str, directory: &PathBuf) -> Result<(), DatabaseCreateError> {
        let directory_name =
            directory.file_name().ok_or_else(|| DatabaseCreateError::DatabaseMove { name: name.to_string() })?;

        let target_path = self.data_directory.join(directory_name);
        fs::rename(directory, &target_path)
            .map_err(|source| DatabaseCreateError::DirectoryWrite { name: name.to_string(), source: Arc::new(source) })
    }

    fn file_name_lossy(path: &PathBuf) -> String {
        path.file_name().unwrap_or("".as_ref()).to_string_lossy().to_string()
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
