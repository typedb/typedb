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

use diagnostics::diagnostics_manager::DiagnosticsManager;
pub(crate) use durability::sync_directory;
use options::byte_size::ByteSize;
use resource::constants::database::INTERNAL_DATABASE_PREFIX;
use storage::{durability_client::WALClient, keyspace::rocks_resources::RocksResources};
use tracing::{Level, debug, event, warn};

pub use crate::database_import_manager::ImportRecovery;
use crate::{
    Database, DatabaseDeleteError, DatabaseOpenError, database::DatabaseCreateError,
    database_import_manager::DatabaseImportManager,
};

type DatabasesMap = HashMap<String, Arc<Database<WALClient>>>;
type Databases = RwLock<DatabasesMap>;
type DatabasesWriteLock<'a> = RwLockWriteGuard<'a, DatabasesMap>;

#[derive(Debug)]
pub struct DatabaseManager {
    data_directory: PathBuf,
    databases: Databases,
    imports: DatabaseImportManager,
    diagnostics_manager: Arc<DiagnosticsManager>,
    rocks_resources: Arc<RocksResources>,
}

impl DatabaseManager {
    pub fn new(
        data_directory: impl AsRef<Path>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        rocksdb_cache_size: ByteSize,
        rocksdb_write_buffers_limit: ByteSize,
        import_recovery: ImportRecovery,
    ) -> Result<Arc<Self>, DatabaseOpenError> {
        let data_directory = data_directory.as_ref().to_owned();
        let rocks_resources = Arc::new(RocksResources::new(rocksdb_cache_size, rocksdb_write_buffers_limit));
        let imports = DatabaseImportManager::new(
            &data_directory,
            import_recovery,
            diagnostics_manager.clone(),
            rocks_resources.clone(),
        )?;
        let databases = RwLock::new(Self::initialise_databases(
            &data_directory,
            imports.directory(),
            &diagnostics_manager,
            &rocks_resources,
        )?);

        Ok(Arc::new(Self { data_directory, databases, imports, diagnostics_manager, rocks_resources }))
    }

    fn initialise_databases(
        data_directory: &PathBuf,
        import_directory: &Path,
        diagnostics_manager: &DiagnosticsManager,
        rocks_resources: &RocksResources,
    ) -> Result<DatabasesMap, DatabaseOpenError> {
        let mut databases = DatabasesMap::new();

        for entry_path in directory_entries(data_directory)? {
            if !entry_path.is_dir() {
                event!(Level::DEBUG, "Not attempting to load database @ {:?}: not a directory", entry_path);
                continue;
            }

            // TODO: Can be extended to "is in ignored/system/private directories"
            if entry_path == import_directory {
                continue;
            }

            let database_name = entry_path.file_name().unwrap().to_string_lossy();
            if Self::validate_database_name(&database_name).is_err() {
                continue;
            }

            let database = match Database::<WALClient>::open(&entry_path, diagnostics_manager, rocks_resources) {
                Ok(database) => database,
                Err(DatabaseOpenError::NotADatabase { .. }) => {
                    warn!("{entry_path:?} is not a database, skipping");
                    continue;
                }
                Err(err) => return Err(err),
            };
            assert!(!databases.contains_key(database.name()));
            databases.insert(database.name().to_owned(), Arc::new(database));
        }

        Ok(databases)
    }

    pub fn put_database(&self, name: impl AsRef<str>) -> Result<(), DatabaseCreateError> {
        Self::validate_user_database_name(name.as_ref())?;
        self.put_database_unrestricted(name)
    }

    pub fn put_database_unrestricted(&self, name: impl AsRef<str>) -> Result<(), DatabaseCreateError> {
        let name = name.as_ref();
        let mut databases = self.databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        self.imports.ensure_available(name)?;
        if !databases.contains_key(name) {
            let database = self.new_public_database(name)?;
            databases.insert(name.to_string(), Arc::new(database));
            sync_directory(&self.data_directory).map_err(|source| DatabaseCreateError::DirectoryWrite {
                name: name.to_string(),
                source: Arc::new(source),
            })?;
        }
        Ok(())
    }

    pub fn delete_database(&self, name: impl AsRef<str>) -> Result<(), DatabaseDeleteError> {
        let name = name.as_ref();
        debug!("Deleting database {name}");
        if Self::is_internal_database(name) {
            return Err(DatabaseDeleteError::InternalDatabaseDeletionProhibited {});
        }

        // TODO: this is a partial implementation, only single threaded and without cooperative transaction shutdown
        // remove from map to make DB unavailable
        let mut databases = self.databases.write().map_err(|_| DatabaseDeleteError::WriteAccessDenied {})?;
        let db = databases.remove(name);
        match db {
            None => return Err(DatabaseDeleteError::DoesNotExist {}),
            Some(db) => match Arc::try_unwrap(db) {
                Ok(unwrapped) => unwrapped.delete()?,
                Err(arc) => {
                    databases.insert(name.to_owned(), arc);
                    return Err(DatabaseDeleteError::InUse {});
                }
            },
        }
        sync_directory(&self.data_directory)
            .map_err(|source| DatabaseDeleteError::DirectoryDelete { source: Arc::new(source) })?;
        Ok(())
    }

    pub fn prepare_imported_database(&self, name: String) -> Result<Arc<Database<WALClient>>, DatabaseCreateError> {
        Self::validate_user_database_name(&name)?;
        let databases = self.databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        if self.exists_public(&databases, &name) {
            return Err(DatabaseCreateError::AlreadyExists { name });
        }
        self.imports.create_staging(&name)
    }

    pub fn imported_database(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        self.imports.get(name)
    }

    pub fn imported_database_names(&self) -> Vec<String> {
        self.imports.names()
    }

    pub fn is_lost_import(&self, name: &str) -> bool {
        self.imports.is_lost(name)
    }

    pub fn import_directory(&self) -> &Path {
        self.imports.directory()
    }

    pub fn finalise_imported_database(&self, name: &str) -> Result<(), DatabaseCreateError> {
        let mut databases = self.databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        let database = match self.imports.take_staging(name) {
            Ok(Some(database)) => database,
            Ok(None) => return Err(DatabaseCreateError::IsNotBeingImported { name: name.to_string() }),
            Err(DatabaseDeleteError::InUse {}) => {
                return Err(DatabaseCreateError::ImportedDatabaseInUse { name: name.to_string() });
            }
            Err(_) => return Err(DatabaseCreateError::WriteAccessDenied {}),
        };
        let database_path = database.path.clone();

        if self.exists_public(&databases, name) {
            database.delete().map_err(|typedb_source| DatabaseCreateError::AlreadyExistsAndCleanupBlocked {
                name: name.to_string(),
                typedb_source,
            })?;
            Err(DatabaseCreateError::AlreadyExists { name: name.to_string() })
        } else {
            drop(database);
            self.move_directory_to_data(name, &database_path)?;
            let map_fs_err =
                |source| DatabaseCreateError::DirectoryWrite { name: name.to_string(), source: Arc::new(source) };
            sync_directory(&self.data_directory).map_err(map_fs_err)?;
            self.imports.sync().map_err(map_fs_err)?;
            let database = self.new_public_database(name)?;
            databases.insert(name.to_string(), Arc::new(database));
            Ok(())
        }
    }

    pub fn cancel_database_import(&self, name: &str) -> Result<(), DatabaseDeleteError> {
        if Self::validate_user_database_name(name).is_err() {
            return Err(DatabaseDeleteError::DatabaseIsNotBeingImported { name: name.to_string() });
        }
        let _databases = self.databases.write().map_err(|_| DatabaseDeleteError::WriteAccessDenied {})?;
        self.imports.delete_staging(name)
    }

    pub fn rocks_resources(&self) -> &Arc<RocksResources> {
        &self.rocks_resources
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
        self.databases.read().unwrap().keys().filter(|&db| Self::is_user_database(db)).cloned().collect()
    }

    pub fn databases(&self) -> RwLockReadGuard<'_, HashMap<String, Arc<Database<WALClient>>>> {
        self.databases.read().unwrap()
    }

    pub fn prepare_for_writes(&self) -> Result<(), DatabaseOpenError> {
        for (name, database) in self.databases.read().unwrap().iter() {
            database
                .prepare_for_writes()
                .map_err(|source| DatabaseOpenError::PrepareForWrites { name: name.clone(), source })?;
        }
        Ok(())
    }

    pub fn is_user_database(name: &str) -> bool {
        !Self::is_internal_database(name)
    }

    pub fn is_internal_database(name: &str) -> bool {
        name.starts_with(INTERNAL_DATABASE_PREFIX)
    }

    fn new_public_database(&self, name: &str) -> Result<Database<WALClient>, DatabaseCreateError> {
        Database::<WALClient>::open(&self.data_directory.join(name), &self.diagnostics_manager, &self.rocks_resources)
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

    fn move_directory_to_data(&self, name: &str, directory: &PathBuf) -> Result<(), DatabaseCreateError> {
        let directory_name =
            directory.file_name().ok_or_else(|| DatabaseCreateError::DatabaseMove { name: name.to_string() })?;

        let target_path = self.data_directory.join(directory_name);
        fs::rename(directory, &target_path)
            .map_err(|source| DatabaseCreateError::DirectoryWrite { name: name.to_string(), source: Arc::new(source) })
    }

    fn validate_user_database_name(name: &str) -> Result<(), DatabaseCreateError> {
        if Self::is_internal_database(name) {
            return Err(DatabaseCreateError::InternalDatabaseCreationProhibited {});
        }
        Self::validate_database_name(name)
    }

    fn validate_database_name(name: &str) -> Result<(), DatabaseCreateError> {
        if !typeql::common::identifier::is_valid_label(name) {
            return Err(DatabaseCreateError::InvalidName { name: name.to_string() });
        }
        Ok(())
    }
}

pub(crate) fn file_name_lossy(path: &Path) -> String {
    path.file_name().unwrap_or("".as_ref()).to_string_lossy().to_string()
}

pub(crate) fn directory_entries(directory: &Path) -> Result<Vec<PathBuf>, DatabaseOpenError> {
    let entries = fs::read_dir(directory).map_err(|error| DatabaseOpenError::DirectoryRead {
        name: file_name_lossy(directory),
        source: Arc::new(error),
    })?;
    entries
        .map(|entry| {
            entry.map(|entry| entry.path()).map_err(|error| DatabaseOpenError::DirectoryRead {
                name: file_name_lossy(directory),
                source: Arc::new(error),
            })
        })
        .collect()
}
