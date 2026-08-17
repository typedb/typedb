/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::DatabaseMetricsSnapshot};
pub(crate) use durability::sync_directory;
use options::byte_size::ByteSize;
use resource::constants::database::INTERNAL_DATABASE_PREFIX;
use storage::{durability_client::WALClient, keyspace::rocks_resources::RocksResources};
use tracing::{Level, debug, event, warn};

pub use crate::database_import_store::ImportOwnership;
use crate::{
    Database, DatabaseDeleteError, DatabaseOpenError,
    database::DatabaseCreateError,
    database_import_store::DatabaseImportStore,
    database_registry::{DatabaseRegistry, DatabasesMap, DatabasesWriteLock},
};

#[derive(Debug)]
pub struct DatabaseManager {
    data_directory: PathBuf,
    database_registry: DatabaseRegistry,
    imports: DatabaseImportStore,
    diagnostics_manager: Arc<DiagnosticsManager>,
    rocks_resources: Arc<RocksResources>,
}

impl DatabaseManager {
    pub fn new(
        data_directory: impl AsRef<Path>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        rocksdb_cache_size: ByteSize,
        rocksdb_write_buffers_limit: ByteSize,
        import_ownership: ImportOwnership,
    ) -> Result<Arc<Self>, DatabaseOpenError> {
        let data_directory = data_directory.as_ref().to_owned();
        let rocks_resources = Arc::new(RocksResources::new(rocksdb_cache_size, rocksdb_write_buffers_limit));
        let (imports, staged_databases) = DatabaseImportStore::new(
            &data_directory,
            import_ownership,
            diagnostics_manager.clone(),
            rocks_resources.clone(),
        )?;
        let served_databases =
            Self::initialise_databases(&data_directory, imports.directory(), &diagnostics_manager, &rocks_resources)?;
        let database_registry = DatabaseRegistry::new(served_databases, staged_databases);

        Ok(Arc::new(Self { data_directory, database_registry, imports, diagnostics_manager, rocks_resources }))
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
        let mut databases = self.database_registry.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        self.imports.ensure_available(&databases.staged, name)?;
        if !databases.served.contains_key(name) {
            let database = self.new_served_database(name)?;
            databases.served.insert(name.to_string(), Arc::new(database));
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
        let mut databases = self.database_registry.write().map_err(|_| DatabaseDeleteError::WriteAccessDenied {})?;
        let db = databases.served.remove(name);
        match db {
            None => return Err(DatabaseDeleteError::DoesNotExist {}),
            Some(db) => match Arc::try_unwrap(db) {
                Ok(unwrapped) => unwrapped.delete()?,
                Err(arc) => {
                    databases.served.insert(name.to_owned(), arc);
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
        let mut databases = self.database_registry.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        if self.exists_served(&databases, &name) {
            return Err(DatabaseCreateError::AlreadyExists { name });
        }
        self.imports.create(&mut databases.staged, &name)
    }

    pub fn imported_database(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        self.database_registry.staged(name)
    }

    pub fn imported_database_names(&self) -> Vec<String> {
        self.database_registry.staged_names()
    }

    pub fn is_abandoned_import(&self, name: &str) -> bool {
        self.imports.is_abandoned(&self.database_registry.read().staged, name)
    }

    pub fn import_directory(&self) -> &Path {
        self.imports.directory()
    }

    pub fn finalise_imported_database(&self, name: &str) -> Result<(), DatabaseCreateError> {
        let mut databases = self.database_registry.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        let database = match self.imports.take(&mut databases.staged, name) {
            Ok(Some(database)) => database,
            Ok(None) => return Err(DatabaseCreateError::IsNotBeingImported { name: name.to_string() }),
            Err(DatabaseDeleteError::InUse {}) => {
                return Err(DatabaseCreateError::ImportedDatabaseInUse { name: name.to_string() });
            }
            Err(_) => return Err(DatabaseCreateError::WriteAccessDenied {}),
        };
        let database_path = database.path.clone();

        if self.exists_served(&databases, name) {
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
            let database = self.new_served_database(name)?;
            databases.served.insert(name.to_string(), Arc::new(database));
            Ok(())
        }
    }

    pub fn discard_imported_database(&self, name: &str) -> Result<(), DatabaseDeleteError> {
        if Self::validate_user_database_name(name).is_err() {
            return Err(DatabaseDeleteError::DatabaseIsNotBeingImported { name: name.to_string() });
        }
        let mut databases = self.database_registry.write().map_err(|_| DatabaseDeleteError::WriteAccessDenied {})?;
        self.imports.delete(&mut databases.staged, name)
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
        self.database_registry.served(name)
    }

    pub fn database_names(&self) -> Vec<String> {
        self.database_registry.served_names().into_iter().filter(|name| Self::is_user_database(name)).collect()
    }

    pub fn user_database_metrics(&self) -> HashMap<Arc<str>, DatabaseMetricsSnapshot> {
        self.database_registry
            .read()
            .served
            .values()
            .filter(|database| Self::is_user_database(database.name()))
            .map(|database| (database.name_arc(), database.get_metrics()))
            .collect()
    }

    pub fn prepare_for_writes(&self) -> Result<(), DatabaseOpenError> {
        for (name, database) in self.database_registry.read().served.iter() {
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

    fn new_served_database(&self, name: &str) -> Result<Database<WALClient>, DatabaseCreateError> {
        Database::<WALClient>::open(&self.data_directory.join(name), &self.diagnostics_manager, &self.rocks_resources)
            .map_err(|typedb_source| DatabaseCreateError::DatabaseOpen { typedb_source })
    }

    fn exists_served<'a>(&'a self, databases: &'a DatabasesWriteLock<'a>, name: &str) -> bool {
        let exists_served = self.data_directory.join(name).is_dir();
        assert_eq!(
            exists_served,
            databases.served.contains_key(name),
            "Served databases should be in the served database list: {name}"
        );
        exists_served
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
