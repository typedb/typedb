/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use cache::CACHE_DB_NAME_PREFIX;
use diagnostics::diagnostics_manager::DiagnosticsManager;
pub(crate) use durability::sync_directory;
use options::byte_size::ByteSize;
use resource::{constants::database::INTERNAL_DATABASE_PREFIX, internal_database_prefix};
use storage::{durability_client::WALClient, keyspace::rocks_resources::RocksResources};
use tracing::{Level, debug, event, warn};

use crate::{
    Database, DatabaseDeleteError, DatabaseOpenError,
    database::DatabaseCreateError,
    database_registry::{DatabaseRegistry, Databases, DatabasesMap},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportOwnership {
    /// This process is the only one importing: nobody else can be holding an unfinished import,
    /// so it may discard and reclaim them freely.
    Exclusive,
    /// Other processes drive the same imports: an unfinished import may still be required, so this
    /// process may neither discard nor reclaim them on its own.
    Shared,
}

#[derive(Debug)]
pub struct DatabaseManager {
    data_directory: PathBuf,
    import_directory: PathBuf,
    import_ownership: ImportOwnership,
    database_registry: DatabaseRegistry,
    diagnostics_manager: Arc<DiagnosticsManager>,
    rocks_resources: Arc<RocksResources>,
}

impl DatabaseManager {
    const IMPORT_DIRECTORY_NAME: &'static str = concat!(internal_database_prefix!(), "import");

    pub fn new(
        data_directory: impl AsRef<Path>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        rocksdb_cache_size: ByteSize,
        rocksdb_write_buffers_limit: ByteSize,
        import_ownership: ImportOwnership,
    ) -> Result<Arc<Self>, DatabaseOpenError> {
        let data_directory = data_directory.as_ref().to_owned();
        let import_directory = data_directory.join(Self::IMPORT_DIRECTORY_NAME);
        let rocks_resources = Arc::new(RocksResources::new(rocksdb_cache_size, rocksdb_write_buffers_limit));
        let served_databases = Self::initialise_served_databases(
            &data_directory,
            &import_directory,
            &diagnostics_manager,
            &rocks_resources,
        )?;
        let staged_databases = Self::initialise_staged_databases(
            &import_directory,
            import_ownership,
            &diagnostics_manager,
            &rocks_resources,
        )?;
        let database_registry = DatabaseRegistry::new(served_databases, staged_databases);

        Ok(Arc::new(Self {
            data_directory,
            import_directory,
            import_ownership,
            database_registry,
            diagnostics_manager,
            rocks_resources,
        }))
    }

    fn initialise_served_databases(
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

    fn initialise_staged_databases(
        import_directory: &PathBuf,
        import_ownership: ImportOwnership,
        diagnostics_manager: &DiagnosticsManager,
        rocks_resources: &RocksResources,
    ) -> Result<DatabasesMap, DatabaseOpenError> {
        match import_ownership {
            ImportOwnership::Exclusive => {
                Self::cleanup_import_directory(import_directory)?;
                Ok(DatabasesMap::new())
            }
            ImportOwnership::Shared => {
                Self::recover_staged_databases(import_directory, diagnostics_manager, rocks_resources)
            }
        }
    }

    fn cleanup_import_directory(directory: &PathBuf) -> Result<(), DatabaseOpenError> {
        if !directory.exists() {
            return Ok(());
        }
        for entry_path in directory_entries(directory)? {
            if entry_path.is_dir() {
                let name = file_name_lossy(&entry_path);
                if name.starts_with(CACHE_DB_NAME_PREFIX) {
                    event!(
                        Level::DEBUG,
                        "Cache '{name}' was not removed after an interrupted import operation. It will be deleted."
                    );
                } else {
                    event!(
                        Level::DEBUG,
                        "Database '{name}' is in an incomplete state after an interrupted import operation. It will be deleted."
                    );
                }
                fs::remove_dir_all(&entry_path).map_err(|source| DatabaseOpenError::DirectoryDelete {
                    name: file_name_lossy(&entry_path),
                    source: Arc::new(source),
                })?;
            } else {
                event!(Level::DEBUG, "Removing import file @ {:?}: expected to be temporary", entry_path);
                fs::remove_file(&entry_path).map_err(|source| DatabaseOpenError::FileDelete {
                    name: file_name_lossy(&entry_path),
                    source: Arc::new(source),
                })?;
            }
        }
        Ok(())
    }

    fn recover_staged_databases(
        directory: &PathBuf,
        diagnostics_manager: &DiagnosticsManager,
        rocks_resources: &RocksResources,
    ) -> Result<DatabasesMap, DatabaseOpenError> {
        let mut recovered = DatabasesMap::new();
        if !directory.exists() {
            return Ok(recovered);
        }
        for entry_path in directory_entries(directory)? {
            let name = file_name_lossy(&entry_path);
            if !entry_path.is_dir() || name.starts_with(CACHE_DB_NAME_PREFIX) {
                Self::remove_import_leftover_best_effort(&entry_path);
                continue;
            }
            match Database::<WALClient>::open(&entry_path, diagnostics_manager, rocks_resources) {
                Ok(database) => {
                    event!(Level::INFO, "Recovered in-progress import of database '{name}' after restart.");
                    recovered.insert(database.name().to_owned(), Arc::new(database));
                }
                Err(typedb_source) => {
                    event!(
                        Level::WARN,
                        "Import database '{name}' could not be reopened after restart. The name stays \
                         reserved until the import is cancelled or prepared again: {typedb_source:?}"
                    );
                }
            }
        }
        Ok(recovered)
    }

    fn remove_import_leftover_best_effort(entry_path: &Path) {
        // Best effort: `is_dir` returns false when the entry's metadata cannot be read at all, so
        // such an entry takes the file branch, which removes it if it is a broken symlink and
        // otherwise fails for the same reason its metadata did. Failures here are logged and the
        // leftover is left for the next startup to retry
        let result = if entry_path.is_dir() { fs::remove_dir_all(entry_path) } else { fs::remove_file(entry_path) };
        if let Err(error) = result {
            debug!("Could not remove import leftover {entry_path:?}: {error}");
        }
    }

    pub fn put_database(&self, name: impl AsRef<str>) -> Result<(), DatabaseCreateError> {
        Self::validate_user_database_name(name.as_ref())?;
        self.put_database_unrestricted(name)
    }

    pub fn put_database_unrestricted(&self, name: impl AsRef<str>) -> Result<(), DatabaseCreateError> {
        let name = name.as_ref();
        let mut databases = self.database_registry.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        self.ensure_not_staged(&databases.staged, name)?;
        if !databases.served.contains_key(name) {
            let database = self.open_served_database(name)?;
            databases.served.insert(name.to_string(), Arc::new(database));
            self.sync_data_directory().map_err(|source| DatabaseCreateError::DirectoryWrite {
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
        let mut databases = self.database_registry.write().map_err(|_| DatabaseDeleteError::WriteAccessDenied {})?;
        let Some(database) = Self::take_database(&mut databases.served, name)? else {
            return Err(DatabaseDeleteError::DoesNotExist {});
        };
        database.delete()?;
        self.sync_data_directory()
            .map_err(|source| DatabaseDeleteError::DirectoryDelete { source: Arc::new(source) })?;
        Ok(())
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

    pub fn map_user_databases<T>(&self, mut f: impl FnMut(&Arc<Database<WALClient>>) -> T) -> Vec<T> {
        self.database_registry
            .read()
            .served
            .values()
            .filter(|database| Self::is_user_database(database.name()))
            .map(&mut f)
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

    pub fn prepare_imported_database(&self, name: String) -> Result<Arc<Database<WALClient>>, DatabaseCreateError> {
        Self::validate_user_database_name(&name)?;
        let mut databases = self.database_registry.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        if self.exists_served(&databases.served, &name) {
            return Err(DatabaseCreateError::AlreadyExists { name });
        }
        self.evict_staged_leftover(&mut databases.staged, &name)?;

        let map_fs_err = |source| DatabaseCreateError::DirectoryWrite { name: name.clone(), source: Arc::new(source) };
        fs::create_dir_all(&self.import_directory).map_err(map_fs_err)?;
        let database = Arc::new(self.open_staged_database(&name)?);
        self.sync_import_directory().map_err(map_fs_err)?;
        databases.staged.insert(name, database.clone());
        Ok(database)
    }

    pub fn finalise_imported_database(&self, name: &str) -> Result<(), DatabaseCreateError> {
        let mut databases = self.database_registry.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        let database = match Self::take_database(&mut databases.staged, name) {
            Ok(Some(database)) => database,
            Ok(None) => return Err(DatabaseCreateError::IsNotBeingImported { name: name.to_string() }),
            Err(_) => return Err(DatabaseCreateError::ImportedDatabaseInUse { name: name.to_string() }),
        };
        if self.exists_served(&databases.served, name) {
            database.delete().map_err(|typedb_source| DatabaseCreateError::AlreadyExistsAndCleanupBlocked {
                name: name.to_string(),
                typedb_source,
            })?;
            return Err(DatabaseCreateError::AlreadyExists { name: name.to_string() });
        }
        self.promote_staged_database(&mut databases, name, database)
    }

    pub fn discard_imported_database(&self, name: &str) -> Result<(), DatabaseDeleteError> {
        if Self::validate_user_database_name(name).is_err() {
            return Err(DatabaseDeleteError::DatabaseIsNotBeingImported { name: name.to_string() });
        }
        let map_fs_err = |source| DatabaseDeleteError::DirectoryDelete { source: Arc::new(source) };
        let mut databases = self.database_registry.write().map_err(|_| DatabaseDeleteError::WriteAccessDenied {})?;
        let Some(database) = Self::take_database(&mut databases.staged, name)? else {
            if self.staged_database_exists(name) {
                fs::remove_dir_all(self.staged_database_path(name)).map_err(map_fs_err)?;
                self.sync_import_directory().map_err(map_fs_err)?;
            }
            return Err(DatabaseDeleteError::DatabaseIsNotBeingImported { name: name.to_string() });
        };
        database.delete()?;
        self.sync_import_directory().map_err(map_fs_err)?;
        Ok(())
    }

    pub fn imported_database(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        self.database_registry.staged(name)
    }

    pub fn imported_database_names(&self) -> Vec<String> {
        self.database_registry.staged_names()
    }

    pub fn is_abandoned_import(&self, name: &str) -> bool {
        !self.database_registry.read().staged.contains_key(name) && self.staged_database_exists(name)
    }

    pub fn import_directory(&self) -> &Path {
        &self.import_directory
    }

    pub fn rocks_resources(&self) -> &Arc<RocksResources> {
        &self.rocks_resources
    }

    pub fn is_user_database(name: &str) -> bool {
        !Self::is_internal_database(name)
    }

    pub fn is_internal_database(name: &str) -> bool {
        name.starts_with(INTERNAL_DATABASE_PREFIX)
    }

    fn take_database(
        databases: &mut DatabasesMap,
        name: &str,
    ) -> Result<Option<Database<WALClient>>, DatabaseDeleteError> {
        let Some(database) = databases.remove(name) else {
            return Ok(None);
        };
        match Arc::try_unwrap(database) {
            Ok(database) => Ok(Some(database)),
            Err(database) => {
                databases.insert(name.to_string(), database);
                Err(DatabaseDeleteError::InUse {})
            }
        }
    }

    fn open_served_database(&self, name: &str) -> Result<Database<WALClient>, DatabaseCreateError> {
        Database::<WALClient>::open(&self.served_database_path(name), &self.diagnostics_manager, &self.rocks_resources)
            .map_err(|typedb_source| DatabaseCreateError::DatabaseOpen { typedb_source })
    }

    fn exists_served(&self, served: &DatabasesMap, name: &str) -> bool {
        let exists_served = self.served_database_path(name).is_dir();
        assert_eq!(
            exists_served,
            served.contains_key(name),
            "Served databases should be in the served database list: {name}"
        );
        exists_served
    }

    fn promote_staged_database(
        &self,
        databases: &mut Databases,
        name: &str,
        database: Database<WALClient>,
    ) -> Result<(), DatabaseCreateError> {
        let staged_path = database.path.clone();
        drop(database);
        self.move_directory_to_data(name, &staged_path)?;
        let map_fs_err =
            |source| DatabaseCreateError::DirectoryWrite { name: name.to_string(), source: Arc::new(source) };
        self.sync_data_directory().map_err(map_fs_err)?;
        self.sync_import_directory().map_err(map_fs_err)?;
        let database = self.open_served_database(name)?;
        databases.served.insert(name.to_string(), Arc::new(database));
        Ok(())
    }

    fn move_directory_to_data(&self, name: &str, directory: &Path) -> Result<(), DatabaseCreateError> {
        let directory_name =
            directory.file_name().ok_or_else(|| DatabaseCreateError::DatabaseMove { name: name.to_string() })?;

        let target_path = self.data_directory.join(directory_name);
        fs::rename(directory, &target_path)
            .map_err(|source| DatabaseCreateError::DirectoryWrite { name: name.to_string(), source: Arc::new(source) })
    }

    fn ensure_not_staged(&self, staged: &DatabasesMap, name: &str) -> Result<(), DatabaseCreateError> {
        if staged.contains_key(name) {
            return Err(DatabaseCreateError::IsBeingImported { name: name.to_string() });
        }
        if !self.staged_database_exists(name) {
            return Ok(());
        }
        match self.import_ownership {
            ImportOwnership::Shared => Err(DatabaseCreateError::IsBeingImported { name: name.to_string() }),
            ImportOwnership::Exclusive => {
                let map_fs_err =
                    |source| DatabaseCreateError::DirectoryWrite { name: name.to_string(), source: Arc::new(source) };
                fs::remove_dir_all(self.staged_database_path(name)).map_err(map_fs_err)?;
                self.sync_import_directory().map_err(map_fs_err)
            }
        }
    }

    fn evict_staged_leftover(&self, staged: &mut DatabasesMap, name: &str) -> Result<(), DatabaseCreateError> {
        match Self::take_database(staged, name) {
            Ok(Some(leftover)) => leftover.delete().map_err(|typedb_source| DatabaseCreateError::ImportCleanupFailed {
                name: name.to_string(),
                typedb_source,
            }),
            Ok(None) if self.staged_database_exists(name) => {
                fs::remove_dir_all(self.staged_database_path(name)).map_err(|source| {
                    DatabaseCreateError::DirectoryWrite { name: name.to_string(), source: Arc::new(source) }
                })
            }
            Ok(None) => Ok(()),
            Err(_) => Err(DatabaseCreateError::ImportedDatabaseInUse { name: name.to_string() }),
        }
    }

    fn open_staged_database(&self, name: &str) -> Result<Database<WALClient>, DatabaseCreateError> {
        Database::<WALClient>::open(&self.staged_database_path(name), &self.diagnostics_manager, &self.rocks_resources)
            .map_err(|typedb_source| DatabaseCreateError::DatabaseOpen { typedb_source })
    }

    fn served_database_path(&self, name: &str) -> PathBuf {
        self.data_directory.join(name)
    }

    fn staged_database_path(&self, name: &str) -> PathBuf {
        self.import_directory.join(name)
    }

    fn staged_database_exists(&self, name: &str) -> bool {
        self.staged_database_path(name).is_dir()
    }

    fn sync_data_directory(&self) -> io::Result<()> {
        sync_directory(&self.data_directory)
    }

    fn sync_import_directory(&self) -> io::Result<()> {
        sync_directory(&self.import_directory)
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
