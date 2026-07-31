/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use cache::CACHE_DB_NAME_PREFIX;
use diagnostics::diagnostics_manager::DiagnosticsManager;
use resource::internal_database_prefix;
use storage::{durability_client::WALClient, keyspace::rocks_resources::RocksResources};
use tracing::{Level, debug, event};

use crate::{
    Database, DatabaseDeleteError, DatabaseOpenError,
    database::DatabaseCreateError,
    database_manager::{directory_entries, file_name_lossy, sync_directory},
};

#[derive(Debug)]
pub(crate) struct DatabaseImportManager {
    directory: PathBuf,
    databases: RwLock<HashMap<String, Arc<Database<WALClient>>>>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    rocks_resources: Arc<RocksResources>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportRecovery {
    Discard,
    Resume,
}

struct RecoveredImports {
    databases: HashMap<String, Arc<Database<WALClient>>>,
}

impl RecoveredImports {
    fn none() -> Self {
        Self { databases: HashMap::new() }
    }
}

impl DatabaseImportManager {
    const DIRECTORY_NAME: &'static str = concat!(internal_database_prefix!(), "import");

    pub(crate) fn new(
        data_directory: &Path,
        recovery: ImportRecovery,
        diagnostics_manager: Arc<DiagnosticsManager>,
        rocks_resources: Arc<RocksResources>,
    ) -> Result<Self, DatabaseOpenError> {
        let directory = data_directory.join(Self::DIRECTORY_NAME);
        let recovered = match recovery {
            ImportRecovery::Discard => {
                Self::cleanup_directory(&directory)?;
                RecoveredImports::none()
            }
            ImportRecovery::Resume => Self::recover_directory(&directory, &diagnostics_manager, &rocks_resources)?,
        };
        Ok(Self { directory, databases: RwLock::new(recovered.databases), diagnostics_manager, rocks_resources })
    }

    pub(crate) fn get(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        self.databases.read().unwrap().get(name).cloned()
    }

    pub(crate) fn names(&self) -> Vec<String> {
        self.databases.read().unwrap().keys().cloned().collect()
    }

    pub(crate) fn ensure_available(&self, name: &str) -> Result<(), DatabaseCreateError> {
        if self.databases.read().map_err(|_| DatabaseCreateError::ReadAccessDenied {})?.contains_key(name) {
            return Err(DatabaseCreateError::IsBeingImported { name: name.to_string() });
        }
        if self.exists(name) {
            let map_fs_err =
                |source| DatabaseCreateError::DirectoryWrite { name: name.to_string(), source: Arc::new(source) };
            fs::remove_dir_all(self.directory.join(name)).map_err(map_fs_err)?;
            self.sync().map_err(map_fs_err)?;
        }
        Ok(())
    }

    pub(crate) fn create_staging(&self, name: &str) -> Result<Arc<Database<WALClient>>, DatabaseCreateError> {
        let map_fs_err =
            |source| DatabaseCreateError::DirectoryWrite { name: name.to_string(), source: Arc::new(source) };
        fs::create_dir_all(&self.directory).map_err(map_fs_err)?;
        let mut databases = self.databases.write().map_err(|_| DatabaseCreateError::WriteAccessDenied {})?;
        if let Some(existing) = databases.remove(name) {
            match Arc::try_unwrap(existing) {
                Ok(existing) => existing.delete().map_err(|typedb_source| {
                    DatabaseCreateError::ImportCleanupFailed { name: name.to_string(), typedb_source }
                })?,
                Err(existing) => {
                    databases.insert(name.to_string(), existing);
                    return Err(DatabaseCreateError::IsBeingImported { name: name.to_string() });
                }
            }
        } else if self.exists(name) {
            fs::remove_dir_all(self.directory.join(name)).map_err(map_fs_err)?;
        }

        let database =
            Database::<WALClient>::open(&self.directory.join(name), &self.diagnostics_manager, &self.rocks_resources)
                .map_err(|typedb_source| DatabaseCreateError::DatabaseOpen { typedb_source })?;
        let database = Arc::new(database);
        databases.insert(name.to_string(), database.clone());
        self.sync().map_err(map_fs_err)?;
        Ok(database)
    }

    pub(crate) fn take_staging(&self, name: &str) -> Result<Option<Database<WALClient>>, DatabaseDeleteError> {
        let mut databases = self.databases.write().map_err(|_| DatabaseDeleteError::WriteAccessDenied {})?;
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

    pub(crate) fn delete_staging(&self, name: &str) -> Result<(), DatabaseDeleteError> {
        let map_fs_err = |source| DatabaseDeleteError::DirectoryDelete { source: Arc::new(source) };
        let Some(database) = self.take_staging(name)? else {
            if self.exists(name) {
                fs::remove_dir_all(self.directory.join(name)).map_err(map_fs_err)?;
                self.sync().map_err(map_fs_err)?;
            }
            return Err(DatabaseDeleteError::DatabaseIsNotBeingImported { name: name.to_string() });
        };
        database.delete()?;
        self.sync().map_err(map_fs_err)?;
        Ok(())
    }

    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }

    pub(crate) fn sync(&self) -> io::Result<()> {
        sync_directory(&self.directory)
    }

    fn exists(&self, name: &str) -> bool {
        self.directory.join(name).is_dir()
    }

    fn cleanup_directory(directory: &PathBuf) -> Result<(), DatabaseOpenError> {
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

    fn recover_directory(
        directory: &PathBuf,
        diagnostics_manager: &DiagnosticsManager,
        rocks_resources: &RocksResources,
    ) -> Result<RecoveredImports, DatabaseOpenError> {
        let mut recovered = RecoveredImports::none();
        if !directory.exists() {
            return Ok(recovered);
        }
        for entry_path in directory_entries(directory)? {
            let name = file_name_lossy(&entry_path);
            if !entry_path.is_dir() || name.starts_with(CACHE_DB_NAME_PREFIX) {
                Self::remove_entry_best_effort(&entry_path);
                continue;
            }
            match Database::<WALClient>::open(&entry_path, diagnostics_manager, rocks_resources) {
                Ok(database) => {
                    event!(Level::INFO, "Recovered in-progress import of database '{name}' after restart.");
                    recovered.databases.insert(database.name().to_owned(), Arc::new(database));
                }
                Err(typedb_source) => {
                    event!(
                        Level::WARN,
                        "Import database '{name}' could not be reopened after restart; discarding it: {typedb_source:?}"
                    );
                    Self::remove_entry_best_effort(&entry_path);
                }
            }
        }
        Ok(recovered)
    }

    fn remove_entry_best_effort(entry_path: &Path) {
        let result = if entry_path.is_dir() { fs::remove_dir_all(entry_path) } else { fs::remove_file(entry_path) };
        if let Err(error) = result {
            debug!("Could not remove import leftover {entry_path:?}: {error}");
        }
    }
}
