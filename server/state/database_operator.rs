/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use concurrency::TokioTaskSpawner;
use database::{
    Database, DatabaseDeleteError, DatabaseOpenError,
    database::DatabaseCreateError,
    database_manager::DatabaseManager,
    migration::database_importer::{DatabaseImporter, ImportCommitError, ImportCommitter},
    transaction::{CommitIntent, DataCommitIntent, SchemaCommitIntent, TransactionRead},
};
use durability::DurabilitySequenceNumber;
use futures::future::join_all;
use resource::{constants::server::MAX_CONCURRENT_IMPORTS, profile::CommitProfile};
use storage::{
    durability_client::{DurabilityClient, WALClient},
    snapshot::snapshot_id::SnapshotId,
};
use tokio::{
    sync::{RwLock, Semaphore, mpsc::Sender},
    task::JoinHandle,
};

use crate::{
    error::{ArcServerStateError, LocalServerStateError, arc_server_state_err},
    service::{
        export_service::{get_transaction_schema, get_transaction_type_schema},
        grpc::migration::import_service::DatabaseImportService,
    },
};

#[async_trait]
pub trait DatabaseOperator: Debug + Send + Sync {
    async fn all(&self) -> Result<Vec<String>, ArcServerStateError>;

    async fn contains(&self, name: &str) -> Result<bool, ArcServerStateError>;

    async fn get(&self, name: &str) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError>;

    async fn get_unrestricted(&self, name: &str) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError>;

    async fn create(&self, name: &str) -> Result<(), ArcServerStateError>;

    async fn create_unrestricted(&self, name: &str) -> Result<(), ArcServerStateError>;

    async fn spawn_import_service(&self, service: DatabaseImportService)
    -> Result<JoinHandle<()>, ArcServerStateError>;

    fn import_committer(&self) -> Arc<dyn ImportCommitter>;

    async fn import_prepare(
        &self,
        name: &str,
        close_sender: Sender<()>,
    ) -> Result<DatabaseImporter, ArcServerStateError>;

    async fn import_cancel(&self, name: &str) -> Result<(), ArcServerStateError>;

    async fn schema(&self, name: &str) -> Result<String, ArcServerStateError>;

    async fn type_schema(&self, name: &str) -> Result<String, ArcServerStateError>;

    async fn schema_commit(
        &self,
        commit_intent: SchemaCommitIntent<WALClient>,
        commit_profile: CommitProfile,
    ) -> (CommitProfile, Result<(), ArcServerStateError>);

    async fn data_commit(
        &self,
        commit_intent: DataCommitIntent<WALClient>,
        commit_profile: CommitProfile,
    ) -> (CommitProfile, Result<(), ArcServerStateError>);

    async fn commit_record_exists(
        &self,
        name: &str,
        open_sequence_number: DurabilitySequenceNumber,
        snapshot_id: SnapshotId,
    ) -> Result<bool, ArcServerStateError>;

    async fn delete(&self, name: &str) -> Result<(), ArcServerStateError>;

    async fn prepare_for_writes(&self) -> Result<(), DatabaseOpenError>;
}

#[derive(Debug)]
pub(crate) struct ImportInfo {
    close_sender: Sender<()>,
}

#[derive(Debug)]
struct LocalImportCommitter {
    database_manager: Arc<DatabaseManager>,
}

fn import_commit<I: CommitIntent>(
    intent: I,
    map_err: impl FnOnce(I::Error) -> LocalServerStateError,
) -> Result<(), ImportCommitError> {
    let mut commit_profile = CommitProfile::DISABLED;
    intent.commit(&mut commit_profile).map_err(|typedb_source| Arc::new(map_err(typedb_source)) as _)
}

impl ImportCommitter for LocalImportCommitter {
    fn commit_schema(&self, intent: SchemaCommitIntent<WALClient>) -> Result<(), ImportCommitError> {
        import_commit(intent, |typedb_source| LocalServerStateError::DatabaseSchemaCommitFailed { typedb_source })
    }

    fn commit_data(&self, intent: DataCommitIntent<WALClient>) -> Result<(), ImportCommitError> {
        import_commit(intent, |typedb_source| LocalServerStateError::DatabaseDataCommitFailed { typedb_source })
    }

    fn finalise(&self, name: &str) -> Result<(), ImportCommitError> {
        self.database_manager.finalise_imported_database(name).map_err(|typedb_source| {
            Arc::new(LocalServerStateError::DatabaseImportFinaliseFailed { typedb_source }) as _
        })
    }
}

#[derive(Debug)]
pub struct LocalDatabaseOperator {
    database_manager: Arc<DatabaseManager>,
    background_task_spawner: TokioTaskSpawner,
    import_permits: Arc<Semaphore>,
    import_committer: Arc<LocalImportCommitter>,
    active_imports: Arc<RwLock<HashMap<String, ImportInfo>>>,
}

impl LocalDatabaseOperator {
    pub fn new(database_manager: Arc<DatabaseManager>, background_task_spawner: TokioTaskSpawner) -> Self {
        Self {
            import_committer: Arc::new(LocalImportCommitter { database_manager: database_manager.clone() }),
            database_manager,
            background_task_spawner,
            import_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_IMPORTS)),
            active_imports: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn record_import(&self, name: String, close_sender: Sender<()>) -> Result<(), DatabaseCreateError> {
        let close_sender_for_cleanup = close_sender.clone();
        let imports_for_cleanup = self.active_imports.clone();
        let mut imports = self.active_imports.write().await;
        match imports.get(&name) {
            Some(live) if !live.close_sender.is_closed() => {
                return Err(DatabaseCreateError::IsBeingImported { name });
            }
            _ => imports.insert(name.clone(), ImportInfo { close_sender }),
        };
        self.background_task_spawner.spawn(async move {
            close_sender_for_cleanup.closed().await;
            let mut imports = imports_for_cleanup.write().await;
            if let Some(info) = imports.get(&name) {
                if info.close_sender.same_channel(&close_sender_for_cleanup) {
                    imports.remove(&name);
                }
            }
        });
        Ok(())
    }

    pub async fn close_all_imports(&self) {
        let close_senders: Vec<Sender<()>> =
            self.active_imports.read().await.values().map(|info| info.close_sender.clone()).collect();
        join_all(close_senders.iter().map(|sender| async move {
            let _ = sender.send(()).await;
            sender.closed().await;
        }))
        .await;
    }

    pub fn new_importer(
        &self,
        database: Arc<Database<WALClient>>,
        committer: Arc<dyn ImportCommitter>,
    ) -> DatabaseImporter {
        DatabaseImporter::new(database, self.database_manager.import_directory().to_owned(), committer)
    }

    pub fn prepare_imported_database(&self, name: String) -> Result<Arc<Database<WALClient>>, DatabaseCreateError> {
        self.database_manager.prepare_imported_database(name)
    }

    pub fn imported_database(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        self.database_manager.imported_database(name)
    }

    pub fn imported_database_names(&self) -> Vec<String> {
        self.database_manager.imported_database_names()
    }

    pub fn is_abandoned_import(&self, name: &str) -> bool {
        self.database_manager.is_abandoned_import(name)
    }

    pub fn finalise_imported_database(&self, name: &str) -> Result<(), DatabaseCreateError> {
        self.database_manager.finalise_imported_database(name)
    }

    pub fn cancel_database_import(&self, name: &str) -> Result<(), DatabaseDeleteError> {
        self.database_manager.cancel_database_import(name)
    }
}

pub fn get_database_schema<D: DurabilityClient>(database: Arc<Database<D>>) -> Result<String, LocalServerStateError> {
    let transaction = TransactionRead::open(database, options::TransactionOptions::default())
        .map_err(|typedb_source| LocalServerStateError::FailedToOpenPrerequisiteTransaction { typedb_source })?;
    let schema = get_transaction_schema(&transaction)
        .map_err(|typedb_source| LocalServerStateError::DatabaseExport { typedb_source })?;
    Ok(schema)
}

pub fn get_database_type_schema<D: DurabilityClient>(
    database: Arc<Database<D>>,
) -> Result<String, LocalServerStateError> {
    let transaction = TransactionRead::open(database, options::TransactionOptions::default())
        .map_err(|typedb_source| LocalServerStateError::FailedToOpenPrerequisiteTransaction { typedb_source })?;
    let type_schema = get_transaction_type_schema(&transaction)
        .map_err(|typedb_source| LocalServerStateError::DatabaseExport { typedb_source })?;
    Ok(type_schema)
}

pub fn get_functions_syntax<D: DurabilityClient>(
    transaction: &TransactionRead<D>,
) -> Result<String, LocalServerStateError> {
    transaction
        .function_manager
        .get_functions_syntax(transaction.snapshot())
        .map_err(|typedb_source| LocalServerStateError::FunctionReadError { typedb_source })
}

pub fn get_types_syntax<D: DurabilityClient>(
    transaction: &TransactionRead<D>,
) -> Result<String, LocalServerStateError> {
    transaction
        .type_manager
        .get_types_syntax(transaction.snapshot())
        .map_err(|typedb_source| LocalServerStateError::ConceptReadError { typedb_source })
}

#[async_trait]
impl DatabaseOperator for LocalDatabaseOperator {
    async fn all(&self) -> Result<Vec<String>, ArcServerStateError> {
        Ok(self.database_manager.database_names())
    }

    async fn contains(&self, name: &str) -> Result<bool, ArcServerStateError> {
        Ok(self.database_manager.database(name).is_some())
    }

    async fn get(&self, name: &str) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError> {
        Ok(self.database_manager.database(name))
    }

    async fn get_unrestricted(&self, name: &str) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError> {
        Ok(self.database_manager.database_unrestricted(name))
    }

    async fn create(&self, name: &str) -> Result<(), ArcServerStateError> {
        self.database_manager
            .put_database(name)
            .map_err(|err| arc_server_state_err(LocalServerStateError::DatabaseCannotBeCreated { typedb_source: err }))
    }

    async fn create_unrestricted(&self, name: &str) -> Result<(), ArcServerStateError> {
        self.database_manager
            .put_database_unrestricted(name)
            .map_err(|err| arc_server_state_err(LocalServerStateError::DatabaseCannotBeCreated { typedb_source: err }))
    }

    async fn spawn_import_service(
        &self,
        service: DatabaseImportService,
    ) -> Result<JoinHandle<()>, ArcServerStateError> {
        let permit = self.import_permits.clone().try_acquire_owned().map_err(|_| {
            arc_server_state_err(LocalServerStateError::ConcurrentImportLimitReached { limit: MAX_CONCURRENT_IMPORTS })
        })?;
        Ok(self.background_task_spawner.spawn(async move {
            let _permit = permit;
            service.listen().await
        }))
    }

    fn import_committer(&self) -> Arc<dyn ImportCommitter> {
        self.import_committer.clone()
    }

    async fn import_prepare(
        &self,
        name: &str,
        close_sender: Sender<()>,
    ) -> Result<DatabaseImporter, ArcServerStateError> {
        let map_err =
            |typedb_source| arc_server_state_err(LocalServerStateError::DatabaseImportPrepareFailed { typedb_source });
        self.record_import(name.to_string(), close_sender).await.map_err(map_err)?;
        let database = self.prepare_imported_database(name.to_string()).map_err(map_err)?;
        Ok(self.new_importer(database, self.import_committer()))
    }

    async fn import_cancel(&self, name: &str) -> Result<(), ArcServerStateError> {
        self.cancel_database_import(name).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabaseImportCancelFailed { typedb_source })
        })
    }

    async fn schema(&self, name: &str) -> Result<String, ArcServerStateError> {
        match self.database_manager.database(name) {
            Some(db) => get_database_schema(db),
            None => Err(LocalServerStateError::DatabaseNotFound { name: name.to_string() }),
        }
        .map_err(arc_server_state_err)
    }

    async fn type_schema(&self, name: &str) -> Result<String, ArcServerStateError> {
        match self.database_manager.database(name) {
            None => Err(Arc::new(LocalServerStateError::DatabaseNotFound { name: name.to_string() })),
            Some(database) => match get_database_type_schema(database) {
                Ok(type_schema) => Ok(type_schema),
                Err(err) => Err(Arc::new(err)),
            },
        }
    }

    async fn schema_commit(
        &self,
        commit_intent: SchemaCommitIntent<WALClient>,
        mut commit_profile: CommitProfile,
    ) -> (CommitProfile, Result<(), ArcServerStateError>) {
        tokio::task::spawn_blocking(move || {
            let result = commit_intent.commit(&mut commit_profile).map_err(|typedb_source| {
                arc_server_state_err(LocalServerStateError::DatabaseSchemaCommitFailed { typedb_source })
            });
            commit_profile.end();
            (commit_profile, result)
        })
        .await
        .expect("Schema commit task panicked")
    }

    async fn data_commit(
        &self,
        commit_intent: DataCommitIntent<WALClient>,
        mut commit_profile: CommitProfile,
    ) -> (CommitProfile, Result<(), ArcServerStateError>) {
        tokio::task::spawn_blocking(move || {
            let result = commit_intent.commit(&mut commit_profile).map_err(|typedb_source| {
                arc_server_state_err(LocalServerStateError::DatabaseDataCommitFailed { typedb_source })
            });
            commit_profile.end();
            (commit_profile, result)
        })
        .await
        .expect("Data commit task panicked")
    }

    async fn commit_record_exists(
        &self,
        name: &str,
        open_sequence_number: DurabilitySequenceNumber,
        snapshot_id: SnapshotId,
    ) -> Result<bool, ArcServerStateError> {
        let Some(database) = self.get_unrestricted(name).await? else {
            return Err(Arc::new(LocalServerStateError::DatabaseNotFound { name: name.to_string() }));
        };
        database.commit_record_exists(open_sequence_number, snapshot_id).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabaseCommitRecordExistsFailed { typedb_source })
        })
    }

    async fn delete(&self, name: &str) -> Result<(), ArcServerStateError> {
        self.database_manager
            .delete_database(name)
            .map_err(|err| arc_server_state_err(LocalServerStateError::DatabaseCannotBeDeleted { typedb_source: err }))
    }

    async fn prepare_for_writes(&self) -> Result<(), DatabaseOpenError> {
        let database_manager = self.database_manager.clone();
        tokio::task::spawn_blocking(move || database_manager.prepare_for_writes())
            .await
            .expect("prepare_for_writes task panicked")
    }
}
