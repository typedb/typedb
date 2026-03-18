/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use concurrency::TokioTaskSpawner;
use database::{
    database_manager::DatabaseManager,
    transaction::{DataCommitError, DataCommitIntent, SchemaCommitError, SchemaCommitIntent, TransactionRead},
    Database,
};
use durability::DurabilitySequenceNumber;
use resource::profile::CommitProfile;
use storage::{
    durability_client::{DurabilityClient, WALClient},
    record::CommitRecord,
    snapshot::{snapshot_id::SnapshotId, CommittableSnapshot, SchemaSnapshot, WriteSnapshot},
};
use tokio::task::JoinHandle;

use crate::{
    error::{arc_server_state_err, ArcServerStateError, LocalServerStateError},
    service::{
        export_service::{get_transaction_schema, get_transaction_type_schema},
        grpc::migration::import_service::DatabaseImportService,
        TransactionType,
    },
};

#[async_trait]
pub trait ServerDatabaseManager: Debug + Send + Sync {
    async fn databases_all(&self) -> Result<Vec<String>, ArcServerStateError>;

    async fn databases_contains(&self, name: &str) -> Result<bool, ArcServerStateError>;

    async fn databases_get(&self, name: &str) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError>;

    async fn databases_get_unrestricted(
        &self,
        name: &str,
    ) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError>;

    async fn databases_create(&self, name: &str) -> Result<(), ArcServerStateError>;

    async fn databases_create_unrestricted(&self, name: &str) -> Result<(), ArcServerStateError>;

    async fn databases_import(&self, service: DatabaseImportService) -> Result<JoinHandle<()>, ArcServerStateError>;

    async fn database_schema(&self, name: &str) -> Result<String, ArcServerStateError>;

    async fn database_type_schema(&self, name: &str) -> Result<String, ArcServerStateError>;

    async fn database_schema_commit(
        &self,
        commit_intent: SchemaCommitIntent<WALClient>,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ArcServerStateError>;

    async fn database_data_commit(
        &self,
        commit_intent: DataCommitIntent<WALClient>,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ArcServerStateError>;

    async fn database_commit_record_exists(
        &self,
        name: &str,
        open_sequence_number: DurabilitySequenceNumber,
        snapshot_id: SnapshotId,
    ) -> Result<bool, ArcServerStateError>;

    async fn database_delete(&self, name: &str) -> Result<(), ArcServerStateError>;

    fn manager(&self) -> Arc<DatabaseManager>;
}

#[derive(Debug)]
pub struct LocalServerDatabaseManager {
    database_manager: Arc<DatabaseManager>,
    background_task_spawner: TokioTaskSpawner,
}

impl LocalServerDatabaseManager {
    pub fn new(database_manager: Arc<DatabaseManager>, background_task_spawner: TokioTaskSpawner) -> Self {
        Self { database_manager, background_task_spawner }
    }
}

pub fn get_database_schema<D: DurabilityClient>(database: Arc<Database<D>>) -> Result<String, LocalServerStateError> {
    let transaction = TransactionRead::open(database, options::TransactionOptions::default())
        .map_err(|typedb_source| LocalServerStateError::FailedToOpenPrerequisiteTransaction { typedb_source })?;
    let schema = get_transaction_schema(&transaction)
        .map_err(|typedb_source| LocalServerStateError::DatabaseExport { typedb_source })?;
    Ok(schema)
}

pub(crate) fn get_database_type_schema<D: DurabilityClient>(
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
impl ServerDatabaseManager for LocalServerDatabaseManager {
    async fn databases_all(&self) -> Result<Vec<String>, ArcServerStateError> {
        Ok(self.database_manager.database_names())
    }

    async fn databases_contains(&self, name: &str) -> Result<bool, ArcServerStateError> {
        Ok(self.database_manager.database(name).is_some())
    }

    async fn databases_get(&self, name: &str) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError> {
        Ok(self.database_manager.database(name))
    }

    async fn databases_get_unrestricted(
        &self,
        name: &str,
    ) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError> {
        Ok(self.database_manager.database_unrestricted(name))
    }

    async fn databases_create(&self, name: &str) -> Result<(), ArcServerStateError> {
        self.database_manager
            .put_database(name)
            .map_err(|err| arc_server_state_err(LocalServerStateError::DatabaseCannotBeCreated { typedb_source: err }))
    }

    async fn databases_create_unrestricted(&self, name: &str) -> Result<(), ArcServerStateError> {
        self.database_manager
            .put_database_unrestricted(name)
            .map_err(|err| arc_server_state_err(LocalServerStateError::DatabaseCannotBeCreated { typedb_source: err }))
    }

    async fn databases_import(&self, service: DatabaseImportService) -> Result<JoinHandle<()>, ArcServerStateError> {
        Ok(self.background_task_spawner.spawn(async move { service.listen().await }))
    }

    async fn database_schema(&self, name: &str) -> Result<String, ArcServerStateError> {
        match self.database_manager.database(name) {
            Some(db) => get_database_schema(db),
            None => Err(LocalServerStateError::DatabaseNotFound { name: name.to_string() }),
        }
        .map_err(arc_server_state_err)
    }

    async fn database_type_schema(&self, name: &str) -> Result<String, ArcServerStateError> {
        match self.database_manager.database(name) {
            None => Err(Arc::new(LocalServerStateError::DatabaseNotFound { name: name.to_string() })),
            Some(database) => match get_database_type_schema(database) {
                Ok(type_schema) => Ok(type_schema),
                Err(err) => Err(Arc::new(err)),
            },
        }
    }

    async fn database_schema_commit(
        &self,
        commit_intent: SchemaCommitIntent<WALClient>,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ArcServerStateError> {
        commit_intent.schema_snapshot.commit(commit_profile).map(|_| ()).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabaseSchemaCommitFailed {
                typedb_source: SchemaCommitError::SnapshotError { typedb_source },
            })
        })
    }

    async fn database_data_commit(
        &self,
        commit_intent: DataCommitIntent<WALClient>,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ArcServerStateError> {
        commit_intent.write_snapshot.commit(commit_profile).map(|_| ()).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabaseDataCommitFailed {
                typedb_source: DataCommitError::SnapshotError { typedb_source },
            })
        })
    }

    async fn database_commit_record_exists(
        &self,
        name: &str,
        open_sequence_number: DurabilitySequenceNumber,
        snapshot_id: SnapshotId,
    ) -> Result<bool, ArcServerStateError> {
        let Some(database) = self.databases_get_unrestricted(name).await? else {
            return Err(Arc::new(LocalServerStateError::DatabaseNotFound { name: name.to_string() }));
        };
        database.commit_record_exists(open_sequence_number, snapshot_id).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabaseCommitRecordExistsFailed { typedb_source })
        })
    }

    async fn database_delete(&self, name: &str) -> Result<(), ArcServerStateError> {
        self.database_manager
            .delete_database(name)
            .map_err(|err| arc_server_state_err(LocalServerStateError::DatabaseCannotBeDeleted { typedb_source: err }))
    }

    fn manager(&self) -> Arc<DatabaseManager> {
        self.database_manager.clone()
    }
}
