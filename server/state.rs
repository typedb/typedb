/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt::Debug, net::SocketAddr, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use concept::error::ConceptReadError;
use concurrency::IntervalRunner;
use database::{
    database::DatabaseCreateError,
    database_manager::DatabaseManager,
    transaction::{DataCommitError, SchemaCommitError, TransactionError, TransactionRead},
    Database, DatabaseDeleteError,
};
use diagnostics::{diagnostics_manager::DiagnosticsManager, Diagnostics};
use error::typedb_error;
use futures::{StreamExt, TryFutureExt};
use ir::pipeline::FunctionReadError;
use itertools::Itertools;
use options::TransactionOptions;
use resource::{
    constants::server::DATABASE_METRICS_UPDATE_INTERVAL, distribution_info::DistributionInfo, profile::CommitProfile,
};
use storage::{
    durability_client::{DurabilityClient, WALClient},
    isolation_manager::CommitRecord,
};
use system::{
    concepts::{Credential, User},
    initialise_system_database,
};
use tokio::{net::lookup_host, sync::watch::Receiver};
use database::transaction::{DataCommitError, SchemaCommitError};
use resource::profile::CommitProfile;
use storage::isolation_manager::CommitRecord;
use storage::snapshot::{SchemaSnapshot, WriteSnapshot};
use user::{
    errors::{UserCreateError, UserDeleteError, UserGetError, UserUpdateError},
    initialise_default_user,
    permission_manager::PermissionManager,
    user_manager::UserManager,
};

use crate::{
    authentication::{
        credential_verifier::CredentialVerifier, token_manager::TokenManager, Accessor, AuthenticationError,
    },
    error::ServerOpenError,
    parameters::config::{Config, DiagnosticsConfig},
    service::export_service::{get_transaction_schema, get_transaction_type_schema, DatabaseExportError},
    status::{LocalServerStatus, ServerStatus},
};

pub type DynServerState = dyn ServerState + Send + Sync;
pub type ArcServerState = Arc<DynServerState>;

pub type BoxServerStatus = Box<dyn ServerStatus + Send + Sync>;

#[async_trait]
pub trait ServerState: Debug {
    async fn distribution_info(&self) -> DistributionInfo;

    // TODO: grpc_address and http_address don't really suit "ServerState"
    async fn grpc_address(&self) -> SocketAddr;

    async fn http_address(&self) -> Option<SocketAddr>;

    // TODO: Name server_status -> servers_get and servers_statuses -> servers_all like in GRPC?
    async fn server_status(&self) -> Result<BoxServerStatus, ServerStateError>;

    async fn servers_statuses(&self) -> Result<Vec<BoxServerStatus>, ServerStateError>;

    async fn servers_register(&self, clustering_id: u64, clustering_address: String) -> Result<(), ServerStateError>;

    async fn servers_deregister(&self, clustering_id: u64) -> Result<(), ServerStateError>;

    async fn databases_all(&self) -> Result<Vec<String>, ServerStateError>;

    async fn databases_get(&self, name: &str) -> Option<Arc<Database<WALClient>>>;

    async fn databases_contains(&self, name: &str) -> bool;

    async fn databases_create(&self, name: &str) -> Result<(), ServerStateError>;

    async fn database_schema(&self, name: String) -> Result<String, ServerStateError>;

    async fn database_type_schema(&self, name: String) -> Result<String, ServerStateError>;

    async fn database_schema_commit(
        &self,
        name: &str,
        commit_record: CommitRecord,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ServerStateError>;

    async fn database_data_commit(
        &self,
        name: &str,
        commit_record: CommitRecord,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ServerStateError>;

    async fn database_delete(&self, name: &str) -> Result<(), ServerStateError>;

    async fn users_get(&self, name: &str, accessor: Accessor) -> Result<User, ServerStateError>;

    async fn users_all(&self, accessor: Accessor) -> Result<Vec<User>, ServerStateError>;

    async fn users_contains(&self, name: &str) -> Result<bool, ServerStateError>;

    async fn users_create(
        &self,
        user: &User,
        credential: &Credential,
        accessor: Accessor,
    ) -> Result<(), ServerStateError>;

    async fn users_update(
        &self,
        name: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
        accessor: Accessor,
    ) -> Result<(), ServerStateError>;

    async fn users_delete(&self, name: &str, accessor: Accessor) -> Result<(), ServerStateError>;

    async fn user_verify_password(&self, username: &str, password: &str) -> Result<(), ServerStateError>;

    async fn token_create(&self, username: String, password: String) -> Result<String, ServerStateError>;

    async fn token_get_owner(&self, token: &str) -> Option<String>;

    async fn database_manager(&self) -> Arc<DatabaseManager>;

    async fn diagnostics_manager(&self) -> Arc<DiagnosticsManager>;

    fn shutdown_receiver(&self) -> Receiver<()>;
}

typedb_error! {
    pub ServerStateError(component = "Server state", prefix = "SRV") {
        NotInitialised(16, "Not yet initialised"),
        Unimplemented(1, "Not implemented: {description}", description: String),
        OperationFailedDueToReplicaUnavailability(12, "Unable to execute as one or more servers could not respond in time"),
        OperationFailedNonPrimaryReplica(13, "Unable to execute as this server is not the primary replica"),
        ReplicaRegistrationNoConnection(21, "Unable to register replica '{id}' as there is no connection to '{address}'. Make sure that a correct clustering address (different from client connection address) is provided.", id: u64, address: String),
        ReplicaNotFound(22, "Unable to execute as the replica is not found."),
        OperationNotPermitted(2, "The user is not permitted to execute the operation"),
        DatabaseNotFound(3, "Database '{name}' not found.", name: String),
        DatabaseSchemaCommitFailed(19, "Schema commit failed.", typedb_source: SchemaCommitError),
        DatabaseDataCommitFailed(20, "Data commit failed.", typedb_source: DataCommitError),
        DatabaseCannotBeCreated(14, "Unable to create database", typedb_source: DatabaseCreateError),
        DatabaseCannotBeDeleted(15, "Unable to delete database", typedb_source: DatabaseDeleteError),
        UserNotFound(4, "User not found."),
        UserCannotBeRetrieved(8, "Unable to retrieve user", typedb_source: UserGetError),
        UserCannotBeCreated(9, "Unable to create user", typedb_source: UserCreateError),
        UserCannotBeUpdated(10, "Unable to update user", typedb_source: UserUpdateError),
        UserCannotBeDeleted(11, "Unable to delete user", typedb_source: UserDeleteError),
        FailedToOpenPrerequisiteTransaction(5, "Failed to open transaction, which is a prerequisite for the operation.", typedb_source: TransactionError),
        ConceptReadError(6, "Error reading concepts", typedb_source: Box<ConceptReadError>),
        FunctionReadError(7, "Error reading functions", typedb_source: FunctionReadError),
        AuthenticationError(17, "Error when authenticating", typedb_source: AuthenticationError),
        DatabaseExport(18, "Database export error", typedb_source: DatabaseExportError),
    }
}

#[derive(Debug)]
pub struct LocalServerState {
    distribution_info: DistributionInfo,
    grpc_address: SocketAddr,
    http_address: Option<SocketAddr>,
    server_status: LocalServerStatus,
    database_manager: Arc<DatabaseManager>,
    user_manager: Option<Arc<UserManager>>,
    credential_verifier: Option<Arc<CredentialVerifier>>,
    token_manager: Arc<TokenManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    _database_diagnostics_updater: IntervalRunner,
    shutdown_receiver: Receiver<()>,
}

impl LocalServerState {
    pub async fn new(
        distribution_info: DistributionInfo,
        config: Config,
        server_id: String,
        deployment_id: Option<String>,
        shutdown_receiver: Receiver<()>,
    ) -> Result<Self, ServerOpenError> {
        let database_manager = DatabaseManager::new(&config.storage.data_directory)
            .map_err(|err| ServerOpenError::DatabaseOpen { typedb_source: err })?;
        let token_manager = Arc::new(
            TokenManager::new(config.server.authentication.token_expiration)
                .map_err(|err| ServerOpenError::TokenConfiguration { typedb_source: err })?,
        );

        let deployment_id = deployment_id.unwrap_or(server_id.clone());
        let diagnostics_manager = Arc::new(
            Self::initialise_diagnostics(
                deployment_id.clone(),
                server_id.clone(),
                distribution_info,
                &config.diagnostics,
                config.storage.data_directory.clone(),
                config.development_mode.enabled,
            )
            .await,
        );

        let grpc_address = Self::resolve_address(&config.server.address).await;
        let http_address = if config.server.http.enabled {
            Some(Self::validate_and_resolve_http_address(&config.server.http.address, grpc_address).await?)
        } else {
            None
        };

        Ok(Self {
            distribution_info,
            server_status: LocalServerStatus::from_addresses(grpc_address, http_address),
            grpc_address,
            http_address,
            database_manager: database_manager.clone(),
            user_manager: None,
            credential_verifier: None,
            token_manager,
            diagnostics_manager: diagnostics_manager.clone(),
            _database_diagnostics_updater: IntervalRunner::new(
                move || Self::synchronize_database_metrics(diagnostics_manager.clone(), database_manager.clone()),
                DATABASE_METRICS_UPDATE_INTERVAL,
            ),
            shutdown_receiver,
        })
    }

    pub fn initialise(&mut self) {
        let system_database = initialise_system_database(&self.database_manager);
        let user_manager = Arc::new(UserManager::new(system_database));
        initialise_default_user(&user_manager);
        let credential_verifier = Some(Arc::new(CredentialVerifier::new(user_manager.clone())));
        self.user_manager = Some(user_manager);
        self.credential_verifier = credential_verifier;
    }

    async fn initialise_diagnostics(
        deployment_id: String,
        server_id: String,
        distribution_info: DistributionInfo,
        config: &DiagnosticsConfig,
        storage_directory: PathBuf,
        is_development_mode: bool,
    ) -> DiagnosticsManager {
        let diagnostics = Diagnostics::new(
            deployment_id,
            server_id,
            distribution_info.distribution.to_owned(),
            distribution_info.version.to_owned(),
            storage_directory,
            config.reporting.report_metrics,
        );
        let diagnostics_manager = DiagnosticsManager::new(
            diagnostics,
            config.monitoring.port,
            config.monitoring.enabled,
            is_development_mode,
        );
        diagnostics_manager.may_start_monitoring().await;
        diagnostics_manager.may_start_reporting().await;

        diagnostics_manager
    }

    fn synchronize_database_metrics(
        diagnostics_manager: Arc<DiagnosticsManager>,
        database_manager: Arc<DatabaseManager>,
    ) {
        let metrics = database_manager
            .databases()
            .values()
            .filter(|database| DatabaseManager::is_user_database(database.name()))
            .map(|database| database.get_metrics())
            .collect();
        diagnostics_manager.submit_database_metrics(metrics);
    }

    pub fn get_database_schema<D: DurabilityClient>(database: Arc<Database<D>>) -> Result<String, ServerStateError> {
        let transaction = TransactionRead::open(database, TransactionOptions::default())
            .map_err(|typedb_source| ServerStateError::FailedToOpenPrerequisiteTransaction { typedb_source })?;
        let schema = get_transaction_schema(&transaction)
            .map_err(|typedb_source| ServerStateError::DatabaseExport { typedb_source })?;
        Ok(schema)
    }

    pub fn get_functions_syntax<D: DurabilityClient>(
        transaction: &TransactionRead<D>,
    ) -> Result<String, ServerStateError> {
        transaction
            .function_manager
            .get_functions_syntax(transaction.snapshot())
            .map_err(|err| ServerStateError::FunctionReadError { typedb_source: err })
    }

    pub(crate) fn get_database_type_schema<D: DurabilityClient>(
        database: Arc<Database<D>>,
    ) -> Result<String, ServerStateError> {
        let transaction = TransactionRead::open(database, TransactionOptions::default())
            .map_err(|typedb_source| ServerStateError::FailedToOpenPrerequisiteTransaction { typedb_source })?;
        let type_schema = get_transaction_type_schema(&transaction)
            .map_err(|typedb_source| ServerStateError::DatabaseExport { typedb_source })?;
        Ok(type_schema)
    }

    pub fn get_types_syntax<D: DurabilityClient>(transaction: &TransactionRead<D>) -> Result<String, ServerStateError> {
        transaction
            .type_manager
            .get_types_syntax(transaction.snapshot())
            .map_err(|err| ServerStateError::ConceptReadError { typedb_source: err })
    }

    pub fn local_server_status(&self) -> LocalServerStatus {
        self.server_status.clone()
    }

    fn get_user_manager(&self) -> Result<Arc<UserManager>, ServerStateError> {
        match self.user_manager.clone() {
            Some(user_manager) => Ok(user_manager),
            None => Err(ServerStateError::NotInitialised {}),
        }
    }

    fn get_credential_verifier(&self) -> Result<Arc<CredentialVerifier>, ServerStateError> {
        match self.credential_verifier.clone() {
            Some(credential_verifier) => Ok(credential_verifier),
            None => Err(ServerStateError::NotInitialised {}),
        }
    }

    async fn validate_and_resolve_http_address(
        http_address: &str,
        grpc_address: SocketAddr,
    ) -> Result<SocketAddr, ServerOpenError> {
        let http_address = Self::resolve_address(http_address).await;
        if grpc_address == http_address {
            return Err(ServerOpenError::GrpcHttpConflictingAddress { address: grpc_address });
        }
        Ok(http_address)
    }

    pub async fn resolve_address(address: &str) -> SocketAddr {
        lookup_host(address)
            .await
            .unwrap()
            .next()
            .unwrap_or_else(|| panic!("Unable to map address '{}' to any IP addresses", address))
    }
}

#[async_trait]
impl ServerState for LocalServerState {
    async fn distribution_info(&self) -> DistributionInfo {
        self.distribution_info
    }

    async fn grpc_address(&self) -> SocketAddr {
        self.grpc_address
    }

    async fn http_address(&self) -> Option<SocketAddr> {
        self.http_address
    }

    async fn server_status(&self) -> Result<BoxServerStatus, ServerStateError> {
        Ok(Box::new(self.server_status.clone()))
    }

    async fn servers_statuses(&self) -> Result<Vec<BoxServerStatus>, ServerStateError> {
        self.server_status().await.map(|status| vec![status])
    }

    async fn servers_register(&self, _clustering_id: u64, _clustering_address: String) -> Result<(), ServerStateError> {
        // todo: error message
        Err(ServerStateError::Unimplemented { description: "This functionality is not available".to_string() })
    }

    async fn servers_deregister(&self, _clustering_id: u64) -> Result<(), ServerStateError> {
        // todo: error message
        Err(ServerStateError::Unimplemented { description: "This functionality is not available".to_string() })
    }

    async fn databases_all(&self) -> Result<Vec<String>, ServerStateError> {
        Ok(self.database_manager.database_names())
    }

    async fn databases_get(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        self.database_manager.database(name)
    }

    async fn databases_contains(&self, name: &str) -> bool {
        self.database_manager.database(name).is_some()
    }

    async fn databases_create(&self, name: &str) -> Result<(), ServerStateError> {
        self.database_manager
            .put_database(name)
            .map_err(|err| ServerStateError::DatabaseCannotBeCreated { typedb_source: err })
    }

    async fn database_schema(&self, name: String) -> Result<String, ServerStateError> {
        match self.database_manager.database(&name) {
            Some(db) => Self::get_database_schema(db),
            None => Err(ServerStateError::DatabaseNotFound { name }),
        }
    }

    async fn database_type_schema(&self, name: String) -> Result<String, ServerStateError> {
        match self.database_manager.database(&name) {
            None => Err(ServerStateError::DatabaseNotFound { name: name.clone() }),
            Some(database) => match Self::get_database_type_schema(database) {
                Ok(type_schema) => Ok(type_schema),
                Err(err) => Err(err),
            },
        }
    }

    async fn database_schema_commit(
        &self,
        name: &str,
        commit_record: CommitRecord,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ServerStateError> {
        let Some(database) = self.databases_get(name).await else {
            return Err(ServerStateError::DatabaseNotFound { name: name.to_string() });
        };
        database
            .schema_commit_with_commit_record(commit_record, commit_profile)
            .map_err(|error| ServerStateError::DatabaseSchemaCommitFailed { typedb_source: error })
    }

    async fn database_data_commit(
        &self,
        name: &str,
        commit_record: CommitRecord,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ServerStateError> {
        let Some(database) = self.databases_get(name).await else {
            return Err(ServerStateError::DatabaseNotFound { name: name.to_string() });
        };
        database
            .data_commit_with_commit_record(commit_record, commit_profile)
            .map_err(|typedb_source| ServerStateError::DatabaseDataCommitFailed { typedb_source })
    }

    async fn database_delete(&self, name: &str) -> Result<(), ServerStateError> {
        self.database_manager
            .delete_database(name)
            .map_err(|err| ServerStateError::DatabaseCannotBeDeleted { typedb_source: err })
    }

    async fn users_get(&self, name: &str, accessor: Accessor) -> Result<User, ServerStateError> {
        if !PermissionManager::exec_user_get_permitted(accessor.0.as_str(), name) {
            return Err(ServerStateError::OperationNotPermitted {});
        }

        match self.get_user_manager() {
            Ok(user_manager) => match user_manager.get(name) {
                Ok(get) => match get {
                    Some((user, _)) => Ok(user),
                    None => Err(ServerStateError::UserNotFound {}),
                },
                Err(err) => Err(ServerStateError::UserCannotBeRetrieved { typedb_source: err }),
            },
            Err(err) => Err(err),
        }
    }

    async fn users_all(&self, accessor: Accessor) -> Result<Vec<User>, ServerStateError> {
        if !PermissionManager::exec_user_all_permitted(accessor.0.as_str()) {
            return Err(ServerStateError::OperationNotPermitted {});
        }

        match self.get_user_manager() {
            Ok(user_manager) => Ok(user_manager.all()),
            Err(err) => Err(err),
        }
    }

    async fn users_contains(&self, name: &str) -> Result<bool, ServerStateError> {
        match self.get_user_manager() {
            Ok(user_manager) => match user_manager.contains(name) {
                Ok(bool) => Ok(bool),
                Err(err) => Err(ServerStateError::UserCannotBeRetrieved { typedb_source: err }),
            },
            Err(err) => Err(err),
        }
    }

    async fn users_create(
        &self,
        user: &User,
        credential: &Credential,
        accessor: Accessor,
    ) -> Result<(), ServerStateError> {
        if !PermissionManager::exec_user_create_permitted(accessor.0.as_str()) {
            return Err(ServerStateError::OperationNotPermitted {});
        }
        match self.get_user_manager() {
            Ok(user_manager) => user_manager
                .create(user, credential)
                .map(|_user| ())
                .map_err(|err| ServerStateError::UserCannotBeCreated { typedb_source: err }),
            Err(err) => Err(err),
        }
    }

    async fn users_update(
        &self,
        name: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
        accessor: Accessor,
    ) -> Result<(), ServerStateError> {
        if !PermissionManager::exec_user_update_permitted(accessor.0.as_str(), name) {
            return Err(ServerStateError::OperationNotPermitted {});
        }
        match self.get_user_manager() {
            Ok(user_manager) => {
                user_manager
                    .update(name, &user_update, &credential_update)
                    .map_err(|err| ServerStateError::UserCannotBeUpdated { typedb_source: err })?;
                self.token_manager.invalidate_user(name).await;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn users_delete(&self, name: &str, accessor: Accessor) -> Result<(), ServerStateError> {
        if !PermissionManager::exec_user_delete_allowed(accessor.0.as_str(), name) {
            return Err(ServerStateError::OperationNotPermitted {});
        }

        match self.get_user_manager() {
            Ok(user_manager) => {
                user_manager
                    .delete(name)
                    .map_err(|err| ServerStateError::UserCannotBeDeleted { typedb_source: err })?;
                self.token_manager.invalidate_user(name).await;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn user_verify_password(&self, username: &str, password: &str) -> Result<(), ServerStateError> {
        match self.get_credential_verifier() {
            Ok(credential_verifier) => match credential_verifier.verify_password(username, password) {
                Ok(()) => Ok(()),
                Err(err) => Err(ServerStateError::AuthenticationError { typedb_source: err }),
            },
            Err(err) => Err(err),
        }
    }

    async fn token_create(&self, username: String, password: String) -> Result<String, ServerStateError> {
        self.user_verify_password(&username, &password).await?;
        Ok(self.token_manager.new_token(username).await)
    }

    async fn token_get_owner(&self, token: &str) -> Option<String> {
        self.token_manager.get_valid_token_owner(token).await
    }

    async fn database_manager(&self) -> Arc<DatabaseManager> {
        self.database_manager.clone()
    }

    async fn diagnostics_manager(&self) -> Arc<DiagnosticsManager> {
        self.diagnostics_manager.clone()
    }

    fn shutdown_receiver(&self) -> Receiver<()> {
        self.shutdown_receiver.clone()
    }
}
