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
    transaction::{TransactionError, TransactionRead, TransactionSchema},
    Database, DatabaseDeleteError,
};
use diagnostics::{diagnostics_manager::DiagnosticsManager, Diagnostics};
use error::typedb_error;
use futures::{StreamExt, TryFutureExt};
use ir::pipeline::FunctionReadError;
use itertools::Itertools;
use options::TransactionOptions;
use resource::{constants::server::DATABASE_METRICS_UPDATE_INTERVAL, distribution_info::DistributionInfo};
use storage::durability_client::{DurabilityClient, WALClient};
use system::{
    concepts::{Credential, User},
    initialise_system_database,
};
use tokio::sync::watch::Receiver;
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

pub type BoxServerState = Box<dyn ServerState + Send + Sync>;

#[async_trait]
pub trait ServerState: Debug {
    async fn distribution_info(&self) -> DistributionInfo;

    async fn servers_statuses(&self) -> Vec<Box<dyn ServerStatus>>;

    async fn servers_register(&self, clustering_id: u64, clustering_address: String) -> Result<(), ServerStateError>;

    async fn servers_deregister(&self, clustering_id: u64) -> Result<(), ServerStateError>;

    async fn databases_all(&self) -> Result<Vec<String>, ServerStateError>;

    async fn databases_get(&self, name: &str) -> Option<Arc<Database<WALClient>>>;

    async fn databases_contains(&self, name: &str) -> bool;

    async fn databases_create(&self, name: &str) -> Result<(), ServerStateError>;

    async fn database_schema(&self, name: String) -> Result<String, ServerStateError>;

    async fn database_type_schema(&self, name: String) -> Result<String, ServerStateError>;

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

    async fn shutdown_receiver(&self) -> Receiver<()>;
}

typedb_error! {
    pub ServerStateError(component = "Server state", prefix = "SRV") {
        NotInitialised(16, "Not yet initialised"),
        Unimplemented(1, "Not implemented: {description}", description: String),
        OperationFailedDueToReplicaUnavailability(12, "Unable to execute as one or more servers could not respond in time"),
        OperationFailedNonPrimaryReplica(13, "Unable to execute as this server is not the primary replica"),
        OperationNotPermitted(2, "The user is not permitted to execute the operation"),
        DatabaseNotFound(3, "Database '{name}' not found.", name: String),
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
    server_address: SocketAddr,
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
        server_address: SocketAddr,
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

        Ok(Self {
            distribution_info,
            server_address,
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
}

#[async_trait]
impl ServerState for LocalServerState {
    async fn distribution_info(&self) -> DistributionInfo {
        self.distribution_info
    }

    async fn servers_statuses(&self) -> Vec<Box<dyn ServerStatus>> {
        vec![Box::new(LocalServerStatus { address: self.server_address })]
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

    async fn shutdown_receiver(&self) -> Receiver<()> {
        self.shutdown_receiver.clone()
    }
}
