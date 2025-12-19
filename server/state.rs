/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use concurrency::{IntervalRunner, IntervalTaskParameters, TokioTaskSpawner};
use database::{
    database_manager::DatabaseManager,
    transaction::{TransactionId, TransactionRead},
    Database,
};
use diagnostics::{diagnostics_manager::DiagnosticsManager, Diagnostics};
use durability::DurabilitySequenceNumber;
use itertools::Itertools;
use options::TransactionOptions;
use resource::{
    constants::{common::SECONDS_IN_MINUTE, server::DATABASE_METRICS_UPDATE_INTERVAL},
    distribution_info::DistributionInfo,
    profile::CommitProfile,
};
use storage::{
    durability_client::{DurabilityClient, WALClient},
    record::CommitRecord,
    snapshot::{snapshot_id::SnapshotId, CommittableSnapshot},
};
use system::concepts::{Credential, User};
use tokio::{
    net::lookup_host,
    sync::{mpsc::Sender, watch::Receiver, RwLock},
    task::JoinHandle,
};
use user::{
    errors::{UserCreateError, UserDeleteError, UserUpdateError},
    permission_manager::PermissionManager,
    user_manager::UserManager,
};

use crate::{
    authentication::{credential_verifier::CredentialVerifier, token_manager::TokenManager, Accessor},
    error::{arc_server_state_err, ArcServerStateError, LocalServerStateError, ServerOpenError},
    parameters::config::{Config, DiagnosticsConfig},
    service::{
        export_service::{get_transaction_schema, get_transaction_type_schema},
        grpc::migration::import_service::DatabaseImportService,
        TransactionType,
    },
    status::{LocalServerStatus, ServerStatus},
    system_init::SYSTEM_DB,
};

pub type DynServerState = dyn ServerState + Send + Sync;
pub type ArcServerState = Arc<DynServerState>;

pub type BoxServerStatus = Box<dyn ServerStatus + Send + Sync>;

#[async_trait]
pub trait ServerState: Debug {
    async fn distribution_info(&self) -> DistributionInfo;

    // TODO: grpc_address and http_address don't really suit "ServerState"
    async fn grpc_serving_address(&self) -> SocketAddr;

    async fn grpc_connection_address(&self) -> String;

    // TODO: HTTP could probably expose two addresses, too
    async fn http_address(&self) -> Option<SocketAddr>;

    // TODO: Name server_status -> servers_get and servers_statuses -> servers_all like in GRPC?
    async fn server_status(&self) -> Result<BoxServerStatus, ArcServerStateError>;

    async fn servers_statuses(&self) -> Result<Vec<BoxServerStatus>, ArcServerStateError>;

    async fn servers_register(&self, clustering_id: u64, clustering_address: String)
        -> Result<(), ArcServerStateError>;

    async fn servers_deregister(&self, clustering_id: u64) -> Result<(), ArcServerStateError>;

    async fn databases_all(&self) -> Result<Vec<String>, ArcServerStateError>;

    async fn databases_contains(&self, name: &str) -> Result<bool, ArcServerStateError>;

    async fn databases_get(&self, name: &str) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError>;

    async fn databases_get_unrestricted(
        &self,
        name: &str,
    ) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError>;

    async fn databases_get_for_transaction(
        &self,
        name: &str,
        transaction_type: TransactionType,
    ) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError>;

    async fn databases_create(&self, name: &str) -> Result<(), ArcServerStateError>;

    async fn databases_create_unrestricted(&self, name: &str) -> Result<(), ArcServerStateError>;

    async fn databases_import(&self, service: DatabaseImportService) -> Result<JoinHandle<()>, ArcServerStateError>;

    async fn database_schema(&self, name: &str) -> Result<String, ArcServerStateError>;

    async fn database_type_schema(&self, name: &str) -> Result<String, ArcServerStateError>;

    async fn database_schema_commit(
        &self,
        name: &str,
        commit_record: CommitRecord,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ArcServerStateError>;

    async fn database_data_commit(
        &self,
        name: &str,
        commit_record: CommitRecord,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ArcServerStateError>;

    // TODO: Instead of `open_sequence_number` + `snapshot_id`, it could be better to use `TransactionId`
    // if we don't want to expose storage entities like `CommitRecord`
    async fn database_commit_record_exists(
        &self,
        name: &str,
        open_sequence_number: DurabilitySequenceNumber,
        snapshot_id: SnapshotId,
    ) -> Result<bool, ArcServerStateError>;

    async fn database_delete(&self, name: &str) -> Result<(), ArcServerStateError>;

    async fn users_all(&self, accessor: Accessor) -> Result<Vec<User>, ArcServerStateError>;

    async fn users_contains(&self, accessor: Accessor, name: &str) -> Result<bool, ArcServerStateError>;

    async fn users_get(&self, accessor: Accessor, name: &str) -> Result<User, ArcServerStateError>;

    async fn users_create(
        &self,
        accessor: Accessor,
        user: User,
        credential: Credential,
    ) -> Result<(), ArcServerStateError>;

    async fn users_update(
        &self,
        accessor: Accessor,
        username: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
    ) -> Result<(), ArcServerStateError>;

    async fn users_delete(&self, accessor: Accessor, username: &str) -> Result<(), ArcServerStateError>;

    async fn user_verify_password(&self, username: &str, password: &str) -> Result<(), ArcServerStateError>;

    async fn token_create(&self, username: String, password: String) -> Result<String, ArcServerStateError>;

    async fn token_get_owner(&self, token: &str) -> Option<String>;

    async fn transactions_add(
        &self,
        transaction_id: TransactionId,
        transaction_type: TransactionType,
        close_sender: Sender<()>,
    ) -> Result<(), ArcServerStateError>;

    async fn transactions_close_types(&self, types: HashSet<TransactionType>) -> Result<(), ArcServerStateError>;

    async fn database_manager(&self) -> Arc<DatabaseManager>;

    async fn user_manager(&self) -> Option<Arc<UserManager>>;

    async fn diagnostics_manager(&self) -> Arc<DiagnosticsManager>;

    async fn shutdown_receiver(&self) -> Receiver<()>;

    async fn background_task_spawner(&self) -> TokioTaskSpawner;
}

#[derive(Debug)]
pub struct LocalServerState {
    distribution_info: DistributionInfo,
    grpc_serving_address: SocketAddr,
    grpc_connection_address: String, // reference info (can be an alias), do not resolve!
    http_address: Option<SocketAddr>,
    server_status: LocalServerStatus,
    database_manager: Arc<DatabaseManager>,
    user_manager: Option<Arc<UserManager>>,
    credential_verifier: Option<Arc<CredentialVerifier>>,
    token_manager: Arc<TokenManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
    shutdown_receiver: Receiver<()>,
    background_task_spawner: TokioTaskSpawner,
    _database_diagnostics_updater: IntervalRunner,
}

impl LocalServerState {
    const TRANSACTION_CHECK_INTERVAL: Duration = Duration::from_secs(5 * SECONDS_IN_MINUTE);

    pub async fn new(
        distribution_info: DistributionInfo,
        config: Config,
        server_id: String,
        deployment_id: Option<String>,
        shutdown_receiver: Receiver<()>,
        background_task_spawner: TokioTaskSpawner,
    ) -> Result<Self, ServerOpenError> {
        let database_manager = DatabaseManager::new(&config.storage.data_directory)
            .map_err(|typedb_source| ServerOpenError::DatabaseOpen { typedb_source })?;
        let token_manager = Arc::new(
            TokenManager::new(config.server.authentication.token_expiration, background_task_spawner.clone())
                .map_err(|typedb_source| ServerOpenError::TokenConfiguration { typedb_source })?,
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
                background_task_spawner.clone(),
            )
            .await,
        );

        let grpc_serving_address = Self::resolve_address(&config.server.address).await;
        let (grpc_connection_address, grpc_connection_address_str) = match config.server.connection_address {
            Some(connection_address) => (Self::resolve_address(&connection_address).await, connection_address),
            None => (grpc_serving_address, grpc_serving_address.to_string()),
        };
        let reserved_addresses = HashSet::from([grpc_serving_address, grpc_connection_address]);
        let reserved_addresses = reserved_addresses.into_iter();
        let http_address = if config.server.http.enabled {
            Some(Self::validate_and_resolve_http_address(&config.server.http.address, reserved_addresses).await?)
        } else {
            None
        };

        let transactions = Arc::new(RwLock::new(HashMap::new()));
        let controlled_transactions = transactions.clone();
        background_task_spawner.spawn_interval(
            move || {
                let transactions = controlled_transactions.clone();
                async move {
                    Self::cleanup_closed_transactions(transactions).await;
                }
            },
            IntervalTaskParameters::new_with_delay(
                Self::TRANSACTION_CHECK_INTERVAL,
                Self::TRANSACTION_CHECK_INTERVAL,
                false,
            ),
        );

        Ok(Self {
            distribution_info,
            server_status: LocalServerStatus::from_addresses(
                grpc_serving_address,
                grpc_connection_address_str.clone(),
                http_address,
            ),
            grpc_serving_address,
            grpc_connection_address: grpc_connection_address_str,
            http_address,
            database_manager: database_manager.clone(),
            user_manager: None,
            credential_verifier: None,
            token_manager,
            diagnostics_manager: diagnostics_manager.clone(),
            transactions: Arc::new(RwLock::new(HashMap::new())),
            shutdown_receiver,
            background_task_spawner,
            _database_diagnostics_updater: IntervalRunner::new(
                move || Self::synchronize_database_metrics(diagnostics_manager.clone(), database_manager.clone()),
                DATABASE_METRICS_UPDATE_INTERVAL,
            ),
        })
    }

    pub async fn initialise(&self) -> Result<(), ArcServerStateError> {
        let system_database =
            if let Some(system_database) = self.database_manager().await.database_unrestricted(SYSTEM_DB) {
                system_database
            } else {
                crate::system_init::initialise_system_database(self).await?
            };

        let user_manager = Arc::new(UserManager::new(system_database));
        crate::system_init::initialise_default_user(&user_manager, self).await?;

        Ok(())
    }

    pub async fn is_initialised(&self) -> bool {
        self.database_manager().await.database_unrestricted(SYSTEM_DB).is_some()
    }

    pub async fn load(&mut self) {
        let system_database = self.database_manager().await.database_unrestricted(SYSTEM_DB).unwrap();
        let user_manager = Arc::new(UserManager::new(system_database));
        let credential_verifier = Arc::new(CredentialVerifier::new(user_manager.clone()));
        self.user_manager = Some(user_manager);
        self.credential_verifier = Some(credential_verifier);
    }

    pub async fn initialise_and_load(&mut self) -> Result<(), ArcServerStateError> {
        if !self.is_initialised().await {
            self.initialise().await?;
        }
        self.load().await;
        Ok(())
    }

    async fn initialise_diagnostics(
        deployment_id: String,
        server_id: String,
        distribution_info: DistributionInfo,
        config: &DiagnosticsConfig,
        storage_directory: PathBuf,
        is_development_mode: bool,
        background_tasks: TokioTaskSpawner,
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
            background_tasks,
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

    pub fn get_database_schema<D: DurabilityClient>(
        database: Arc<Database<D>>,
    ) -> Result<String, LocalServerStateError> {
        let transaction = TransactionRead::open(database, TransactionOptions::default())
            .map_err(|typedb_source| LocalServerStateError::FailedToOpenPrerequisiteTransaction { typedb_source })?;
        let schema = get_transaction_schema(&transaction)
            .map_err(|typedb_source| LocalServerStateError::DatabaseExport { typedb_source })?;
        Ok(schema)
    }

    pub fn get_functions_syntax<D: DurabilityClient>(
        transaction: &TransactionRead<D>,
    ) -> Result<String, LocalServerStateError> {
        transaction
            .function_manager
            .get_functions_syntax(transaction.snapshot())
            .map_err(|typedb_source| LocalServerStateError::FunctionReadError { typedb_source })
    }

    pub(crate) fn get_database_type_schema<D: DurabilityClient>(
        database: Arc<Database<D>>,
    ) -> Result<String, LocalServerStateError> {
        let transaction = TransactionRead::open(database, TransactionOptions::default())
            .map_err(|typedb_source| LocalServerStateError::FailedToOpenPrerequisiteTransaction { typedb_source })?;
        let type_schema = get_transaction_type_schema(&transaction)
            .map_err(|typedb_source| LocalServerStateError::DatabaseExport { typedb_source })?;
        Ok(type_schema)
    }

    pub fn get_types_syntax<D: DurabilityClient>(
        transaction: &TransactionRead<D>,
    ) -> Result<String, LocalServerStateError> {
        transaction
            .type_manager
            .get_types_syntax(transaction.snapshot())
            .map_err(|typedb_source| LocalServerStateError::ConceptReadError { typedb_source })
    }

    pub fn local_server_status(&self) -> LocalServerStatus {
        self.server_status.clone()
    }

    fn get_user_manager(&self) -> Result<Arc<UserManager>, LocalServerStateError> {
        match self.user_manager.clone() {
            Some(user_manager) => Ok(user_manager),
            None => Err(LocalServerStateError::NotInitialised {}),
        }
    }

    fn get_credential_verifier(&self) -> Result<Arc<CredentialVerifier>, LocalServerStateError> {
        match self.credential_verifier.clone() {
            Some(credential_verifier) => Ok(credential_verifier),
            None => Err(LocalServerStateError::NotInitialised {}),
        }
    }

    async fn validate_and_resolve_http_address(
        http_address: &str,
        mut reserved_addresses: impl Iterator<Item = SocketAddr>,
    ) -> Result<SocketAddr, ServerOpenError> {
        let http_address = Self::resolve_address(http_address).await;
        if reserved_addresses.contains(&http_address) {
            return Err(ServerOpenError::HttpConflictingAddress { address: http_address });
        }
        Ok(http_address)
    }

    pub async fn resolve_address(address: &str) -> SocketAddr {
        lookup_host(address)
            .await
            .expect(&format!("Invalid address '{}'", address))
            .next()
            .expect(&format!("Unable to map address '{}' to any IP address", address))
    }

    async fn cleanup_closed_transactions(transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>) {
        let mut transactions = transactions.write().await;
        transactions.retain(|_, info| !info.close_sender.is_closed());
    }
}

#[async_trait]
impl ServerState for LocalServerState {
    async fn distribution_info(&self) -> DistributionInfo {
        self.distribution_info
    }

    async fn grpc_serving_address(&self) -> SocketAddr {
        self.grpc_serving_address
    }

    async fn grpc_connection_address(&self) -> String {
        self.grpc_connection_address.clone()
    }

    async fn http_address(&self) -> Option<SocketAddr> {
        self.http_address
    }

    async fn server_status(&self) -> Result<BoxServerStatus, ArcServerStateError> {
        Ok(Box::new(self.server_status.clone()))
    }

    async fn servers_statuses(&self) -> Result<Vec<BoxServerStatus>, ArcServerStateError> {
        self.server_status().await.map(|status| vec![status])
    }

    async fn servers_register(
        &self,
        _clustering_id: u64,
        _clustering_address: String,
    ) -> Result<(), ArcServerStateError> {
        Err(Arc::new(LocalServerStateError::NotSupportedByDistribution {
            description: "exclusive to TypeDB Cloud and TypeDB Enterprise".to_string(),
        }))
    }

    async fn servers_deregister(&self, _clustering_id: u64) -> Result<(), ArcServerStateError> {
        Err(Arc::new(LocalServerStateError::NotSupportedByDistribution {
            description: "exclusive to TypeDB Cloud and TypeDB Enterprise".to_string(),
        }))
    }

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

    async fn databases_get_for_transaction(
        &self,
        name: &str,
        _transaction_type: TransactionType,
    ) -> Result<Option<Arc<Database<WALClient>>>, ArcServerStateError> {
        self.databases_get(name).await
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
            Some(db) => Self::get_database_schema(db),
            None => Err(LocalServerStateError::DatabaseNotFound { name: name.to_string() }),
        }
        .map_err(|err| arc_server_state_err(err))
    }

    async fn database_type_schema(&self, name: &str) -> Result<String, ArcServerStateError> {
        match self.database_manager.database(name) {
            None => Err(Arc::new(LocalServerStateError::DatabaseNotFound { name: name.to_string() })),
            Some(database) => match Self::get_database_type_schema(database) {
                Ok(type_schema) => Ok(type_schema),
                Err(err) => Err(Arc::new(err)),
            },
        }
    }

    // TODO: It's bad that the system database is allowed here. Come up with a better design
    async fn database_schema_commit(
        &self,
        name: &str,
        commit_record: CommitRecord,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ArcServerStateError> {
        let Some(database) = self.databases_get_unrestricted(name).await? else {
            return Err(Arc::new(LocalServerStateError::DatabaseNotFound { name: name.to_string() }));
        };
        database.schema_commit_with_commit_record(commit_record, commit_profile).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabaseSchemaCommitFailed { typedb_source })
        })
    }

    async fn database_data_commit(
        &self,
        name: &str,
        commit_record: CommitRecord,
        commit_profile: &mut CommitProfile,
    ) -> Result<(), ArcServerStateError> {
        let Some(database) = self.databases_get_unrestricted(name).await? else {
            return Err(Arc::new(LocalServerStateError::DatabaseNotFound { name: name.to_string() }));
        };
        database.data_commit_with_commit_record(commit_record, commit_profile).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabaseDataCommitFailed { typedb_source })
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

    async fn users_all(&self, accessor: Accessor) -> Result<Vec<User>, ArcServerStateError> {
        if !PermissionManager::exec_user_all_permitted(accessor.as_str()) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        match self.get_user_manager() {
            Ok(user_manager) => Ok(user_manager.all()),
            Err(err) => Err(Arc::new(err)),
        }
    }

    async fn users_contains(&self, accessor: Accessor, name: &str) -> Result<bool, ArcServerStateError> {
        if !PermissionManager::exec_user_get_permitted(accessor.as_str(), name) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        match self.get_user_manager() {
            Ok(user_manager) => match user_manager.contains(name) {
                Ok(bool) => Ok(bool),
                Err(typedb_source) => Err(Arc::new(LocalServerStateError::UserCannotBeRetrieved { typedb_source })),
            },
            Err(err) => Err(Arc::new(err)),
        }
    }

    async fn users_get(&self, accessor: Accessor, name: &str) -> Result<User, ArcServerStateError> {
        if !PermissionManager::exec_user_get_permitted(accessor.as_str(), name) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        match self.get_user_manager() {
            Ok(user_manager) => match user_manager.get(name) {
                Ok(get) => match get {
                    Some((user, _)) => Ok(user),
                    None => Err(Arc::new(LocalServerStateError::UserNotFound {})),
                },
                Err(typedb_source) => Err(Arc::new(LocalServerStateError::UserCannotBeRetrieved { typedb_source })),
            },
            Err(err) => Err(Arc::new(err)),
        }
    }

    async fn users_create(
        &self,
        accessor: Accessor,
        user: User,
        credential: Credential,
    ) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_create_permitted(accessor.as_str()) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = self.user_manager().await.ok_or(LocalServerStateError::NotInitialised {})?;

        let (mut transaction_profile, commit_intent) = user_manager
            .create(&user, &credential)
            .map_err(|(_, typedb_source)| LocalServerStateError::UserCannotBeCreated { typedb_source })?;

        let commit_profile = transaction_profile.commit_profile();
        let commit_record = commit_intent.write_snapshot.finalise(commit_profile).map_err(|_error| {
            LocalServerStateError::UserCannotBeCreated { typedb_source: UserCreateError::Unexpected {} }
        })?;

        if let Some(commit_record) = commit_record {
            let commit_profile = transaction_profile.commit_profile();
            self.database_data_commit(SYSTEM_DB, commit_record, commit_profile).await?;
        }

        Ok(())
    }

    async fn users_update(
        &self,
        accessor: Accessor,
        username: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
    ) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_update_permitted(accessor.as_str(), username) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = self.user_manager().await.ok_or(LocalServerStateError::NotInitialised {})?;

        let (mut transaction_profile, commit_intent_result) =
            user_manager.update(username, &user_update, &credential_update);

        let commit_intent = commit_intent_result
            .map_err(|typedb_source| LocalServerStateError::UserCannotBeUpdated { typedb_source })?;

        let commit_profile = transaction_profile.commit_profile();
        let commit_record = commit_intent.write_snapshot.finalise(commit_profile).map_err(|_error| {
            LocalServerStateError::UserCannotBeUpdated { typedb_source: UserUpdateError::Unexpected {} }
        })?;

        if let Some(commit_record) = commit_record {
            let commit_profile = transaction_profile.commit_profile();
            self.database_data_commit(SYSTEM_DB, commit_record, commit_profile).await?;
            self.token_manager.invalidate_user(username).await;
            // TODO #7430: Store users as owners of transactions in TransactionInfo and close transactions
            // when the user is invalidated!
        }

        Ok(())
    }

    async fn users_delete(&self, accessor: Accessor, username: &str) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_delete_allowed(accessor.as_str(), username) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = self.user_manager().await.ok_or(LocalServerStateError::NotInitialised {})?;

        let (mut transaction_profile, commit_intent) = user_manager
            .delete(username)
            .map_err(|(_, typedb_source)| LocalServerStateError::UserCannotBeDeleted { typedb_source })?;

        let commit_profile = transaction_profile.commit_profile();
        let commit_record = commit_intent.write_snapshot.finalise(commit_profile).map_err(|_error| {
            LocalServerStateError::UserCannotBeDeleted { typedb_source: UserDeleteError::Unexpected {} }
        })?;

        if let Some(commit_record) = commit_record {
            let commit_profile = transaction_profile.commit_profile();
            self.database_data_commit(SYSTEM_DB, commit_record, commit_profile).await?;
            self.token_manager.invalidate_user(username).await;
            // TODO #7430: Store users as owners of transactions in TransactionInfo and close transactions
            // when the user is invalidated!
        }

        Ok(())
    }

    async fn user_verify_password(&self, username: &str, password: &str) -> Result<(), ArcServerStateError> {
        match self.get_credential_verifier() {
            Ok(credential_verifier) => match credential_verifier.verify_password(username, password) {
                Ok(()) => Ok(()),
                Err(typedb_source) => Err(Arc::new(LocalServerStateError::AuthenticationError { typedb_source })),
            },
            Err(err) => Err(Arc::new(err)),
        }
    }

    async fn token_create(&self, username: String, password: String) -> Result<String, ArcServerStateError> {
        self.user_verify_password(&username, &password).await?;
        Ok(self.token_manager.new_token(username).await)
    }

    async fn token_get_owner(&self, token: &str) -> Option<String> {
        self.token_manager.get_valid_token_owner(token).await
    }

    async fn transactions_add(
        &self,
        transaction_id: TransactionId,
        transaction_type: TransactionType,
        close_sender: Sender<()>,
    ) -> Result<(), ArcServerStateError> {
        let mut transactions_lock = self.transactions.write().await;
        transactions_lock.insert(transaction_id, TransactionInfo { transaction_type, close_sender });
        Ok(())
    }

    async fn transactions_close_types(&self, types: HashSet<TransactionType>) -> Result<(), ArcServerStateError> {
        let mut txs = self.transactions.write().await;

        let to_close: Vec<_> =
            txs.iter().filter(|(_, info)| types.contains(&info.transaction_type)).map(|(id, _)| *id).collect();

        for id in to_close {
            if let Some(info) = txs.remove(&id) {
                let _ = info.close_sender.send(());
            }
        }
        Ok(())
    }

    async fn database_manager(&self) -> Arc<DatabaseManager> {
        self.database_manager.clone()
    }

    async fn user_manager(&self) -> Option<Arc<UserManager>> {
        self.user_manager.clone()
    }

    async fn diagnostics_manager(&self) -> Arc<DiagnosticsManager> {
        self.diagnostics_manager.clone()
    }

    async fn shutdown_receiver(&self) -> Receiver<()> {
        self.shutdown_receiver.clone()
    }

    async fn background_task_spawner(&self) -> TokioTaskSpawner {
        self.background_task_spawner.clone()
    }
}

#[derive(Debug)]
struct TransactionInfo {
    transaction_type: TransactionType,
    close_sender: Sender<()>,
}
