/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod database_operator;
pub mod server_operator;
pub mod transaction_operator;
pub mod user_operator;

use std::{collections::HashSet, net::SocketAddr, path::PathBuf, sync::Arc};

use concurrency::{IntervalRunner, TokioTaskSpawner};
use database::database_manager::DatabaseManager;
use diagnostics::{diagnostics_manager::DiagnosticsManager, Diagnostics};
use itertools::Itertools;
use resource::{constants::server::DATABASE_METRICS_UPDATE_INTERVAL, distribution_info::DistributionInfo};
use tokio::{net::lookup_host, sync::watch::Receiver};

pub use self::{
    database_operator::{
        get_database_schema, get_functions_syntax, get_types_syntax, DatabaseOperator, LocalDatabaseOperator,
    },
    server_operator::{LocalServerOperator, ServerOperator},
    transaction_operator::{LocalTransactionOperator, TransactionOperator},
    user_operator::{LocalUserOperator, UserOperator},
};
use crate::{
    authentication::token_manager::TokenManager,
    error::{ArcServerStateError, ServerOpenError},
    parameters::config::{Config, DiagnosticsConfig},
    status::{LocalServerStatus, ServerStatus},
};

pub type BoxServerStatus = Box<dyn ServerStatus + Send + Sync>;

#[derive(Debug)]
pub struct ServerState {
    distribution_info: DistributionInfo,
    grpc_serving_address: SocketAddr,
    grpc_connection_address: String, // reference info (can be an alias), do not resolve!
    http_address: Option<SocketAddr>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    shutdown_receiver: Receiver<()>,
    background_task_spawner: TokioTaskSpawner,
    _database_diagnostics_updater: IntervalRunner,

    server_operator: Arc<dyn ServerOperator>,
    database_operator: Arc<dyn DatabaseOperator>,
    transaction_operator: Arc<dyn TransactionOperator>,
    user_operator: Arc<dyn UserOperator>,
}

impl ServerState {
    pub async fn new(
        distribution_info: DistributionInfo,
        config: Config,
        server_id: String,
        deployment_id: Option<String>,
        shutdown_receiver: Receiver<()>,
        background_task_spawner: TokioTaskSpawner,
    ) -> Result<ServerStateBuilder, ServerOpenError> {
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
        let database_diagnostics_updater = IntervalRunner::new(
            {
                let diagnostics_manager = diagnostics_manager.clone();
                let database_manager = database_manager.clone();
                move || ServerState::synchronize_database_metrics(diagnostics_manager.clone(), database_manager.clone())
            },
            DATABASE_METRICS_UPDATE_INTERVAL,
        );

        let grpc_serving_address = Self::resolve_address(&config.server.address).await?;
        let (grpc_connection_address, grpc_connection_address_str) = match config.server.connection_address {
            Some(connection_address) => (Self::resolve_address(&connection_address).await?, connection_address),
            None => (grpc_serving_address, grpc_serving_address.to_string()),
        };
        let reserved_addresses = HashSet::from([grpc_serving_address, grpc_connection_address]);
        let reserved_addresses = reserved_addresses.into_iter();
        let http_address = if config.server.http.enabled {
            Some(Self::validate_and_resolve_http_address(&config.server.http.address, reserved_addresses).await?)
        } else {
            None
        };

        let server_status =
            LocalServerStatus::from_addresses(grpc_serving_address, grpc_connection_address_str.clone(), http_address);

        Ok(ServerStateBuilder {
            distribution_info,
            grpc_serving_address,
            grpc_connection_address: grpc_connection_address_str,
            http_address,
            server_status,
            database_manager: database_manager,
            token_manager,
            diagnostics_manager,
            database_diagnostics_updater,
            shutdown_receiver,
            background_task_spawner,
            server_operator_override: None,
            database_operator_override: None,
            transaction_operator_override: None,
            user_operator_override: None,
        })
    }

    pub fn distribution_info(&self) -> DistributionInfo {
        self.distribution_info
    }

    pub fn grpc_serving_address(&self) -> SocketAddr {
        self.grpc_serving_address
    }

    pub fn grpc_connection_address(&self) -> String {
        self.grpc_connection_address.clone()
    }

    pub fn http_address(&self) -> Option<SocketAddr> {
        self.http_address
    }

    pub fn servers(&self) -> &dyn ServerOperator {
        &*self.server_operator
    }

    pub fn databases(&self) -> &dyn DatabaseOperator {
        &*self.database_operator
    }

    pub fn transactions(&self) -> &dyn TransactionOperator {
        &*self.transaction_operator
    }

    pub fn users(&self) -> &dyn UserOperator {
        &*self.user_operator
    }

    pub fn diagnostics_manager(&self) -> Arc<DiagnosticsManager> {
        self.diagnostics_manager.clone()
    }

    pub fn shutdown_receiver(&self) -> Receiver<()> {
        self.shutdown_receiver.clone()
    }

    pub fn background_task_spawner(&self) -> TokioTaskSpawner {
        self.background_task_spawner.clone()
    }

    pub fn is_initialised(&self) -> bool {
        // Other operators don't require initialization
        self.user_operator.is_initialised()
    }

    pub async fn initialise(&self) -> Result<(), ArcServerStateError> {
        if self.is_initialised() {
            return Ok(());
        }

        // Initialize self for user_operator
        crate::system_init::initialise_system_database(self).await?;
        crate::system_init::initialise_system_database_schema(self).await?;
        crate::system_init::initialise_default_user(self).await?;

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

    pub async fn resolve_address(address: &str) -> Result<SocketAddr, ServerOpenError> {
        lookup_host(address)
            .await
            .map_err(|source| ServerOpenError::AddressResolutionFailed {
                address: address.to_string(),
                source: Arc::new(source),
            })?
            .next()
            .ok_or_else(|| ServerOpenError::AddressResolutionEmpty { address: address.to_string() })
    }

    async fn validate_and_resolve_http_address(
        http_address: &str,
        mut reserved_addresses: impl Iterator<Item = SocketAddr>,
    ) -> Result<SocketAddr, ServerOpenError> {
        let http_address = Self::resolve_address(http_address).await?;
        if reserved_addresses.contains(&http_address) {
            return Err(ServerOpenError::HttpConflictingAddress { address: http_address });
        }
        Ok(http_address)
    }
}

pub struct ServerStateBuilder {
    distribution_info: DistributionInfo,
    grpc_serving_address: SocketAddr,
    grpc_connection_address: String,
    http_address: Option<SocketAddr>,
    server_status: LocalServerStatus,
    database_manager: Arc<DatabaseManager>,
    token_manager: Arc<TokenManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_diagnostics_updater: IntervalRunner,
    shutdown_receiver: Receiver<()>,
    background_task_spawner: TokioTaskSpawner,

    server_operator_override: Option<Arc<dyn ServerOperator>>,
    database_operator_override: Option<Arc<dyn DatabaseOperator>>,
    transaction_operator_override: Option<Arc<dyn TransactionOperator>>,
    user_operator_override: Option<Arc<dyn UserOperator>>,
}

impl ServerStateBuilder {
    pub fn database_manager(&self) -> Arc<DatabaseManager> {
        self.database_manager.clone()
    }

    pub fn token_manager(&self) -> Arc<TokenManager> {
        self.token_manager.clone()
    }

    pub fn background_task_spawner(&self) -> TokioTaskSpawner {
        self.background_task_spawner.clone()
    }

    pub fn server_status(&self) -> LocalServerStatus {
        self.server_status.clone()
    }

    pub fn server_operator(mut self, operator: Arc<dyn ServerOperator>) -> Self {
        self.server_operator_override = Some(operator);
        self
    }

    pub fn database_operator(mut self, operator: Arc<dyn DatabaseOperator>) -> Self {
        self.database_operator_override = Some(operator);
        self
    }

    pub fn transaction_operator(mut self, operator: Arc<dyn TransactionOperator>) -> Self {
        self.transaction_operator_override = Some(operator);
        self
    }

    pub fn user_operator(mut self, operator: Arc<dyn UserOperator>) -> Self {
        self.user_operator_override = Some(operator);
        self
    }

    pub fn build(self) -> ServerState {
        let server_operator =
            self.server_operator_override.unwrap_or_else(|| Arc::new(LocalServerOperator::new(self.server_status)));

        let database_operator = self.database_operator_override.unwrap_or_else(|| {
            Arc::new(LocalDatabaseOperator::new(self.database_manager.clone(), self.background_task_spawner.clone()))
        });

        let transaction_operator = self.transaction_operator_override.unwrap_or_else(|| {
            Arc::new(LocalTransactionOperator::new(self.database_manager.clone(), self.background_task_spawner.clone()))
        });

        let user_operator = self.user_operator_override.unwrap_or_else(|| {
            Arc::new(LocalUserOperator::new(
                self.database_manager.clone(),
                self.token_manager.clone(),
                transaction_operator.clone(),
            ))
        });

        ServerState {
            distribution_info: self.distribution_info,
            grpc_serving_address: self.grpc_serving_address,
            grpc_connection_address: self.grpc_connection_address,
            http_address: self.http_address,
            diagnostics_manager: self.diagnostics_manager,
            shutdown_receiver: self.shutdown_receiver,
            background_task_spawner: self.background_task_spawner,
            _database_diagnostics_updater: self.database_diagnostics_updater,
            server_operator,
            database_operator,
            transaction_operator,
            user_operator,
        }
    }
}
