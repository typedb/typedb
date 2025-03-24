/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use axum_server::{tls_rustls::RustlsConfig, Handle};
use concurrency::IntervalRunner;
use database::database_manager::DatabaseManager;
use diagnostics::{diagnostics_manager::DiagnosticsManager, Diagnostics};
use rand::seq::SliceRandom;
use resource::constants::server::{
    ASCII_LOGO, DATABASE_METRICS_UPDATE_INTERVAL, GRPC_CONNECTION_KEEPALIVE, SERVER_ID_ALPHABET, SERVER_ID_FILE_NAME,
    SERVER_ID_LENGTH,
};
use system::initialise_system_database;
use tokio::net::lookup_host;
use user::{initialise_default_user, user_manager::UserManager};

use crate::{
    authentication::{credential_verifier::CredentialVerifier, token_manager::TokenManager},
    error::ServerOpenError,
    parameters::config::{Config, DiagnosticsConfig, EncryptionConfig},
    service::{grpc, http},
};

#[derive(Debug)]
pub struct Server {
    id: String,
    deployment_id: String,
    logo: &'static str,
    distribution: &'static str,
    version: &'static str,
    config: Config,
    data_directory: PathBuf,
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_diagnostics_updater: IntervalRunner,
    user_manager: Arc<UserManager>,
    credential_verifier: Arc<CredentialVerifier>,
    token_manager: Arc<TokenManager>,
    grpc_service: grpc::typedb_service::TypeDBService,
    http_service: Option<http::typedb_service::TypeDBService>,
    shutdown_sender: tokio::sync::watch::Sender<()>,
    shutdown_receiver: tokio::sync::watch::Receiver<()>,
}

impl Server {
    pub async fn new(
        config: Config,
        logo: &'static str,
        distribution: &'static str,
        version: &'static str,
        deployment_id: Option<String>,
    ) -> Result<Self, ServerOpenError> {
        let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(());
        Self::new_with_external_shutdown(
            config,
            logo,
            distribution,
            version,
            deployment_id,
            shutdown_sender,
            shutdown_receiver,
        )
        .await
    }

    pub async fn new_with_external_shutdown(
        config: Config,
        logo: &'static str,
        distribution: &'static str,
        version: &'static str,
        deployment_id: Option<String>,
        shutdown_sender: tokio::sync::watch::Sender<()>,
        shutdown_receiver: tokio::sync::watch::Receiver<()>,
    ) -> Result<Self, ServerOpenError> {
        let storage_directory = &config.storage.data;
        let server_config = &config.server;
        let diagnostics_config = &config.diagnostics;

        Self::may_initialise_storage_directory(storage_directory)?;

        let server_id = Self::may_initialise_server_id(storage_directory)?;

        let deployment_id = deployment_id.unwrap_or(server_id.clone());

        let diagnostics_manager = Arc::new(
            Self::initialise_diagnostics(
                deployment_id.clone(),
                server_id.clone(),
                distribution,
                version,
                diagnostics_config,
                storage_directory.clone(),
                config.is_development_mode,
            )
            .await,
        );

        let database_manager = DatabaseManager::new(storage_directory)
            .map_err(|typedb_source| ServerOpenError::DatabaseOpen { typedb_source })?;
        let system_database = initialise_system_database(&database_manager);

        let user_manager = Arc::new(UserManager::new(system_database));
        initialise_default_user(&user_manager);

        let credential_verifier = Arc::new(CredentialVerifier::new(user_manager.clone()));
        let token_manager = Arc::new(
            TokenManager::new(server_config.authentication.token_expiration_seconds)
                .map_err(|typedb_source| ServerOpenError::TokenConfiguration { typedb_source })?,
        );

        let grpc_server_address = Self::resolve_address(server_config.address.clone()).await;

        let grpc_service = grpc::typedb_service::TypeDBService::new(
            grpc_server_address,
            database_manager.clone(),
            user_manager.clone(),
            credential_verifier.clone(),
            token_manager.clone(),
            diagnostics_manager.clone(),
            shutdown_receiver.clone(),
        );

        let http_server_address = match server_config.http_address.clone() {
            Some(http_address) => Some(Self::resolve_address(http_address).await),
            None => None,
        };
        let http_service = http_server_address.map(|http_address| {
            http::typedb_service::TypeDBService::new(
                http_address,
                database_manager.clone(),
                user_manager.clone(),
                credential_verifier.clone(),
                token_manager.clone(),
                diagnostics_manager.clone(),
                shutdown_receiver.clone(),
            )
        });

        Ok(Self {
            id: server_id,
            deployment_id,
            logo,
            distribution,
            version,
            data_directory: storage_directory.to_owned(),
            diagnostics_manager: diagnostics_manager.clone(),
            database_diagnostics_updater: IntervalRunner::new(
                move || Self::synchronize_database_metrics(diagnostics_manager.clone(), database_manager.clone()),
                DATABASE_METRICS_UPDATE_INTERVAL,
            ),
            user_manager,
            credential_verifier,
            token_manager,
            grpc_service,
            http_service,
            shutdown_sender,
            shutdown_receiver,
            config,
        })
    }

    fn may_initialise_server_id(storage_directory: &Path) -> Result<String, ServerOpenError> {
        let server_id_file = storage_directory.join(SERVER_ID_FILE_NAME);
        if server_id_file.exists() {
            let server_id = fs::read_to_string(&server_id_file)
                .map_err(|source| ServerOpenError::CouldNotReadServerIDFile {
                    path: server_id_file.to_str().unwrap_or("").to_owned(),
                    source: Arc::new(source),
                })?
                .trim()
                .to_owned();
            if server_id.is_empty() {
                Err(ServerOpenError::InvalidServerID { path: server_id_file.to_str().unwrap_or("").to_owned() })
            } else {
                Ok(server_id)
            }
        } else {
            let server_id = Self::generate_server_id();
            assert!(!server_id.is_empty(), "Generated server ID should not be empty");
            fs::write(server_id_file.clone(), &server_id).map_err(|source| {
                ServerOpenError::CouldNotCreateServerIDFile {
                    path: server_id_file.to_str().unwrap_or("").to_owned(),
                    source: Arc::new(source),
                }
            })?;
            Ok(server_id)
        }
    }

    fn generate_server_id() -> String {
        let mut rng = rand::thread_rng();
        (0..SERVER_ID_LENGTH).map(|_| SERVER_ID_ALPHABET.choose(&mut rng).unwrap()).collect()
    }

    fn may_initialise_storage_directory(storage_directory: &Path) -> Result<(), ServerOpenError> {
        if !storage_directory.exists() {
            Self::create_storage_directory(storage_directory)
        } else if !storage_directory.is_dir() {
            Err(ServerOpenError::NotADirectory { path: storage_directory.to_str().unwrap_or("").to_owned() })
        } else {
            Ok(())
        }
    }

    fn create_storage_directory(storage_directory: &Path) -> Result<(), ServerOpenError> {
        fs::create_dir_all(storage_directory).map_err(|source| ServerOpenError::CouldNotCreateDataDirectory {
            path: storage_directory.to_str().unwrap_or("").to_owned(),
            source: Arc::new(source),
        })?;
        Ok(())
    }

    async fn initialise_diagnostics(
        deployment_id: String,
        server_id: String,
        distribution: &str,
        version: &str,
        config: &DiagnosticsConfig,
        storage_directory: PathBuf,
        is_development_mode: bool,
    ) -> DiagnosticsManager {
        let diagnostics = Diagnostics::new(
            deployment_id,
            server_id,
            distribution.to_owned(),
            version.to_owned(),
            storage_directory,
            config.is_reporting_metric_enabled,
        );
        let diagnostics_manager = DiagnosticsManager::new(
            diagnostics,
            config.monitoring_port,
            config.is_monitoring_enabled,
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

    pub async fn serve(mut self) -> Result<(), ServerOpenError> {
        Self::print_hello(ASCII_LOGO, self.distribution, self.version, self.config.is_development_mode);

        Self::install_default_encryption_provider()?;

        let grpc_address = *self.grpc_service.address();
        let grpc_server = Self::serve_grpc(
            grpc_address,
            self.credential_verifier.clone(),
            self.token_manager.clone(),
            self.diagnostics_manager.clone(),
            &self.config.server.encryption,
            self.shutdown_receiver.clone(),
            self.grpc_service,
        );

        let (http_server, http_address) = if let Some(mut http_service) = self.http_service {
            let http_address = *http_service.address();
            if grpc_address == http_address {
                return Err(ServerOpenError::GrpcHttpConflictingAddress { address: grpc_address });
            }
            let server = Self::serve_http(
                http_address,
                self.credential_verifier,
                self.token_manager,
                self.diagnostics_manager,
                &self.config.server.encryption,
                self.shutdown_receiver,
                http_service,
            );
            (Some(server), Some(http_address))
        } else {
            (None, None)
        };

        Self::spawn_shutdown_handler(self.shutdown_sender);

        Self::print_serving_information(grpc_address, http_address);

        if let Some(http_server) = http_server {
            let (grpc_result, http_result) = tokio::join!(grpc_server, http_server);
            grpc_result?;
            http_result?;
        } else {
            grpc_server.await?;
        }
        Ok(())
    }

    fn install_default_encryption_provider() -> Result<(), ServerOpenError> {
        tokio_rustls::rustls::crypto::ring::default_provider()
            .install_default()
            .map_err(|_| ServerOpenError::HttpTlsUnsetDefaultCryptoProvider {})
    }

    async fn serve_grpc(
        address: SocketAddr,
        credential_verifier: Arc<CredentialVerifier>,
        token_manager: Arc<TokenManager>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        encryption_config: &EncryptionConfig,
        mut shutdown_receiver: tokio::sync::watch::Receiver<()>,
        service: grpc::typedb_service::TypeDBService,
    ) -> Result<(), ServerOpenError> {
        let mut grpc_server =
            tonic::transport::Server::builder().http2_keepalive_interval(Some(GRPC_CONNECTION_KEEPALIVE));
        if let Some(tls_config) = grpc::encryption::prepare_tls_config(encryption_config)? {
            grpc_server = grpc_server
                .tls_config(tls_config)
                .map_err(|source| ServerOpenError::GrpcTlsFailedConfiguration { source: Arc::new(source) })?;
        }
        let authenticator =
            grpc::authenticator::Authenticator::new(credential_verifier, token_manager, diagnostics_manager);

        grpc_server
            .layer(&authenticator)
            .add_service(typedb_protocol::type_db_server::TypeDbServer::new(service))
            .serve_with_shutdown(address, async {
                // The tonic server starts a shutdown process when this closure execution finishes
                shutdown_receiver.changed().await.expect("Expected shutdown receiver signal");
            })
            .await
            .map_err(|source| ServerOpenError::GrpcServe { address, source: Arc::new(source) })
    }

    async fn serve_http(
        address: SocketAddr,
        credential_verifier: Arc<CredentialVerifier>,
        token_manager: Arc<TokenManager>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        encryption_config: &EncryptionConfig,
        mut shutdown_receiver: tokio::sync::watch::Receiver<()>,
        service: http::typedb_service::TypeDBService,
    ) -> Result<(), ServerOpenError> {
        let authenticator =
            http::authenticator::Authenticator::new(credential_verifier, token_manager, diagnostics_manager);

        let encryption_config = http::encryption::prepare_tls_config(encryption_config)?;
        let http_service = Arc::new(service);
        let router_service = http::typedb_service::TypeDBService::create_protected_router(http_service.clone())
            .layer(authenticator)
            .merge(http::typedb_service::TypeDBService::create_unprotected_router(http_service))
            .layer(http::typedb_service::TypeDBService::create_cors_layer())
            .into_make_service();

        let shutdown_handle = Handle::new();
        let shutdown_handle_clone = shutdown_handle.clone();
        tokio::spawn(async move {
            shutdown_receiver.changed().await.expect("Expected shutdown receiver signal");
            shutdown_handle_clone.graceful_shutdown(None); // None: indefinite shutdown time
        });

        match encryption_config {
            Some(encryption_config) => {
                axum_server::bind_rustls(address, RustlsConfig::from_config(Arc::new(encryption_config)))
                    .handle(shutdown_handle)
                    .serve(router_service)
                    .await
            }
            None => axum_server::bind(address).handle(shutdown_handle).serve(router_service).await,
        }
        .map_err(|source| ServerOpenError::HttpServe { address, source: Arc::new(source) })
    }

    async fn resolve_address(address: String) -> SocketAddr {
        lookup_host(address.clone())
            .await
            .unwrap()
            .next()
            .unwrap_or_else(|| panic!("Unable to map address '{}' to any IP addresses", address))
    }

    fn print_hello(logo: &str, distribution: &str, version: &str, is_development_mode_enabled: bool) {
        println!("{logo}"); // very important
        let version = version.trim();
        if is_development_mode_enabled {
            println!("Running {distribution} {version} in development mode.");
        } else {
            println!("Running {distribution} {version}.");
        }
    }

    fn print_serving_information(grpc_address: SocketAddr, http_address: Option<SocketAddr>) {
        print!("Serving gRPC on {grpc_address}");
        if let Some(http_address) = http_address {
            print!(" and HTTP on {http_address}");
        }
        println!(".\nReady!");
    }

    fn spawn_shutdown_handler(shutdown_signal_sender: tokio::sync::watch::Sender<()>) {
        tokio::spawn(async move {
            Self::listen_to_ctrl_c_signal().await;
            println!("\nReceived CTRL-C. Initiating shutdown...");
            shutdown_signal_sender.send(()).expect("Expected a successful shutdown signal");

            tokio::spawn(Self::forced_shutdown_handler());
        });
    }

    async fn forced_shutdown_handler() {
        Self::listen_to_ctrl_c_signal().await;
        println!("\nReceived CTRL-C. Forcing shutdown...");
        std::process::exit(1);
    }

    async fn listen_to_ctrl_c_signal() {
        tokio::signal::ctrl_c().await.expect("Failed to listen for CTRL-C signal");
    }

    pub fn database_manager(&self) -> &DatabaseManager {
        self.grpc_service.database_manager()
    }
}
