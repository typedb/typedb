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
};

use concurrency::IntervalRunner;
use database::database_manager::DatabaseManager;
use diagnostics::{diagnostics_manager::DiagnosticsManager, Diagnostics};
use rand::seq::SliceRandom;
use resource::constants::server::{
    DATABASE_METRICS_UPDATE_INTERVAL, GRPC_CONNECTION_KEEPALIVE, SERVER_ID_ALPHABET, SERVER_ID_FILE_NAME,
    SERVER_ID_LENGTH,
};
use system::initialise_system_database;
use tokio::net::lookup_host;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use user::{initialise_default_user, user_manager::UserManager};

use crate::{
    authenticator::Authenticator,
    authenticator_cache::AuthenticatorCache,
    parameters::config::{Config, DiagnosticsConfig, EncryptionConfig},
    service::typedb_service::TypeDBService,
};
use resource::constants::server::ASCII_LOGO;
use crate::error::ServerOpenError;

#[derive(Debug)]
pub struct Server {
    id: String,
    deployment_id: String,
    distribution: &'static str,
    version: &'static str,
    address: SocketAddr,
    data_directory: PathBuf,
    user_manager: Arc<UserManager>,
    authenticator_cache: Arc<AuthenticatorCache>,
    typedb_service: Option<TypeDBService>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    config: Config,
    shutdown_sender: tokio::sync::watch::Sender<()>,
    _database_diagnostics_updater: IntervalRunner,
}

impl Server {
    pub async fn create(
        config: Config,
        distribution: &'static str,
        version: &'static str,
        deployment_id: Option<String>,
    ) -> Result<Self, ServerOpenError> {
        let storage_directory = &config.storage.data;
        let server_config = &config.server;

        Self::may_initialise_storage_directory(storage_directory)?;

        let server_id = Self::may_initialise_server_id(storage_directory)?;

        let deployment_id = deployment_id.unwrap_or(server_id.clone());

        let server_address = Self::resolve_address(server_config.address.clone()).await;

        let diagnostics_manager = Arc::new(
            Self::initialise_diagnostics(
                deployment_id.clone(),
                server_id.clone(),
                distribution,
                version,
                &server_config.diagnostics,
                storage_directory.clone(),
                server_config.is_development_mode,
            ).await
        );

        let database_manager = DatabaseManager::new(storage_directory)
            .map_err(|typedb_source| ServerOpenError::DatabaseOpen { typedb_source })?;
        let system_database = initialise_system_database(&database_manager);

        let user_manager = Arc::new(UserManager::new(system_database));
        initialise_default_user(&user_manager);

        let authenticator_cache = Arc::new(AuthenticatorCache::new());

        let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(());

        let typedb_service = TypeDBService::new(
            &server_address,
            database_manager.clone(),
            user_manager.clone(),
            authenticator_cache.clone(),
            diagnostics_manager.clone(),
            shutdown_receiver,
        );

        Ok(Self {
            id: server_id,
            deployment_id,
            distribution,
            version,
            address: server_address,
            data_directory: storage_directory.to_owned(),
            user_manager,
            authenticator_cache,
            typedb_service: Some(typedb_service),
            diagnostics_manager: diagnostics_manager.clone(),
            config,
            shutdown_sender,
            _database_diagnostics_updater: IntervalRunner::new(
                move || Self::synchronize_database_metrics(diagnostics_manager.clone(), database_manager.clone()),
                DATABASE_METRICS_UPDATE_INTERVAL,
            ),
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
        distribution: &'static str,
        version: &'static str,
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
            config.is_reporting_enabled,
        );
        let diagnostics_manager = DiagnosticsManager::new(diagnostics, config.monitoring_port, config.is_monitoring_enabled, is_development_mode);
        diagnostics_manager.may_start_monitoring().await;
        diagnostics_manager.may_start_reporting().await;

        diagnostics_manager
    }

    fn synchronize_database_metrics(diagnostics_manager: Arc<DiagnosticsManager>, database_manager: Arc<DatabaseManager>) {
        let metrics = database_manager
            .databases()
            .values()
            .filter(|database| DatabaseManager::is_user_database(database.name()))
            .map(|database| database.get_metrics())
            .collect();
        diagnostics_manager.submit_database_metrics(metrics);
    }

    async fn resolve_address(address: String) -> SocketAddr {
        lookup_host(address.clone())
            .await
            .unwrap()
            .next()
            .unwrap_or_else(|| panic!("Unable to map address '{}' to any IP addresses", address))
    }

    pub async fn serve(mut self) -> Result<(), ServerOpenError> {
        let service = typedb_protocol::type_db_server::TypeDbServer::new(self.typedb_service.take().unwrap());
        let authenticator = Authenticator::new(
            self.user_manager.clone(),
            self.authenticator_cache.clone(),
            self.diagnostics_manager.clone(),
        );

        Self::print_hello(self.distribution, self.version, self.config.server.is_development_mode);

        Self::create_tonic_server(&self.config.server.encryption)?
            .layer(&authenticator)
            .add_service(service)
            .serve_with_shutdown(self.address, async {
                // The tonic server starts a shutdown process when this closure execution finishes
                Self::shutdown_handler(self.shutdown_sender).await;
            })
            .await
            .map_err(|source| ServerOpenError::Serve { address: self.address, source: Arc::new(source) })
    }

    fn create_tonic_server(encryption_config: &EncryptionConfig) -> Result<tonic::transport::Server, ServerOpenError> {
        let mut tonic_server =
            Self::configure_tonic_server_encryption(tonic::transport::Server::builder(), encryption_config)?;
        Ok(tonic_server.http2_keepalive_interval(Some(GRPC_CONNECTION_KEEPALIVE)))
    }

    fn configure_tonic_server_encryption(
        server: tonic::transport::Server,
        encryption_config: &EncryptionConfig,
    ) -> Result<tonic::transport::Server, ServerOpenError> {
        if !encryption_config.enabled {
            return Ok(server);
        }

        let cert_path = encryption_config.cert.as_ref().ok_or_else(|| ServerOpenError::MissingTLSCertificate {})?;
        let cert = fs::read_to_string(cert_path).map_err(|source| ServerOpenError::CouldNotReadTLSCertificate {
            path: cert_path.display().to_string(),
            source: Arc::new(source),
        })?;
        let cert_key_path =
            encryption_config.cert_key.as_ref().ok_or_else(|| ServerOpenError::MissingTLSCertificateKey {})?;
        let cert_key =
            fs::read_to_string(cert_key_path).map_err(|source| ServerOpenError::CouldNotReadTLSCertificateKey {
                path: cert_key_path.display().to_string(),
                source: Arc::new(source),
            })?;
        let mut tls_config = ServerTlsConfig::new().identity(Identity::from_pem(cert, cert_key));

        if let Some(root_ca_path) = &encryption_config.root_ca {
            let root_ca = fs::read_to_string(root_ca_path).map_err(|source| ServerOpenError::CouldNotReadRootCA {
                path: root_ca_path.display().to_string(),
                source: Arc::new(source),
            })?;
            tls_config = tls_config.client_ca_root(Certificate::from_pem(root_ca)).client_auth_optional(true);
        }
        server.tls_config(tls_config).map_err(|source| ServerOpenError::TLSConfigError { source: Arc::new(source) })
    }

    fn print_hello(distribution: &'static str, version: &'static str, is_development_mode_enabled: bool) {
        println!("{ASCII_LOGO}"); // very important
        if is_development_mode_enabled {
            println!("Running {distribution} {version} in development mode.");
        } else {
            println!("Running {distribution} {version}.");
        }
        println!("Ready!");
    }

    async fn shutdown_handler(shutdown_signal_sender: tokio::sync::watch::Sender<()>) {
        Self::listen_to_ctrl_c_signal().await;
        println!("\nReceived CTRL-C. Initiating shutdown...");
        shutdown_signal_sender.send(()).expect("Expected a successful shutdown signal");

        tokio::spawn(Self::forced_shutdown_handler());
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
        self.typedb_service.as_ref().unwrap().database_manager()
    }
}
