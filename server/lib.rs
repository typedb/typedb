/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod authentication;
pub mod error;
pub mod parameters;
pub mod service;
pub mod state;
pub mod status;
pub mod system_init;

use std::{fs, future::Future, net::SocketAddr, path::Path, pin::Pin, sync::Arc};

use axum_server::{tls_rustls::RustlsConfig, Handle};
use database::database_manager::DatabaseManager;
use rand::prelude::SliceRandom;
use resource::{
    constants::server::{
        DISTRIBUTION_INFO, GRPC_CONNECTION_KEEPALIVE, SERVER_ID_ALPHABET, SERVER_ID_FILE_NAME, SERVER_ID_LENGTH,
    },
    distribution_info::DistributionInfo,
};
use tokio::sync::watch::{channel, Receiver, Sender};

use crate::{
    error::ServerOpenError,
    parameters::config::{Config, EncryptionConfig, StorageConfig},
    service::{grpc, http},
    state::{ArcServerState, BoxServerStatus, LocalServerState},
};

#[derive(Debug)]
pub struct ServerBuilder {
    distribution_info: Option<DistributionInfo>,
    server_state: Option<ArcServerState>,
    shutdown_channel: Option<(Sender<()>, Receiver<()>)>,
    storage_server_id: Option<String>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self { distribution_info: None, server_state: None, shutdown_channel: None, storage_server_id: None }
    }
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn distribution_info(mut self, distribution_info: DistributionInfo) -> Self {
        self.distribution_info = Some(distribution_info);
        self
    }

    pub fn server_state(mut self, server_state: ArcServerState) -> Self {
        self.server_state = Some(server_state);
        self
    }

    pub fn shutdown_channel(mut self, shutdown_channel: (Sender<()>, Receiver<()>)) -> Self {
        self.shutdown_channel = Some(shutdown_channel);
        self
    }

    pub async fn build(mut self, config: Config) -> Result<Server, ServerOpenError> {
        let server_id = self.initialise_storage(&config.storage)?.to_string();
        let distribution_info = self.distribution_info.unwrap_or(DISTRIBUTION_INFO);
        let (shutdown_sender, shutdown_receiver) = self.shutdown_channel.unwrap_or_else(|| channel(()));

        let server_state = match self.server_state {
            Some(server_state) => server_state,
            None => {
                let mut server_state = LocalServerState::new(
                    distribution_info,
                    config.clone(),
                    server_id,
                    None,
                    shutdown_receiver.clone(),
                )
                .await?;
                server_state.initialise_and_load().await.map_err(|error| ServerOpenError::ServerState { typedb_source: Box::new(error) })?;
                Arc::new(server_state)
            }
        };

        Ok(Server::new(distribution_info, config, server_state, shutdown_sender, shutdown_receiver))
    }

    pub fn initialise_storage(&mut self, storage_config: &StorageConfig) -> Result<&str, ServerOpenError> {
        if self.storage_server_id.is_none() {
            Self::may_initialise_storage_directory(&storage_config.data_directory)?;
            self.storage_server_id = Some(Self::may_initialise_server_id(&storage_config.data_directory)?);
        }
        Ok(self.storage_server_id.as_ref().unwrap())
    }

    fn may_initialise_storage_directory(storage_directory: &Path) -> Result<(), ServerOpenError> {
        debug_assert!(storage_directory.is_absolute());
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
}

#[derive(Debug)]
pub struct Server {
    distribution_info: DistributionInfo,
    config: Config,
    server_state: ArcServerState,
    shutdown_sender: Sender<()>,
    shutdown_receiver: Receiver<()>,
}

impl Server {
    pub fn new(
        distribution_info: DistributionInfo,
        config: Config,
        server_state: ArcServerState,
        shutdown_sender: Sender<()>,
        shutdown_receiver: Receiver<()>,
    ) -> Self {
        Self { distribution_info, config, server_state, shutdown_sender, shutdown_receiver }
    }

    pub async fn serve(self) -> Result<(), ServerOpenError> {
        Self::print_hello(self.distribution_info, self.config.development_mode.enabled);

        Self::install_default_encryption_provider()?;

        let server_state = self.server_state;

        let grpc_server = Self::serve_grpc(
            server_state.grpc_address().await,
            &self.config.server.encryption,
            server_state.clone(),
            self.shutdown_receiver.clone(),
        );
        let http_server = if let Some(http_address) = server_state.http_address().await {
            let server = Self::serve_http(
                self.distribution_info,
                http_address,
                &self.config.server.encryption,
                server_state.clone(),
                self.shutdown_receiver,
            );
            Some(server)
        } else {
            None
        };

        Self::print_serving_information(
            server_state
                .server_status()
                .await
                .map_err(|typedb_source| ServerOpenError::ServerState { typedb_source })?,
            &self.config.server.encryption,
        );

        Self::spawn_shutdown_handler(self.shutdown_sender);
        if let Some(http_server) = http_server {
            let (grpc_result, http_result) = tokio::join!(grpc_server, http_server);
            grpc_result?;
            http_result?;
        } else {
            grpc_server.await?;
        }
        Ok(())
    }

    async fn serve_grpc(
        address: SocketAddr,
        encryption_config: &EncryptionConfig,
        server_state: ArcServerState,
        mut shutdown_receiver: Receiver<()>,
    ) -> Result<(), ServerOpenError> {
        let authenticator = grpc::authenticator::Authenticator::new(server_state.clone());
        let service = grpc::typedb_service::TypeDBService::new(address.clone(), server_state.clone());
        let mut grpc_server =
            tonic::transport::Server::builder().http2_keepalive_interval(Some(GRPC_CONNECTION_KEEPALIVE));
        if let Some(tls_config) = grpc::encryption::prepare_tls_config(encryption_config)? {
            grpc_server = grpc_server
                .tls_config(tls_config)
                .map_err(|source| ServerOpenError::GrpcTlsFailedConfiguration { source: Arc::new(source) })?;
        }
        grpc_server
            .layer(&authenticator)
            .add_service(typedb_protocol::type_db_server::TypeDbServer::new(service))
            .serve_with_shutdown(address, async {
                // The tonic server starts a shutdown process when this closure execution finishes
                shutdown_receiver.changed().await.expect("Expected shutdown receiver signal");
            })
            .await
            .map_err(|err| ServerOpenError::GrpcServe { address, source: Arc::new(err) })
    }

    async fn serve_http(
        distribution_info: DistributionInfo,
        address: SocketAddr,
        encryption_config: &EncryptionConfig,
        server_state: ArcServerState,
        mut shutdown_receiver: Receiver<()>,
    ) -> Result<(), ServerOpenError> {
        let authenticator = http::authenticator::Authenticator::new(server_state.clone());
        let service = http::typedb_service::TypeDBService::new(distribution_info, address, server_state.clone());
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

    fn print_hello(distribution_info: DistributionInfo, is_development_mode_enabled: bool) {
        println!("{}", distribution_info.logo); // very important
        if is_development_mode_enabled {
            println!("Running {} {} in development mode.", distribution_info.distribution, distribution_info.version);
        } else {
            println!("Running {} {}.", distribution_info.distribution, distribution_info.version);
        }
    }

    fn print_serving_information(server_status: BoxServerStatus, encryption_config: &EncryptionConfig) {
        print!("Serving gRPC on {}", server_status.grpc_address());
        if let Some(http_address) = server_status.http_address() {
            print!(" and HTTP on {http_address}");
        }
        if encryption_config.enabled {
            println!(" with TLS enabled.");
            println!("**To allow driver connections, drivers must also be configured to use TLS.**")
        } else {
            println!(" without TLS.");
            println!("WARNING: TLS NOT ENABLED. This means connections are insecure and transmit username/password credentials unencrypted over the network.");
            println!("**To allow driver connections, drivers must also be configured to *not* use TLS**")
        }
        println!("\nReady!");
    }

    fn spawn_shutdown_handler(shutdown_sender: Sender<()>) {
        tokio::spawn(async move {
            Self::wait_for_ctrl_c_signal().await;
            println!("\nReceived CTRL-C. Initiating shutdown...");
            shutdown_sender.send(()).expect("Expected a successful shutdown signal");

            tokio::spawn(Self::forced_shutdown_handler());
        });
    }

    async fn forced_shutdown_handler() {
        Self::wait_for_ctrl_c_signal().await;
        println!("\nReceived CTRL-C. Forcing shutdown...");
        std::process::exit(1);
    }

    async fn wait_for_ctrl_c_signal() {
        tokio::signal::ctrl_c().await.expect("Failed to listen for CTRL-C signal");
    }

    fn install_default_encryption_provider() -> Result<(), ServerOpenError> {
        tokio_rustls::rustls::crypto::ring::default_provider()
            .install_default()
            .map_err(|_| ServerOpenError::HttpTlsUnsetDefaultCryptoProvider {})
    }

    pub async fn database_manager(&self) -> Arc<DatabaseManager> {
        self.server_state.database_manager().await
    }
}
