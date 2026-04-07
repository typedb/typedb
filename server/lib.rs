/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    fs,
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    pin::Pin,
    sync::Arc,
};

use axum_server::{tls_rustls::RustlsConfig, Handle};
use concurrency::{TokioTaskSpawner, TokioTaskTracker};
use database::database_manager::DatabaseManager;
use futures::future::try_join_all;
use rand::prelude::SliceRandom;
use resource::{
    constants::server::{
        DISTRIBUTION, DISTRIBUTION_INFO, GRPC_CONNECTION_KEEPALIVE, SERVER_ID_ALPHABET, SERVER_ID_FILE_NAME,
        SERVER_ID_LENGTH,
    },
    distribution_info::DistributionInfo,
};
use tokio::sync::watch::{channel, Receiver, Sender};
use tracing::info;

use crate::{
    error::ServerOpenError,
    parameters::config::{AdminConfig, Config, EncryptionConfig, ServerConfig, StorageConfig},
    service::{admin::localhost_guard::LocalhostGuardLayer, grpc, http},
    state::{BoxServerStatus, ServerState},
};

pub mod authentication;
pub mod error;
pub mod parameters;
pub mod service;
pub mod state;
pub mod status;
pub mod system_init;
pub mod transaction;

pub mod admin_proto {
    pub use server_admin_proto::*;
}

pub type AdminServeFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ServerOpenError>> + Send>>;

pub struct ServerBuilder {
    distribution_info: Option<DistributionInfo>,
    server_state: Option<Arc<ServerState>>,
    shutdown_channel: Option<(Sender<()>, Receiver<()>)>,
    storage_server_id: Option<String>,
    background_tasks_tracker: Option<TokioTaskTracker>,
    admin_serve_override: Option<AdminServeFuture>,
}

impl std::fmt::Debug for ServerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerBuilder")
            .field("distribution_info", &self.distribution_info)
            .field("server_state", &self.server_state)
            .field("shutdown_channel", &self.shutdown_channel)
            .field("storage_server_id", &self.storage_server_id)
            .field("background_tasks_tracker", &self.background_tasks_tracker)
            .field("admin_serve_override", &self.admin_serve_override.as_ref().map(|_| "..."))
            .finish()
    }
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            distribution_info: None,
            server_state: None,
            shutdown_channel: None,
            storage_server_id: None,
            background_tasks_tracker: None,
            admin_serve_override: None,
        }
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

    pub fn server_state(mut self, server_state: Arc<ServerState>) -> Self {
        self.server_state = Some(server_state);
        self
    }

    pub fn shutdown_channel(mut self, shutdown_channel: (Sender<()>, Receiver<()>)) -> Self {
        self.shutdown_channel = Some(shutdown_channel);
        self
    }

    pub fn background_tasks_tracker(mut self, background_tasks_tracker: TokioTaskTracker) -> Self {
        self.background_tasks_tracker = Some(background_tasks_tracker);
        self
    }

    pub fn admin_serve_override(mut self, serve_future: AdminServeFuture) -> Self {
        self.admin_serve_override = Some(serve_future);
        self
    }

    pub async fn build(mut self, config: Config) -> Result<Server, ServerOpenError> {
        let server_id = self.initialise_storage(&config.storage)?.to_string();
        let distribution_info = self.distribution_info.unwrap_or(DISTRIBUTION_INFO);
        let (shutdown_sender, shutdown_receiver) = self.shutdown_channel.unwrap_or_else(|| channel(()));
        let background_tasks_tracker =
            self.background_tasks_tracker.unwrap_or_else(|| TokioTaskTracker::new(shutdown_receiver.clone()));

        let server_state = match self.server_state {
            Some(server_state) => server_state,
            None => {
                let server_state = ServerState::new(
                    distribution_info,
                    config.clone(),
                    server_id,
                    None,
                    shutdown_receiver.clone(),
                    background_tasks_tracker.get_spawner(),
                )
                .await?
                .build();
                server_state
                    .initialise()
                    .await
                    .map_err(|error| ServerOpenError::ServerState { typedb_source: error })?;
                Arc::new(server_state)
            }
        };

        Ok(Server::new(
            distribution_info,
            config,
            server_state,
            shutdown_sender,
            shutdown_receiver,
            background_tasks_tracker,
            self.admin_serve_override,
        ))
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

pub struct Server {
    distribution_info: DistributionInfo,
    config: Config,
    server_state: Arc<ServerState>,
    shutdown_sender: Sender<()>,
    shutdown_receiver: Receiver<()>,
    background_tasks_tracker: TokioTaskTracker,
    admin_serve_override: Option<AdminServeFuture>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("distribution_info", &self.distribution_info)
            .field("config", &self.config)
            .field("server_state", &self.server_state)
            .field("shutdown_sender", &self.shutdown_sender)
            .field("shutdown_receiver", &self.shutdown_receiver)
            .field("background_tasks_tracker", &self.background_tasks_tracker)
            .field("admin_serve_override", &self.admin_serve_override.as_ref().map(|_| "..."))
            .finish()
    }
}

impl Server {
    pub fn new(
        distribution_info: DistributionInfo,
        config: Config,
        server_state: Arc<ServerState>,
        shutdown_sender: Sender<()>,
        shutdown_receiver: Receiver<()>,
        background_tasks_tracker: TokioTaskTracker,
        admin_serve_override: Option<AdminServeFuture>,
    ) -> Self {
        Self {
            distribution_info,
            config,
            server_state,
            shutdown_sender,
            shutdown_receiver,
            background_tasks_tracker,
            admin_serve_override,
        }
    }

    pub async fn serve(self) -> Result<(), ServerOpenError> {
        Self::print_hello(self.distribution_info, self.config.development_mode.enabled);
        let serve_result = Self::serve_all(
            self.distribution_info,
            self.config.server.clone(),
            self.server_state,
            self.shutdown_sender.clone(),
            self.shutdown_receiver,
            self.background_tasks_tracker.get_spawner(),
            self.admin_serve_override,
        )
        .await;
        let _ = self.shutdown_sender.send(());
        self.background_tasks_tracker.join().await;
        serve_result
    }

    async fn serve_all(
        distribution_info: DistributionInfo,
        server_config: ServerConfig,
        server_state: Arc<ServerState>,
        shutdown_sender: Sender<()>,
        shutdown_receiver: Receiver<()>,
        background_tasks_spawner: TokioTaskSpawner,
        admin_serve_override: Option<AdminServeFuture>,
    ) -> Result<(), ServerOpenError> {
        Self::install_default_encryption_provider()?;

        let mut servers: Vec<Pin<Box<dyn Future<Output = Result<(), ServerOpenError>> + Send>>> = Vec::new();

        let grpc_server = Self::serve_grpc(
            server_state.grpc_serving_address(),
            &server_config.encryption,
            server_state.clone(),
            shutdown_receiver.clone(),
        );
        servers.push(Box::pin(grpc_server));

        if let Some(http_serving_address) = server_state.http_serving_address() {
            let http_server = Self::serve_http(
                http_serving_address,
                &server_config.encryption,
                server_state.clone(),
                shutdown_receiver.clone(),
                background_tasks_spawner,
            );
            servers.push(Box::pin(http_server));
        }

        if server_config.admin.enabled {
            let admin_server = admin_serve_override.unwrap_or_else(|| {
                let address = SocketAddr::from((Ipv4Addr::LOCALHOST, server_config.admin.port));
                assert!(address.ip().is_loopback(), "The admin server must server only on a loopback address");
                Self::serve_admin(address, server_state.clone(), shutdown_receiver.clone())
            });
            servers.push(Box::pin(admin_server));
        }

        Self::print_serving_information(
            server_state
                .servers()
                .status()
                .await
                .map_err(|typedb_source| ServerOpenError::ServerState { typedb_source })?,
            distribution_info,
            &server_config.encryption,
        );

        Self::spawn_shutdown_handler(shutdown_sender);
        try_join_all(servers).await.map(|_| ())
    }

    async fn serve_grpc(
        address: SocketAddr,
        encryption_config: &EncryptionConfig,
        server_state: Arc<ServerState>,
        mut shutdown_receiver: Receiver<()>,
    ) -> Result<(), ServerOpenError> {
        let authenticator = grpc::authenticator::Authenticator::new(server_state.clone());
        let service = grpc::typedb_service::GRPCTypeDBService::new(server_state.clone());
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
        address: SocketAddr,
        encryption_config: &EncryptionConfig,
        server_state: Arc<ServerState>,
        mut shutdown_receiver: Receiver<()>,
        background_tasks: TokioTaskSpawner,
    ) -> Result<(), ServerOpenError> {
        let authenticator = http::authenticator::Authenticator::new(server_state.clone());
        let service = http::typedb_service::HTTPTypeDBService::new(server_state.clone(), background_tasks);
        let encryption_config = http::encryption::prepare_tls_config(encryption_config)?;
        let http_service = Arc::new(service);
        let router_service = http::typedb_service::HTTPTypeDBService::create_protected_router(http_service.clone())
            .layer(authenticator)
            .merge(http::typedb_service::HTTPTypeDBService::create_unprotected_router(http_service))
            .layer(http::typedb_service::HTTPTypeDBService::create_cors_layer())
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

    fn serve_admin(
        address: SocketAddr,
        server_state: Arc<ServerState>,
        mut shutdown_receiver: Receiver<()>,
    ) -> AdminServeFuture {
        let admin_service = service::admin::AdminService::new(server_state);
        Box::pin(async move {
            tonic::transport::Server::builder()
                .http2_keepalive_interval(Some(GRPC_CONNECTION_KEEPALIVE))
                .layer(LocalhostGuardLayer)
                .add_service(admin_proto::type_db_admin_server::TypeDbAdminServer::new(admin_service))
                .serve_with_shutdown(address, async {
                    // The tonic server starts a shutdown process when this closure execution finishes
                    shutdown_receiver.changed().await.expect("Expected shutdown receiver signal");
                })
                .await
                .map_err(|err| ServerOpenError::AdminServe { address, source: Arc::new(err) })
        })
    }

    fn print_hello(distribution_info: DistributionInfo, is_development_mode_enabled: bool) {
        println!("{}", distribution_info.logo); // very important
        let version = distribution_info.version.trim();
        if is_development_mode_enabled {
            println!("Running {} {} in development mode.", distribution_info.distribution, version);
        } else {
            println!("Running {} {}.", distribution_info.distribution, version);
        }
    }

    fn print_serving_information(
        server_status: BoxServerStatus,
        distribution_info: DistributionInfo,
        encryption_config: &EncryptionConfig,
    ) {
        const UNKNOWN: &str = "<UNKNOWN ADDRESS>";
        let grpc_serving_address = server_status.grpc_serving_address().unwrap_or(UNKNOWN);
        let grpc_connection_address = server_status.grpc_connection_address().unwrap_or(UNKNOWN);
        print!("Serving gRPC on {grpc_serving_address}");
        if grpc_connection_address != grpc_serving_address {
            print!(" (connect through {grpc_connection_address})");
        }
        if let Some(http_serving_address) = server_status.http_serving_address() {
            print!(", HTTP on {http_serving_address}");
            if let Some(http_connection_address) = server_status.http_connection_address() {
                if http_serving_address != http_connection_address {
                    print!(" (connect through {http_connection_address})");
                }
            }
        }
        if let Some(admin_address) = server_status.admin_address() {
            print!(", Admin on {admin_address}");
        }
        if encryption_config.enabled {
            println!(" with TLS enabled.");
            println!("**To allow driver connections, drivers must also be configured to use TLS.**")
        } else {
            println!(" without TLS.");
            println!("WARNING: TLS NOT ENABLED. This means connections are insecure and transmit username/password credentials unencrypted over the network.");
            println!("**To allow driver connections, drivers must also be configured to *not* use TLS**")
        }
        if distribution_info.distribution == DISTRIBUTION {
            // Same distribution -> the initialization is finished.
            println!();
            info!("\nReady!");
        }
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

    pub fn database_manager(&self) -> Arc<DatabaseManager> {
        self.server_state.databases().manager()
    }
}
