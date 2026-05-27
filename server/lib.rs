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
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use axum_server::{Handle, tls_rustls::RustlsConfig};
use concurrency::{TokioTaskSpawner, TokioTaskTracker};
use database::database_manager::DatabaseManager;
use futures::future::try_join_all;
use rand::prelude::SliceRandom;
use resource::{
    constants::{
        common::STUDIO_URL,
        server::{
            ADMIN_SOCKET_FILE_MODE, DISTRIBUTION_INFO, GRPC_CONNECTION_KEEPALIVE, GRPC_MAX_MESSAGE_SIZE,
            SERVER_ID_ALPHABET, SERVER_ID_FILE_NAME, SERVER_ID_LENGTH,
        },
    },
    distribution_info::DistributionInfo,
};
use tokio::{
    net::UnixListener,
    sync::watch::{Receiver, Sender, channel},
};
use tokio_stream::wrappers::UnixListenerStream;
use tracing::{info, warn};

use crate::{
    error::ServerOpenError,
    parameters::config::{Config, EncryptionConfig, ServerConfig, StorageConfig},
    service::{grpc, http},
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
            server_state.grpc_listen_address(),
            &server_config.encryption,
            server_state.clone(),
            shutdown_receiver.clone(),
        );
        servers.push(Box::pin(grpc_server));

        if let Some(http_listen_address) = server_state.http_listen_address() {
            let http_server = Self::serve_http(
                http_listen_address,
                &server_config.encryption,
                server_state.clone(),
                shutdown_receiver.clone(),
                background_tasks_spawner,
            );
            servers.push(Box::pin(http_server));
        }

        if let Some(socket_path) = server_state.admin_socket_path().map(Path::to_path_buf) {
            let admin_server = admin_serve_override
                .unwrap_or_else(|| Self::serve_admin(socket_path, server_state.clone(), shutdown_receiver.clone()));
            servers.push(Box::pin(admin_server));
        }

        let server_status = server_state
            .servers()
            .status()
            .await
            .map_err(|typedb_source| ServerOpenError::ServerState { typedb_source })?;
        Self::print_serving_information(&server_status, &server_config.encryption);
        if distribution_info.is_default_distribution() {
            Self::print_ready();
        }

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
            .add_service(
                typedb_protocol::type_db_server::TypeDbServer::new(service)
                    .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
                    .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE),
            )
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
        socket_path: PathBuf,
        server_state: Arc<ServerState>,
        mut shutdown_receiver: Receiver<()>,
    ) -> AdminServeFuture {
        let admin_service = service::admin::AdminService::new(server_state);
        Box::pin(async move {
            let listener = bind_admin_socket(&socket_path)?;
            let incoming = UnixListenerStream::new(listener);
            let socket_path_for_cleanup = socket_path.clone();
            let serve_result = tonic::transport::Server::builder()
                .add_service(admin_proto::type_db_admin_server::TypeDbAdminServer::new(admin_service))
                .serve_with_incoming_shutdown(incoming, async {
                    shutdown_receiver.changed().await.expect("Expected shutdown receiver signal");
                })
                .await
                .map_err(|err| ServerOpenError::AdminServe {
                    path: socket_path.to_string_lossy().into_owned(),
                    source: Arc::new(err),
                });
            // Best-effort: remove the socket file when serving stops so the next start
            // doesn't trip the "path exists" guard. Failures here only get logged because
            // the more important "couldn't serve" error is already being returned, and on
            // hard crashes the unlink wouldn't run anyway — the stale-socket cleanup at
            // bind time is the durable mechanism.
            if let Err(err) = fs::remove_file(&socket_path_for_cleanup) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    warn!(
                        "Could not remove admin socket file '{}' after shutdown: {err}",
                        socket_path_for_cleanup.display()
                    );
                }
            }
            serve_result
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

    pub fn print_serving_information(server_status: &BoxServerStatus, encryption_config: &EncryptionConfig) {
        const UNKNOWN: &str = "<UNKNOWN ADDRESS>";
        const DISABLED: &str = "disabled";
        println!("Serving:");

        let grpc_listen_address = server_status.grpc_listen_address().unwrap_or(UNKNOWN);
        match server_status.grpc_advertise_address() {
            Some(grpc_advertise_address) if grpc_advertise_address != grpc_listen_address => {
                println!("  gRPC:       {grpc_listen_address} (connect via {grpc_advertise_address})");
            }
            _ => println!("  gRPC:       {grpc_listen_address}"),
        }

        match server_status.http_listen_address() {
            Some(http_listen_address) => match server_status.http_advertise_address() {
                Some(http_advertise_address) if http_advertise_address != http_listen_address => {
                    println!("  HTTP:       {http_listen_address} (connect via {http_advertise_address})");
                }
                _ => println!("  HTTP:       {http_listen_address}"),
            },
            None => println!("  HTTP:       {DISABLED}"),
        }

        match server_status.admin_address() {
            Some(admin_address) => println!("  Admin:      {admin_address} (Unix socket, mode 0600)"),
            None => println!("  Admin:      {DISABLED}"),
        }

        match server_status.monitoring_address() {
            Some(monitoring_address) => {
                println!("  Monitoring: http://{monitoring_address}/diagnostics (Prometheus scrape)");
                println!("              http://{monitoring_address}/diagnostics?format=json (JSON)");
            }
            None => println!("  Monitoring: {DISABLED}"),
        }

        if encryption_config.enabled {
            println!("TLS: enabled");
            println!("  Drivers must also be configured to use TLS.");
        } else {
            println!("TLS: disabled");
            println!("  WARNING: TLS NOT ENABLED. Credentials are transmitted unencrypted in plaintext.");
            println!("  Drivers must be configured to connect *without TLS*.");
        }

        let grpc_connect_address =
            Self::connect_address(server_status.grpc_advertise_address(), server_status.grpc_listen_address());
        let http_connect_address =
            Self::connect_address(server_status.http_advertise_address(), server_status.http_listen_address());
        if grpc_connect_address.is_some() || http_connect_address.is_some() {
            println!("\nTo connect:");
            if let Some(http_connect_address) = http_connect_address.as_deref() {
                println!("  Studio:  {}", Self::studio_connect_link(http_connect_address, encryption_config));
            }
            if let Some(grpc_connect_address) = grpc_connect_address.as_deref() {
                println!("  Console: {}", Self::console_connect_command(grpc_connect_address, encryption_config));
            }
        }

        println!();
    }

    pub fn print_ready() {
        info!("\nReady!");
    }

    fn connect_address(advertise: Option<&str>, listen: Option<&str>) -> Option<String> {
        if let Some(advertise) = advertise {
            Some(advertise.to_owned())
        } else {
            listen.map(|listen| listen.replace("0.0.0.0", "127.0.0.1"))
        }
    }

    fn studio_connect_link(http_advertise_address: &str, encryption_config: &EncryptionConfig) -> String {
        let scheme = if encryption_config.enabled { "https" } else { "http" };
        format!("{STUDIO_URL}/connect?address={scheme}://{http_advertise_address}&username=admin")
    }

    fn console_connect_command(grpc_advertise_address: &str, encryption_config: &EncryptionConfig) -> String {
        if encryption_config.enabled {
            if let Some(cert_path) = encryption_config.ca_certificate.as_ref() {
                let cert_path = cert_path.as_path().display();
                format!(
                    "typedb console --address https://{grpc_advertise_address} --username admin --tls-root-ca={cert_path}"
                )
            } else {
                format!("typedb console --address https://{grpc_advertise_address} --username admin")
            }
        } else {
            format!("typedb console --address {grpc_advertise_address} --tls-disabled --username admin")
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

    // TODO: It is only used in tests, and exposing it directly outside of the DatabaseOperator is risky.
    // Remove?
    pub fn database_manager(&self) -> Arc<DatabaseManager> {
        self.server_state.databases().manager()
    }
}

/// Bind the admin Unix domain socket with restrictive permissions.
///
/// Trust on the admin channel is anchored entirely in filesystem access to this socket
/// file, so the cleanup-then-bind-then-chmod sequence is load-bearing:
///
/// 1. If the path already exists as a socket from a previous run, unlink it — `bind(2)`
///    would otherwise fail with EADDRINUSE.
/// 2. If the path exists but is *not* a socket (regular file, symlink, etc.), refuse:
///    overwriting an arbitrary file could be a foothold for a malicious local process,
///    and the user should investigate before we delete it.
/// 3. Bind, then immediately `chmod 0600`. Tokio's `UnixListener::bind` honours `umask`,
///    which we can't trust to be `0o077`, so an explicit chmod is required to guarantee
///    the socket is owner-readable only.
fn bind_admin_socket(socket_path: &Path) -> Result<UnixListener, ServerOpenError> {
    if let Some(parent) = socket_path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).map_err(|source| ServerOpenError::AdminSocketBind {
                path: socket_path.to_string_lossy().into_owned(),
                source: Arc::new(source),
            })?;
        }
    }

    match fs::symlink_metadata(socket_path) {
        Ok(metadata) => {
            use std::os::unix::fs::FileTypeExt;
            if metadata.file_type().is_socket() {
                fs::remove_file(socket_path).map_err(|source| ServerOpenError::AdminSocketCleanup {
                    path: socket_path.to_string_lossy().into_owned(),
                    source: Arc::new(source),
                })?;
            } else {
                return Err(ServerOpenError::AdminSocketPathInUse {
                    path: socket_path.to_string_lossy().into_owned(),
                });
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(ServerOpenError::AdminSocketBind {
                path: socket_path.to_string_lossy().into_owned(),
                source: Arc::new(source),
            });
        }
    }

    let listener = UnixListener::bind(socket_path).map_err(|source| ServerOpenError::AdminSocketBind {
        path: socket_path.to_string_lossy().into_owned(),
        source: Arc::new(source),
    })?;

    fs::set_permissions(socket_path, fs::Permissions::from_mode(ADMIN_SOCKET_FILE_MODE)).map_err(|source| {
        ServerOpenError::AdminSocketChmod {
            path: socket_path.to_string_lossy().into_owned(),
            source: Arc::new(source),
        }
    })?;

    info!("Admin Unix socket bound at {} (mode {:#o})", socket_path.display(), ADMIN_SOCKET_FILE_MODE);
    Ok(listener)
}
