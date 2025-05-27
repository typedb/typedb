/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{net::SocketAddr, sync::Arc};

use crate::{
    error::ServerOpenError,
    parameters::config::{Config, EncryptionConfig},
    service::{grpc, http},
    state::LocalServerState,
};
use axum_server::{tls_rustls::RustlsConfig, Handle};
use database::database_manager::DatabaseManager;
use resource::constants::server::SERVER_INFO;
use resource::{constants::server::GRPC_CONNECTION_KEEPALIVE, server_info::ServerInfo};
use tokio::{
    net::lookup_host,
    sync::watch::{Receiver, Sender},
};
use crate::state::ServerState;

pub struct Server {
    server_info: ServerInfo,
    config: Config,
    server_state: Arc<Box<dyn ServerState + Send + Sync>>,
    shutdown_sender: Sender<()>,
    shutdown_receiver: Receiver<()>,
}

impl Server {
    pub async fn new_with_local_server_state(
        server_info: ServerInfo,
        config: Config,
        shutdown_sender: Sender<()>,
        shutdown_receiver: Receiver<()>,
    ) -> Result<Self, ServerOpenError> {
        let server_state = LocalServerState::new(SERVER_INFO, config.clone(), None, shutdown_receiver.clone()).await?;
        Ok(Self::new(server_info, config, Arc::new(Box::new(server_state)), shutdown_sender, shutdown_receiver))
    }
    
    pub fn new(
        server_info: ServerInfo,
        config: Config,
        server_state: Arc<Box<dyn ServerState + Send + Sync>>,
        shutdown_sender: Sender<()>,
        shutdown_receiver: Receiver<()>,
    ) -> Self {
        Self {
            server_info,
            config,
            server_state,
            shutdown_sender,
            shutdown_receiver,
        }
    }

    pub async fn serve(self) -> Result<(), ServerOpenError> {
        Self::print_hello(self.server_info, self.config.development_mode.enabled);

        Self::install_default_encryption_provider()?;

        let grpc_address = Self::resolve_address(self.config.server.address).await;
        let http_address_opt = if self.config.server.http_enabled {
            Some(
                Self::validate_and_resolve_http_address(self.config.server.http_address.clone(), grpc_address.clone())
                    .await?,
            )
        } else {
            None
        };

        let grpc_server = Self::serve_grpc(
            grpc_address,
            &self.config.server.encryption,
            self.server_state.clone(),
            self.shutdown_receiver.clone(),
        );
        let http_server = if let Some(http_address) = http_address_opt {
            let server = Self::serve_http(
                self.server_info,
                http_address,
                &self.config.server.encryption,
                self.server_state.clone(),
                self.shutdown_receiver,
            );
            Some(server)
        } else {
            None
        };

        Self::print_serving_information(grpc_address, http_address_opt);

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
        server_state: Arc<Box<dyn ServerState + Send + Sync>>,
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
        server_info: ServerInfo,
        address: SocketAddr,
        encryption_config: &EncryptionConfig,
        server_state: Arc<Box<dyn ServerState + Send + Sync>>,
        mut shutdown_receiver: Receiver<()>,
    ) -> Result<(), ServerOpenError> {
        let authenticator = http::authenticator::Authenticator::new(server_state.clone());
        let service = http::typedb_service::TypeDBService::new(server_info, address, server_state.clone());
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

    async fn validate_and_resolve_http_address(
        http_address: String,
        grpc_address: SocketAddr,
    ) -> Result<SocketAddr, ServerOpenError> {
        let http_address = Self::resolve_address(http_address).await;
        if grpc_address == http_address {
            return Err(ServerOpenError::GrpcHttpConflictingAddress { address: grpc_address });
        }
        Ok(http_address)
    }

    pub async fn resolve_address(address: String) -> SocketAddr {
        lookup_host(address.clone())
            .await
            .unwrap()
            .next()
            .unwrap_or_else(|| panic!("Unable to map address '{}' to any IP addresses", address))
    }

    fn print_hello(server_info: ServerInfo, is_development_mode_enabled: bool) {
        println!("{}", server_info.logo); // very important
        if is_development_mode_enabled {
            println!("Running {} {} in development mode.", server_info.distribution, server_info.version);
        } else {
            println!("Running {} {}.", server_info.distribution, server_info.version);
        }
    }

    fn print_serving_information(grpc_address: SocketAddr, http_address: Option<SocketAddr>) {
        print!("Serving gRPC on {grpc_address}");
        if let Some(http_address) = http_address {
            print!(" and HTTP on {http_address}");
        }
        println!(".\nReady!");
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
        self.server_state.database_manager()
    }
}
