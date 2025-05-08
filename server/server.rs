/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::service::state::ServerState;
use tokio::sync::watch::{channel, Sender, Receiver};
use crate::{
    error::ServerOpenError,
    parameters::config::{Config, EncryptionConfig},
};
use database::database_manager::DatabaseManager;
use resource::constants::server::GRPC_CONNECTION_KEEPALIVE;
use resource::server_info::ServerInfo;
use std::{
    net::SocketAddr,
    sync::Arc,
};
use typedb_protocol::type_db_server::TypeDbServer;

#[derive(Debug)]
pub struct Server {
    server_info: ServerInfo,
    config: Config,
    server_state: Arc<ServerState>,
    shutdown_sig_sender: Sender<()>,
    shutdown_sig_receiver: Receiver<()>
}

impl Server {
    pub async fn new(
        server_info: ServerInfo,
        config: Config,
        deployment_id: Option<String>
    ) -> Result<Self, ServerOpenError> {
        let (shutdown_sig_sender, shutdown_sig_receiver) = channel(());
        Self::new_with_external_shutdown(
            server_info,
            config,
            deployment_id,
            shutdown_sig_sender,
            shutdown_sig_receiver
        )
    }

    pub async fn new_with_external_shutdown(
        server_info: ServerInfo,
        config: Config,
        deployment_id: Option<String>,
        shutdown_sig_sender: Sender<()>,
        shutdown_sig_receiver: Receiver<()>,
    ) -> Result<Self, ServerOpenError> {
        let server_state = ServerState::new(
            server_info.clone(),
            config.clone(),
            deployment_id,
            shutdown_sig_receiver.clone()
        ).await;
        server_state
            .map(|srv_state| Self {
                server_info,
                config,
                server_state: Arc::new(srv_state),
                shutdown_sig_sender,
                shutdown_sig_receiver
            })
    }

    pub async fn serve(mut self) -> Result<(), ServerOpenError> {
        Self::print_hello(self.server_info, self.config.is_development_mode);
        Self::install_default_encryption_provider()?;

        let grpc_service = grpc::typedb_service::TypeDBService::new(
            todo!(),
            self.server_state,
            shutdown_sig_receiver.clone(),
        );
        let grpc_server = Self::serve_grpc(
            todo!(),
            &self.config.server.encryption,
            self.server_state,
            self.shutdown_receiver.clone(),
            grpc_service,
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

        Self::spawn_shutdown_handler(self.shutdown_sig_sender);

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

    async fn serve_grpc(
        address: SocketAddr,
        credential_verifier: Arc<CredentialVerifier>,
        token_manager: Arc<TokenManager>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        encryption_config: &EncryptionConfig,
        mut shutdown_receiver: Receiver<()>,
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
        mut shutdown_receiver: Receiver<()>,
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

    fn spawn_shutdown_handler(shutdown_signal_sender: tokio::sync::watch::Sender<()>) {
        tokio::spawn(async move {
            Self::wait_for_ctrl_c_signal().await;
            println!("\nReceived CTRL-C. Initiating shutdown...");
            shutdown_signal_sender.send(()).expect("Expected a successful shutdown signal");

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

    // todo: used for test. expose appropriately
    pub fn database_manager(&self) -> &DatabaseManager {
        self.server_state.database_manager()
    }
}
