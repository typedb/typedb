/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use axum::response::IntoResponse;
use database::{database_manager::DatabaseManager, transaction::TransactionRead};
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind, Diagnostics};
use error::typedb_error;
use http::StatusCode;
use options::TransactionOptions;
use resource::constants::server::DEFAULT_USER_NAME;
use system::concepts::{Credential, PasswordHash, User};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::{
    self,
    server_manager::all::{Req, Res},
    transaction::{Client, Server},
};
use user::{permission_manager::PermissionManager, user_manager::UserManager};
use uuid::Uuid;

use crate::{
    authentication::{
        credential_verifier::CredentialVerifier, token_manager::TokenManager, Accessor, AuthenticationError,
    },
    service::{
        grpc::{
            diagnostics::{run_with_diagnostics, run_with_diagnostics_async},
            error::{IntoGrpcStatus, IntoProtocolErrorMessage, ProtocolError},
            request_parser::{users_create_req, users_update_req},
            response_builders::{
                authentication::token_create_res,
                connection::connection_open_res,
                database::database_delete_res,
                database_manager::{
                    database_all_res, database_contains_res, database_create_res, database_get_res,
                    database_schema_res, database_type_schema_res,
                },
                server_manager::servers_all_res,
                user_manager::{
                    user_create_res, user_update_res, users_all_res, users_contains_res, users_delete_res,
                    users_get_res,
                },
            },
            transaction_service::TransactionService,
            ConnectionID,
        },
        transaction_service::TRANSACTION_REQUEST_BUFFER_SIZE,
        typedb_service::{get_database_schema, get_database_type_schema},
        ServiceError,
    },
};
use crate::service::grpc::state::ServerState;

#[derive(Debug)]
pub(crate) struct TypeDBService {
    server_state: Arc<ServerState>
}

impl TypeDBService {
    pub(crate) fn new(server_state: Arc<ServerState>) -> Self {
        Self { server_state }
    }

    fn generate_connection_id(&self) -> ConnectionID {
        Uuid::new_v4().into_bytes()
    }
}

#[tonic::async_trait]
impl typedb_protocol::type_db_server::TypeDb for TypeDBService {
    // Update AUTHENTICATION_FREE_METHODS if this method is renamed
    async fn connection_open(
        &self,
        request: Request<typedb_protocol::connection::open::Req>,
    ) -> Result<Response<typedb_protocol::connection::open::Res>, Status> {
        self.server_state.connection_open(request).await
    }

    // Update AUTHENTICATION_FREE_METHODS if this method is renamed
    async fn authentication_token_create(
        &self,
        request: Request<typedb_protocol::authentication::token::create::Req>,
    ) -> Result<Response<typedb_protocol::authentication::token::create::Res>, Status> {
        self.server_state.authentication_token_create(request).await
    }

    async fn servers_all(&self, request: Request<Req>) -> Result<Response<Res>, Status> {
        self.server_state.servers_all(request).await
    }

    async fn databases_all(
        &self,
        request: Request<typedb_protocol::database_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        self.server_state.databases_all(request).await
    }

    async fn databases_get(
        &self,
        request: Request<typedb_protocol::database_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::get::Res>, Status> {
        self.server_state.databases_get(request).await
    }

    async fn databases_contains(
        &self,
        request: Request<typedb_protocol::database_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
        self.server_state.databases_contains(request).await
    }

    async fn databases_create(
        &self,
        request: Request<typedb_protocol::database_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::create::Res>, Status> {
        self.server_state.databases_create(request).await
    }

    async fn database_schema(
        &self,
        request: Request<typedb_protocol::database::schema::Req>,
    ) -> Result<Response<typedb_protocol::database::schema::Res>, Status> {
        self.server_state.database_schema(request).await
    }

    async fn database_type_schema(
        &self,
        request: Request<typedb_protocol::database::type_schema::Req>,
    ) -> Result<Response<typedb_protocol::database::type_schema::Res>, Status> {
        self.server_state.database_type_schema(request).await
    }

    async fn database_delete(
        &self,
        request: Request<typedb_protocol::database::delete::Req>,
    ) -> Result<Response<typedb_protocol::database::delete::Res>, Status> {
        self.server_state.database_delete(request).await
    }

    async fn users_get(
        &self,
        request: Request<typedb_protocol::user_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::get::Res>, Status> {
        self.server_state.users_get(request).await
    }

    async fn users_all(
        &self,
        request: Request<typedb_protocol::user_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::all::Res>, Status> {
        self.server_state.users_all(request).await
    }

    async fn users_contains(
        &self,
        request: Request<typedb_protocol::user_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::contains::Res>, Status> {
        self.server_state.users_contains(request).await
    }

    async fn users_create(
        &self,
        request: Request<typedb_protocol::user_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::create::Res>, Status> {
        self.server_state.users_create(request).await
    }

    async fn users_update(
        &self,
        request: Request<typedb_protocol::user::update::Req>,
    ) -> Result<Response<typedb_protocol::user::update::Res>, Status> {
        self.server_state.users_update(request).await
    }

    async fn users_delete(
        &self,
        request: Request<typedb_protocol::user::delete::Req>,
    ) -> Result<Response<typedb_protocol::user::delete::Res>, Status> {
        self.server_state.users_delete(request).await
    }

    type transactionStream = Pin<Box<ReceiverStream<Result<Server, Status>>>>;

    async fn transaction(
        &self,
        request: Request<Streaming<Client>>,
    ) -> Result<Response<Self::transactionStream>, Status> {
        self.server_state.transaction(request).await
    }
}
