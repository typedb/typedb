/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use axum::response::IntoResponse;
use database::database_manager::DatabaseManager;
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind, Diagnostics};
use error::typedb_error;
use http::StatusCode;
use resource::constants::server::DEFAULT_USER_NAME;
use system::concepts::{Credential, PasswordHash, User};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{metadata::MetadataMap, IntoRequest, Request, Response, Status, Streaming};
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
        credential_verifier::CredentialVerifier, extract_metadata_authorization_token,
        extract_parts_authorization_token, token_manager::TokenManager, Accessor, AuthenticationError,
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
                database_manager::{database_all_res, database_contains_res, database_create_res, database_get_res},
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
        ServiceError,
    },
};

#[derive(Debug)]
pub(crate) struct TypeDBService {
    address: SocketAddr,
    database_manager: Arc<DatabaseManager>,
    user_manager: Arc<UserManager>,
    credential_verifier: Arc<CredentialVerifier>,
    token_manager: Arc<TokenManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    shutdown_receiver: tokio::sync::watch::Receiver<()>,
}

impl TypeDBService {
    pub(crate) fn new(
        address: SocketAddr,
        database_manager: Arc<DatabaseManager>,
        user_manager: Arc<UserManager>,
        credential_verifier: Arc<CredentialVerifier>,
        token_manager: Arc<TokenManager>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        shutdown_receiver: tokio::sync::watch::Receiver<()>,
    ) -> Self {
        Self {
            address,
            database_manager,
            user_manager,
            credential_verifier,
            token_manager,
            diagnostics_manager,
            shutdown_receiver,
        }
    }

    pub(crate) fn database_manager(&self) -> &DatabaseManager {
        &self.database_manager
    }

    pub(crate) fn address(&self) -> &SocketAddr {
        &self.address
    }

    fn generate_connection_id(&self) -> ConnectionID {
        Uuid::new_v4().into_bytes()
    }

    async fn process_token_create(
        &self,
        request: typedb_protocol::authentication::token::create::Req,
    ) -> Result<String, AuthenticationError> {
        let Some(typedb_protocol::authentication::token::create::req::Credentials::Password(password_credentials)) =
            request.credentials
        else {
            return Err(AuthenticationError::InvalidCredential {});
        };

        self.credential_verifier.verify_password(&password_credentials.username, &password_credentials.password)?;

        Ok(self.token_manager.new_token(password_credentials.username).await)
    }

    async fn get_request_accessor<T>(&self, request: &Request<T>) -> Result<String, Status> {
        let Accessor(accessor) = Accessor::from_extensions(request.extensions())
            .map_err(|typedb_source| typedb_source.into_error_message().into_status())?;
        Ok(accessor)
    }
}

#[tonic::async_trait]
impl typedb_protocol::type_db_server::TypeDb for TypeDBService {
    // Update AUTHENTICATION_FREE_METHODS if this method is renamed
    async fn connection_open(
        &self,
        request: Request<typedb_protocol::connection::open::Req>,
    ) -> Result<Response<typedb_protocol::connection::open::Res>, Status> {
        run_with_diagnostics_async(
            self.diagnostics_manager.clone(),
            None::<&str>,
            ActionKind::ConnectionOpen,
            || async {
                let receive_time = Instant::now();
                let message = request.into_inner();
                if message.version != typedb_protocol::Version::Version as i32 {
                    let err = ProtocolError::IncompatibleProtocolVersion {
                        server_protocol_version: typedb_protocol::Version::Version as i32,
                        driver_protocol_version: message.version,
                        driver_lang: message.driver_lang.clone(),
                        driver_version: message.driver_version.clone(),
                    };
                    event!(Level::TRACE, "Rejected connection_open: {:?}", &err);
                    return Err(err.into_status());
                } else {
                    let Some(authentication) = message.authentication else {
                        return Err(ProtocolError::MissingField {
                            name: "authentication",
                            description: "Connection message must contain authentication information.",
                        }
                        .into_status());
                    };
                    let token = self
                        .process_token_create(authentication)
                        .await
                        .map_err(|typedb_source| typedb_source.into_error_message().into_status())?;

                    event!(
                        Level::TRACE,
                        "Successful connection_open from '{}' version '{}'",
                        &message.driver_lang,
                        &message.driver_version
                    );

                    Ok(Response::new(connection_open_res(
                        self.generate_connection_id(),
                        receive_time,
                        database_all_res(&self.address, self.database_manager.database_names()),
                        token_create_res(token),
                    )))
                }
            },
        )
        .await
    }

    // Update AUTHENTICATION_FREE_METHODS if this method is renamed
    async fn authentication_token_create(
        &self,
        request: Request<typedb_protocol::authentication::token::create::Req>,
    ) -> Result<Response<typedb_protocol::authentication::token::create::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics_async(self.diagnostics_manager.clone(), None::<&str>, ActionKind::SignIn, || async {
            self.process_token_create(message)
                .await
                .map(|result| Response::new(token_create_res(result)))
                .map_err(|typedb_source| typedb_source.into_error_message().into_status())
        })
        .await
    }

    async fn servers_all(&self, _request: Request<Req>) -> Result<Response<Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::ServersAll, || {
            Ok(Response::new(servers_all_res(&self.address)))
        })
    }

    async fn databases_all(
        &self,
        _request: Request<typedb_protocol::database_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::DatabasesAll, || {
            Ok(Response::new(database_all_res(&self.address, self.database_manager.database_names())))
        })
    }

    async fn databases_get(
        &self,
        request: Request<typedb_protocol::database_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::get::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics(&self.diagnostics_manager, Some(message.name.clone()), ActionKind::DatabasesGet, || {
            let database = self.database_manager.database(&message.name);
            match database {
                None => {
                    Err(ServiceError::DatabaseDoesNotExist { name: message.name }.into_error_message().into_status())
                }
                Some(_database) => Ok(Response::new(database_get_res(&self.address, message.name))),
            }
        })
    }

    async fn databases_contains(
        &self,
        request: Request<typedb_protocol::database_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics(&self.diagnostics_manager, Some(&message.name), ActionKind::DatabasesContains, || {
            Ok(Response::new(database_contains_res(self.database_manager.database(&message.name).is_some())))
        })
    }

    async fn databases_create(
        &self,
        request: Request<typedb_protocol::database_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::create::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics(&self.diagnostics_manager, Some(message.name.clone()), ActionKind::DatabasesCreate, || {
            self.database_manager
                .create_database(message.name.clone())
                .map(|_| Response::new(database_create_res(message.name, &self.address)))
                .map_err(|err| err.into_error_message().into_status())
        })
    }

    async fn database_schema(
        &self,
        request: Request<typedb_protocol::database::schema::Req>,
    ) -> Result<Response<typedb_protocol::database::schema::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics(&self.diagnostics_manager, Some(&message.name), ActionKind::DatabaseSchema, || {
            Err(ServiceError::Unimplemented { description: "Database schema retrieval.".to_string() }
                .into_error_message()
                .into_status())
        })
    }

    async fn database_type_schema(
        &self,
        request: Request<typedb_protocol::database::type_schema::Req>,
    ) -> Result<Response<typedb_protocol::database::type_schema::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics(&self.diagnostics_manager, Some(&message.name), ActionKind::DatabaseTypeSchema, || {
            Err(ServiceError::Unimplemented { description: "Database schema (types only) retrieval.".to_string() }
                .into_error_message()
                .into_status())
        })
    }

    async fn database_delete(
        &self,
        request: Request<typedb_protocol::database::delete::Req>,
    ) -> Result<Response<typedb_protocol::database::delete::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics(&self.diagnostics_manager, Some(message.name.clone()), ActionKind::DatabaseDelete, || {
            self.database_manager
                .delete_database(message.name)
                .map(|_| Response::new(database_delete_res()))
                .map_err(|err| err.into_error_message().into_status())
        })
    }

    async fn users_get(
        &self,
        request: Request<typedb_protocol::user_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::get::Res>, Status> {
        run_with_diagnostics_async(self.diagnostics_manager.clone(), None::<&str>, ActionKind::UsersGet, || async {
            let accessor = self.get_request_accessor(&request).await?;
            let get_req = request.into_inner();
            if !PermissionManager::exec_user_get_permitted(accessor.as_str(), get_req.name.as_str()) {
                return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
            }
            match self.user_manager.get(get_req.name.as_str()) {
                Ok(get_result) => match get_result {
                    Some((user, _)) => Ok(Response::new(users_get_res(user))),
                    None => Err(ServiceError::UserDoesNotExist {}.into_error_message().into_status()),
                },
                Err(user_get_error) => Err(user_get_error.into_error_message().into_status()),
            }
        })
        .await
    }

    async fn users_all(
        &self,
        request: Request<typedb_protocol::user_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::all::Res>, Status> {
        run_with_diagnostics_async(self.diagnostics_manager.clone(), None::<&str>, ActionKind::UsersAll, || async {
            let accessor = self.get_request_accessor(&request).await?;
            if !PermissionManager::exec_user_all_permitted(accessor.as_str()) {
                return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
            }
            let users = self.user_manager.all();
            Ok(Response::new(users_all_res(users)))
        })
        .await
    }

    async fn users_contains(
        &self,
        request: Request<typedb_protocol::user_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::contains::Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersContains, || {
            let contains_req = request.into_inner();
            self.user_manager
                .contains(contains_req.name.as_str())
                .map(|contains| Response::new(users_contains_res(contains)))
                .map_err(|err| err.into_error_message().into_status())
        })
    }

    async fn users_create(
        &self,
        request: Request<typedb_protocol::user_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::create::Res>, Status> {
        run_with_diagnostics_async(self.diagnostics_manager.clone(), None::<&str>, ActionKind::UsersCreate, || async {
            let accessor = self.get_request_accessor(&request).await?;
            if !PermissionManager::exec_user_create_permitted(accessor.as_str()) {
                return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
            }
            users_create_req(request)
                .and_then(|(usr, cred)| self.user_manager.create(&usr, &cred))
                .map(|_| Response::new(user_create_res()))
                .map_err(|err| err.into_error_message().into_status())
        })
        .await
    }

    async fn users_update(
        &self,
        request: Request<typedb_protocol::user::update::Req>,
    ) -> Result<Response<typedb_protocol::user::update::Res>, Status> {
        run_with_diagnostics_async(self.diagnostics_manager.clone(), None::<&str>, ActionKind::UsersUpdate, || async {
            let accessor = self.get_request_accessor(&request).await?;
            let (username, user_update, credential_update) =
                users_update_req(request).map_err(|typedb_source| typedb_source.into_error_message().into_status())?;
            let username = username.as_str();
            if !PermissionManager::exec_user_update_permitted(accessor.as_str(), username) {
                return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
            }
            self.user_manager
                .update(username, &user_update, &credential_update)
                .map_err(|typedb_source| typedb_source.into_error_message().into_status())?;
            self.token_manager.invalidate_user(username).await;
            Ok(Response::new(user_update_res()))
        })
        .await
    }

    async fn users_delete(
        &self,
        request: Request<typedb_protocol::user::delete::Req>,
    ) -> Result<Response<typedb_protocol::user::delete::Res>, Status> {
        run_with_diagnostics_async(self.diagnostics_manager.clone(), None::<&str>, ActionKind::UsersDelete, || async {
            let accessor = self.get_request_accessor(&request).await?;
            let delete_req = request.into_inner();
            let username = delete_req.name.as_str();
            if !PermissionManager::exec_user_delete_allowed(accessor.as_str(), username) {
                return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
            }
            self.user_manager
                .delete(username)
                .map_err(|typedb_source| typedb_source.into_error_message().into_status())?;
            self.token_manager.invalidate_user(username).await;
            Ok(Response::new(users_delete_res()))
        })
        .await
    }

    type transactionStream = Pin<Box<ReceiverStream<Result<Server, Status>>>>;

    async fn transaction(
        &self,
        request: Request<Streaming<Client>>,
    ) -> Result<Response<Self::transactionStream>, Status> {
        let request_stream = request.into_inner();
        let (response_sender, response_receiver) = channel(TRANSACTION_REQUEST_BUFFER_SIZE);
        let mut service = TransactionService::new(
            request_stream,
            response_sender,
            self.database_manager.clone(),
            self.diagnostics_manager.clone(),
            self.shutdown_receiver.clone(),
        );
        tokio::spawn(async move { service.listen().await });
        let stream: ReceiverStream<Result<Server, Status>> = ReceiverStream::new(response_receiver);
        Ok(Response::new(Box::pin(stream)))
    }
}
