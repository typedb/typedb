/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use diagnostics::metrics::ActionKind;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::{
    self,
    server_manager::all::{Req, Res},
    transaction::{Client, Server},
};
use uuid::Uuid;

use crate::{
    authentication::{Accessor, AuthenticationError},
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
    },
    state::{BoxServerState, StateError},
};

#[derive(Debug)]
pub(crate) struct TypeDBService {
    address: SocketAddr,
    server_state: Arc<BoxServerState>,
}

impl TypeDBService {
    pub(crate) fn new(address: SocketAddr, server_state: Arc<BoxServerState>) -> Self {
        Self { address, server_state }
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
            self.server_state.diagnostics_manager(),
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
                    Err(err.into_status())
                } else {
                    let Some(authentication) = message.authentication else {
                        return Err(ProtocolError::MissingField {
                            name: "authentication",
                            description: "Connection message must contain authentication information.",
                        }
                        .into_status());
                    };
                    let Some(typedb_protocol::authentication::token::create::req::Credentials::Password(
                        password_credentials,
                    )) = authentication.credentials
                    else {
                        return Err(AuthenticationError::InvalidCredential {}.into_error_message().into_status());
                    };

                    let token = self
                        .server_state
                        .token_create(password_credentials.username, password_credentials.password)
                        .await
                        .map_err(|typedb_source| typedb_source.into_error_message().into_status())?;
                    event!(
                        Level::TRACE,
                        "Successful connection_open from '{}' version '{}'",
                        &message.driver_lang,
                        &message.driver_version
                    );

                    Ok(Response::new(connection_open_res(
                        generate_connection_id(),
                        receive_time,
                        database_all_res(&self.address, self.server_state.databases_all()),
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
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager(),
            None::<&str>,
            ActionKind::SignIn,
            || async {
                let request = request.into_inner();
                let Some(typedb_protocol::authentication::token::create::req::Credentials::Password(
                    password_credentials,
                )) = request.credentials
                else {
                    return Err(AuthenticationError::InvalidCredential {}.into_error_message().into_status());
                };

                self.server_state
                    .token_create(password_credentials.username, password_credentials.password)
                    .await
                    .map(|result| Response::new(token_create_res(result)))
                    .map_err(|typedb_source| typedb_source.into_error_message().into_status())
            },
        )
        .await
    }

    async fn servers_all(&self, _request: Request<Req>) -> Result<Response<Res>, Status> {
        run_with_diagnostics(&self.server_state.diagnostics_manager(), None::<&str>, ActionKind::ServersAll, || {
            Ok(Response::new(servers_all_res(&self.address)))
        })
    }

    async fn databases_all(
        &self,
        _request: Request<typedb_protocol::database_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        run_with_diagnostics(&self.server_state.diagnostics_manager(), None::<&str>, ActionKind::DatabasesAll, || {
            Ok(Response::new(database_all_res(&self.address, self.server_state.databases_all())))
        })
    }

    async fn databases_get(
        &self,
        request: Request<typedb_protocol::database_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::get::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics(
            &self.server_state.diagnostics_manager(),
            Some(name.clone()),
            ActionKind::DatabasesGet,
            || match self.server_state.databases_get(&name) {
                Some(db) => Ok(Response::new(database_get_res(&self.address, db.name().to_string()))),
                None => Err(StateError::DatabaseDoesNotExist { name }.into_error_message().into_status()),
            },
        )
    }

    async fn databases_contains(
        &self,
        request: Request<typedb_protocol::database_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics(
            &self.server_state.diagnostics_manager(),
            Some(name.clone()),
            ActionKind::DatabasesContains,
            || Ok(Response::new(database_contains_res(self.server_state.databases_contains(&name)))),
        )
    }

    async fn databases_create(
        &self,
        request: Request<typedb_protocol::database_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::create::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics(
            &self.server_state.diagnostics_manager(),
            Some(name.clone()),
            ActionKind::DatabasesCreate,
            || {
                self.server_state
                    .databases_create(&name)
                    .map(|_| Response::new(database_create_res(name, &self.address)))
                    .map_err(|err| err.into_error_message().into_status())
            },
        )
    }

    async fn database_schema(
        &self,
        request: Request<typedb_protocol::database::schema::Req>,
    ) -> Result<Response<typedb_protocol::database::schema::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics(
            &self.server_state.diagnostics_manager(),
            Some(name.clone()),
            ActionKind::DatabaseSchema,
            || match self.server_state.database_schema(name) {
                Ok(schema) => Ok(Response::new(database_schema_res(schema))),
                Err(err) => Err(err.into_error_message().into_status()),
            },
        )
    }

    async fn database_type_schema(
        &self,
        request: Request<typedb_protocol::database::type_schema::Req>,
    ) -> Result<Response<typedb_protocol::database::type_schema::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics(
            &self.server_state.diagnostics_manager(),
            Some(name.clone()),
            ActionKind::DatabaseTypeSchema,
            || match self.server_state.database_type_schema(name) {
                Ok(schema) => Ok(Response::new(database_type_schema_res(schema))),
                Err(err) => Err(err.into_error_message().into_status()),
            },
        )
    }

    async fn database_delete(
        &self,
        request: Request<typedb_protocol::database::delete::Req>,
    ) -> Result<Response<typedb_protocol::database::delete::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics(
            &self.server_state.diagnostics_manager(),
            Some(name.clone()),
            ActionKind::DatabaseDelete,
            || {
                self.server_state
                    .database_delete(&name)
                    .map(|_| Response::new(database_delete_res()))
                    .map_err(|err| err.into_error_message().into_status())
            },
        )
    }

    async fn users_get(
        &self,
        request: Request<typedb_protocol::user_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::get::Res>, Status> {
        run_with_diagnostics(&self.server_state.diagnostics_manager(), None::<&str>, ActionKind::UsersGet, || {
            let accessor = Accessor::from_extensions(&request.extensions())
                .map_err(|err| err.into_error_message().into_status())?;
            let name = request.into_inner().name;
            self.server_state
                .users_get(&name, accessor)
                .map(|user| Ok(Response::new(users_get_res(user))))
                .map_err(|err| err.into_error_message().into_status())?
        })
    }

    async fn users_all(
        &self,
        request: Request<typedb_protocol::user_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::all::Res>, Status> {
        run_with_diagnostics(&self.server_state.diagnostics_manager(), None::<&str>, ActionKind::UsersAll, || {
            let accessor = Accessor::from_extensions(&request.extensions())
                .map_err(|err| err.into_error_message().into_status())?;
            self.server_state
                .users_all(accessor)
                .map(|users| Ok(Response::new(users_all_res(users))))
                .map_err(|err| err.into_error_message().into_status())?
        })
    }

    async fn users_contains(
        &self,
        request: Request<typedb_protocol::user_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::contains::Res>, Status> {
        run_with_diagnostics(&self.server_state.diagnostics_manager(), None::<&str>, ActionKind::UsersContains, || {
            let name = request.into_inner().name;
            self.server_state
                .users_contains(name.as_str())
                .map(|contains| Response::new(users_contains_res(contains)))
                .map_err(|err| err.into_error_message().into_status())
        })
    }

    async fn users_create(
        &self,
        request: Request<typedb_protocol::user_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::create::Res>, Status> {
        run_with_diagnostics(&self.server_state.diagnostics_manager(), None::<&str>, ActionKind::UsersCreate, || {
            let accessor = Accessor::from_extensions(&request.extensions())
                .map_err(|err| err.into_error_message().into_status())?;
            let (user, credential) = users_create_req(request).map_err(|err| err.into_error_message().into_status())?;
            self.server_state
                .users_create(&user, &credential, accessor)
                .map(|_| Response::new(user_create_res()))
                .map_err(|err| err.into_error_message().into_status())
        })
    }

    async fn users_update(
        &self,
        request: Request<typedb_protocol::user::update::Req>,
    ) -> Result<Response<typedb_protocol::user::update::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager(),
            None::<&str>,
            ActionKind::UsersUpdate,
            || async {
                let accessor = Accessor::from_extensions(&request.extensions())
                    .map_err(|err| err.into_error_message().into_status())?;
                let (username, user_update, credential_update) =
                    users_update_req(request).map_err(|err| err.into_error_message().into_status())?;
                let username = username.as_str();
                self.server_state
                    .users_update(username, user_update, credential_update, accessor)
                    .await
                    .map(|_| Response::new(user_update_res()))
                    .map_err(|err| err.into_error_message().into_status())
            },
        )
        .await
    }

    async fn users_delete(
        &self,
        request: Request<typedb_protocol::user::delete::Req>,
    ) -> Result<Response<typedb_protocol::user::delete::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager(),
            None::<&str>,
            ActionKind::UsersDelete,
            || async {
                let accessor = Accessor::from_extensions(&request.extensions())
                    .map_err(|err| err.into_error_message().into_status())?;
                let name = request.into_inner().name;
                self.server_state
                    .users_delete(name.as_str(), accessor)
                    .await
                    .map(|_| Response::new(users_delete_res()))
                    .map_err(|err| err.into_error_message().into_status())
            },
        )
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
            self.server_state.database_manager(),
            self.server_state.diagnostics_manager(),
            self.server_state.shutdown_receiver(),
        );
        tokio::spawn(async move { service.listen().await });
        let stream: ReceiverStream<Result<Server, Status>> = ReceiverStream::new(response_receiver);

        Ok(Response::new(Box::pin(stream)))
    }
}

fn generate_connection_id() -> ConnectionID {
    Uuid::new_v4().into_bytes()
}
