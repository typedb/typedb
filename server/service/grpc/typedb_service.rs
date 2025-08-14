/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use diagnostics::metrics::ActionKind;
use itertools::Itertools;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::{
    self,
    database::export::Server as DatabaseExportServerProto,
    database_manager::import::Server as DatabasesImportServerProto,
    transaction::{Client as TransactionClientProto, Server as TransactionServerProto},
};
use uuid::Uuid;

use crate::{
    authentication::{Accessor, AuthenticationError},
    service::{
        grpc::{
            diagnostics::{run_with_diagnostics, run_with_diagnostics_async},
            error::{GrpcServiceError, IntoGrpcStatus, IntoProtocolErrorMessage, ProtocolError},
            migration::{
                export_service::{DatabaseExportService, DATABASE_EXPORT_REQUEST_BUFFER_SIZE},
                import_service::{DatabaseImportService, IMPORT_RESPONSE_BUFFER_SIZE},
            },
            request_parser::{users_create_req, users_update_req},
            response_builders::{
                authentication::token_create_res,
                connection::connection_open_res,
                database::{database_delete_res, database_schema_res, database_type_schema_res},
                database_manager::{database_all_res, database_contains_res, database_create_res, database_get_res},
                server::{server_version_res, servers_deregister_res, servers_register_res},
                server_manager::{servers_all_res, servers_get_res},
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
    state::{ArcServerState, ServerStateError},
};

#[derive(Debug)]
pub(crate) struct TypeDBService {
    address: SocketAddr,
    server_state: ArcServerState,
}

impl TypeDBService {
    pub(crate) fn new(address: SocketAddr, server_state: ArcServerState) -> Self {
        Self { address, server_state }
    }

    async fn servers_statuses(&self) -> Result<Vec<typedb_protocol::Server>, Status> {
        let statuses = self
            .server_state
            .servers_statuses()
            .await
            .map_err(|typedb_source| typedb_source.into_error_message().into_status())?;
        Ok(statuses.into_iter().map(|status| status.to_proto()).collect())
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
            self.server_state.diagnostics_manager().await,
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
                        servers_all_res(self.servers_statuses().await?),
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
            self.server_state.diagnostics_manager().await,
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

    async fn server_version(
        &self,
        _request: Request<typedb_protocol::server::version::Req>,
    ) -> Result<Response<typedb_protocol::server::version::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::ServerVersion,
            || async { Ok(Response::new(server_version_res(self.server_state.distribution_info().await))) },
        )
        .await
    }

    async fn servers_all(
        &self,
        _request: Request<typedb_protocol::server_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::server_manager::all::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::ServersAll,
            || async { Ok(Response::new(servers_all_res(self.servers_statuses().await?))) },
        )
        .await
    }

    async fn servers_get(
        &self,
        _request: Request<typedb_protocol::server_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::server_manager::get::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::ServersGet,
            || async {
                let status = self
                    .server_state
                    .server_status()
                    .await
                    .map_err(|typedb_source| typedb_source.into_error_message().into_status())?;
                Ok(Response::new(servers_get_res(status.to_proto())))
            },
        )
        .await
    }

    async fn servers_register(
        &self,
        request: Request<typedb_protocol::server_manager::register::Req>,
    ) -> Result<Response<typedb_protocol::server_manager::register::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::ServersRegister,
            || async {
                let request = request.into_inner();

                let typedb_protocol::server_manager::register::Req { address, replica_id } = request else {
                    return Err(ProtocolError::MissingField {
                        name: "req",
                        description: "No server information provided.",
                    }
                    .into_status());
                };
                self.server_state
                    .servers_register(replica_id, address)
                    .await
                    .map(|()| Response::new(servers_register_res()))
                    .map_err(|typedb_source| typedb_source.into_error_message().into_status())
            },
        )
        .await
    }

    async fn servers_deregister(
        &self,
        request: Request<typedb_protocol::server_manager::deregister::Req>,
    ) -> Result<Response<typedb_protocol::server_manager::deregister::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::ServersDeregister,
            || async {
                let request = request.into_inner();
                self.server_state
                    .servers_deregister(request.replica_id)
                    .await
                    .map(|()| Response::new(servers_deregister_res()))
                    .map_err(|typedb_source| typedb_source.into_error_message().into_status())
            },
        )
        .await
    }

    async fn databases_all(
        &self,
        _request: Request<typedb_protocol::database_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::DatabasesAll,
            || async {
                self.server_state
                    .databases_all()
                    .await
                    .map(|dbs| Response::new(database_all_res(dbs)))
                    .map_err(|e| e.into_error_message().into_status())
            },
        )
        .await
    }

    async fn databases_get(
        &self,
        request: Request<typedb_protocol::database_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::get::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            Some(name.clone()),
            ActionKind::DatabasesGet,
            || async {
                match self.server_state.databases_get(&name).await {
                    Some(db) => Ok(Response::new(database_get_res(db.name().to_string()))),
                    None => Err(ServerStateError::DatabaseNotFound { name }.into_error_message().into_status()),
                }
            },
        )
        .await
    }

    async fn databases_contains(
        &self,
        request: Request<typedb_protocol::database_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            Some(name.clone()),
            ActionKind::DatabasesContains,
            || async { Ok(Response::new(database_contains_res(self.server_state.databases_contains(&name).await))) },
        )
        .await
    }

    async fn databases_create(
        &self,
        request: Request<typedb_protocol::database_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::create::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            Some(name.clone()),
            ActionKind::DatabasesCreate,
            || async {
                self.server_state
                    .databases_create(&name)
                    .await
                    .map(|_| Response::new(database_create_res(name)))
                    .map_err(|err| err.into_error_message().into_status())
            },
        )
        .await
    }

    type databases_importStream = Pin<Box<ReceiverStream<Result<DatabasesImportServerProto, Status>>>>;

    async fn databases_import(
        &self,
        request: Request<Streaming<typedb_protocol::database_manager::import::Client>>,
    ) -> Result<Response<Self::databases_importStream>, Status> {
        // diagnostics are inside the service
        let request_stream = request.into_inner();
        let (response_sender, response_receiver) = channel(IMPORT_RESPONSE_BUFFER_SIZE);
        let service = DatabaseImportService::new(
            self.server_state.database_manager().await,
            self.server_state.diagnostics_manager().await,
            request_stream,
            response_sender,
            self.server_state.shutdown_receiver().await,
        );
        tokio::spawn(async move { service.listen().await });
        let stream: ReceiverStream<Result<DatabasesImportServerProto, Status>> = ReceiverStream::new(response_receiver);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn database_schema(
        &self,
        request: Request<typedb_protocol::database::schema::Req>,
    ) -> Result<Response<typedb_protocol::database::schema::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            Some(name.clone()),
            ActionKind::DatabaseSchema,
            || async {
                match self.server_state.database_schema(name).await {
                    Ok(schema) => Ok(Response::new(database_schema_res(schema))),
                    Err(err) => Err(err.into_error_message().into_status()),
                }
            },
        )
        .await
    }

    async fn database_type_schema(
        &self,
        request: Request<typedb_protocol::database::type_schema::Req>,
    ) -> Result<Response<typedb_protocol::database::type_schema::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            Some(name.clone()),
            ActionKind::DatabaseTypeSchema,
            || async {
                match self.server_state.database_type_schema(name).await {
                    Ok(schema) => Ok(Response::new(database_type_schema_res(schema))),
                    Err(err) => Err(err.into_error_message().into_status()),
                }
            },
        )
        .await
    }

    type database_exportStream = Pin<Box<ReceiverStream<Result<DatabaseExportServerProto, Status>>>>;

    async fn database_export(
        &self,
        request: Request<typedb_protocol::database::export::Req>,
    ) -> Result<Response<Self::database_exportStream>, Status> {
        let database_name = request
            .into_inner()
            .req
            .ok_or_else(|| {
                GrpcServiceError::UnexpectedMissingField { field: "req".to_string() }.into_error_message().into_status()
            })?
            .name;
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            Some(database_name.clone()),
            ActionKind::DatabaseExport,
            || async {
                match self.server_state.database_manager().await.database(&database_name) {
                    None => Err(ServerStateError::DatabaseNotFound { name: database_name }
                        .into_error_message()
                        .into_status()),
                    Some(database) => {
                        let (response_sender, response_receiver) = channel(DATABASE_EXPORT_REQUEST_BUFFER_SIZE);
                        let service = DatabaseExportService::new(
                            self.server_state.distribution_info().await,
                            database,
                            response_sender,
                            self.server_state.shutdown_receiver().await,
                        );
                        tokio::spawn(async move { service.export().await });
                        let stream: ReceiverStream<Result<DatabaseExportServerProto, Status>> =
                            ReceiverStream::new(response_receiver);
                        Ok(Response::new(Box::pin(stream)))
                    }
                }
            },
        )
        .await
    }

    async fn database_delete(
        &self,
        request: Request<typedb_protocol::database::delete::Req>,
    ) -> Result<Response<typedb_protocol::database::delete::Res>, Status> {
        let name = request.into_inner().name;
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            Some(name.clone()),
            ActionKind::DatabaseDelete,
            || async {
                self.server_state
                    .database_delete(&name)
                    .await
                    .map(|_| Response::new(database_delete_res()))
                    .map_err(|err| err.into_error_message().into_status())
            },
        )
        .await
    }

    async fn users_get(
        &self,
        request: Request<typedb_protocol::user_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::get::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::UsersGet,
            || async {
                let accessor = Accessor::from_extensions(&request.extensions())
                    .map_err(|err| err.into_error_message().into_status())?;
                let name = request.into_inner().name;
                self.server_state
                    .users_get(&name, accessor)
                    .await
                    .map(|user| Ok(Response::new(users_get_res(user))))
                    .map_err(|err| err.into_error_message().into_status())?
            },
        )
        .await
    }

    async fn users_all(
        &self,
        request: Request<typedb_protocol::user_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::all::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::UsersAll,
            || async {
                let accessor = Accessor::from_extensions(&request.extensions())
                    .map_err(|err| err.into_error_message().into_status())?;
                self.server_state
                    .users_all(accessor)
                    .await
                    .map(|users| Ok(Response::new(users_all_res(users))))
                    .map_err(|err| err.into_error_message().into_status())?
            },
        )
        .await
    }

    async fn users_contains(
        &self,
        request: Request<typedb_protocol::user_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::contains::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::UsersContains,
            || async {
                let name = request.into_inner().name;
                self.server_state
                    .users_contains(name.as_str())
                    .await
                    .map(|contains| Response::new(users_contains_res(contains)))
                    .map_err(|err| err.into_error_message().into_status())
            },
        )
        .await
    }

    async fn users_create(
        &self,
        request: Request<typedb_protocol::user_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::create::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::UsersCreate,
            || async {
                let accessor = Accessor::from_extensions(&request.extensions())
                    .map_err(|err| err.into_error_message().into_status())?;
                let (user, credential) =
                    users_create_req(request).map_err(|err| err.into_error_message().into_status())?;
                self.server_state
                    .users_create(&user, &credential, accessor)
                    .await
                    .map(|_| Response::new(user_create_res()))
                    .map_err(|err| err.into_error_message().into_status())
            },
        )
        .await
    }

    async fn users_update(
        &self,
        request: Request<typedb_protocol::user::update::Req>,
    ) -> Result<Response<typedb_protocol::user::update::Res>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
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
            self.server_state.diagnostics_manager().await,
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

    type transactionStream = Pin<Box<ReceiverStream<Result<TransactionServerProto, Status>>>>;

    async fn transaction(
        &self,
        request: Request<Streaming<TransactionClientProto>>,
    ) -> Result<Response<Self::transactionStream>, Status> {
        let request_stream = request.into_inner();
        let (response_sender, response_receiver) = channel(TRANSACTION_REQUEST_BUFFER_SIZE);
        let mut service = TransactionService::new(self.server_state.clone(), request_stream, response_sender);
        tokio::spawn(async move { service.listen().await });
        let stream: ReceiverStream<Result<TransactionServerProto, Status>> = ReceiverStream::new(response_receiver);
        Ok(Response::new(Box::pin(stream)))
    }
}

fn generate_connection_id() -> ConnectionID {
    Uuid::new_v4().into_bytes()
}
