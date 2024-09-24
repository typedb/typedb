/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use database::database_manager::DatabaseManager;
use error::typedb_error;
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

use crate::service::{
    error::{IntoGRPCStatus, IntoProtocolErrorMessage, ProtocolError},
    response_builders::{
        connection::connection_open_res,
        database::database_delete_res,
        database_manager::{database_all_res, database_contains_res, database_create_res, database_get_res},
        server_manager::servers_all_res,
    },
    transaction_service::TransactionService,
    ConnectionID,
};

#[derive(Debug)]
pub(crate) struct TypeDBService {
    address: SocketAddr,
    database_manager: Arc<DatabaseManager>,
    // map of connection ID to ConnectionService
}

impl TypeDBService {
    pub(crate) fn new(address: &SocketAddr, database_manager: DatabaseManager) -> Self {
        Self { address: address.clone(), database_manager: Arc::new(database_manager) }
    }

    pub(crate) fn database_manager(&self) -> &DatabaseManager {
        &self.database_manager
    }

    fn generate_connection_id(&self) -> ConnectionID {
        Uuid::new_v4().into_bytes()
    }
}

#[tonic::async_trait]
impl typedb_protocol::type_db_server::TypeDb for TypeDBService {
    async fn connection_open(
        &self,
        request: Request<typedb_protocol::connection::open::Req>,
    ) -> Result<Response<typedb_protocol::connection::open::Res>, Status> {
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
            event!(Level::TRACE, "Successful connection_open from '{}' version '{}'", &message.driver_lang, &message.driver_version);
            // generate a connection ID per 'connection_open' to be able to trace different connections by the same user
            Ok(Response::new(connection_open_res(
                self.generate_connection_id(),
                receive_time,
                database_all_res(&self.address, self.database_manager.database_names()),
            )))
        }
    }

    async fn servers_all(&self, _request: Request<Req>) -> Result<Response<Res>, Status> {
        Ok(Response::new(servers_all_res(&self.address)))
    }

    async fn databases_get(
        &self,
        request: Request<typedb_protocol::database_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::get::Res>, Status> {
        let message = request.into_inner();
        let database = self.database_manager.database(&message.name);
        match database {
            None => Err(ServiceError::DatabaseDoesNotExist { name: message.name }.into_error_message().into_status()),
            Some(_database) => Ok(Response::new(database_get_res(&self.address, message.name))),
        }
    }

    async fn databases_all(
        &self,
        _request: Request<typedb_protocol::database_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        Ok(Response::new(database_all_res(&self.address, self.database_manager.database_names())))
    }

    async fn databases_contains(
        &self,
        request: Request<typedb_protocol::database_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
        let message = request.into_inner();
        Ok(Response::new(database_contains_res(self.database_manager.database(&message.name).is_some())))
    }

    async fn databases_create(
        &self,
        request: Request<typedb_protocol::database_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::create::Res>, Status> {
        let message = request.into_inner();
        self.database_manager.create_database(message.name.clone());
        Ok(Response::new(database_create_res(message.name, &self.address)))
    }

    async fn database_schema(
        &self,
        _request: Request<typedb_protocol::database::schema::Req>,
    ) -> Result<Response<typedb_protocol::database::schema::Res>, Status> {
        Err(ServiceError::Unimplemented { description: "Database schema retrieval.".to_string() }
            .into_error_message()
            .into_status())
    }

    async fn database_type_schema(
        &self,
        _request: Request<typedb_protocol::database::type_schema::Req>,
    ) -> Result<Response<typedb_protocol::database::type_schema::Res>, Status> {
        Err(ServiceError::Unimplemented { description: "Database schema (types only) retrieval.".to_string() }
            .into_error_message()
            .into_status())
    }

    async fn database_delete(
        &self,
        request: Request<typedb_protocol::database::delete::Req>,
    ) -> Result<Response<typedb_protocol::database::delete::Res>, Status> {
        let message = request.into_inner();
        self.database_manager
            .delete_database(message.name)
            .map(|_| Response::new(database_delete_res()))
            .map_err(|err| err.into_error_message().into_status())
    }

    type transactionStream = Pin<Box<ReceiverStream<Result<typedb_protocol::transaction::Server, tonic::Status>>>>;

    async fn transaction(
        &self,
        request: Request<Streaming<Client>>,
    ) -> Result<Response<Self::transactionStream>, Status> {
        let request_stream = request.into_inner();
        let (response_sender, response_receiver) = channel(10);
        let mut service = TransactionService::new(request_stream, response_sender, self.database_manager.clone());
        tokio::spawn(async move { service.listen().await });
        let stream: ReceiverStream<Result<Server, Status>> = ReceiverStream::new(response_receiver);
        Ok(Response::new(Box::pin(stream)))
    }
}

typedb_error!(
    ServiceError(component = "Server", prefix = "SRV") {
        Unimplemented(1, "Not implemented: {description}", description: String),
        DatabaseDoesNotExist(2, "Database '{name}' does not exist.", name: String),
    }
);
