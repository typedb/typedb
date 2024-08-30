/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{pin::Pin, sync::Arc};

use database::database_manager::DatabaseManager;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use typedb_protocol::{
    self,
    connection::pulse::{Req, Res},
    transaction::{Client, Server},
};

use crate::service::transaction_service::TransactionService;

#[derive(Debug)]
pub(crate) struct TypeDBService {
    database_manager: Arc<DatabaseManager>,
    // map of connection ID to ConnectionService
}

impl TypeDBService {
    pub(crate) fn new(database_manager: DatabaseManager) -> Self {
        Self { database_manager: Arc::new(database_manager) }
    }

    pub(crate) fn database_manager(&self) -> &DatabaseManager {
        &self.database_manager
    }
}

#[tonic::async_trait]
impl typedb_protocol::type_db_server::TypeDb for TypeDBService {
    async fn connection_open(
        &self,
        request: Request<typedb_protocol::connection::open::Req>,
    ) -> Result<Response<typedb_protocol::connection::open::Res>, Status> {
        todo!()
    }

    async fn connection_pulse(&self, request: Request<Req>) -> Result<Response<Res>, Status> {
        todo!()
    }

    async fn databases_get(
        &self,
        request: Request<typedb_protocol::database_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::get::Res>, Status> {
        todo!()
    }

    async fn databases_all(
        &self,
        request: Request<typedb_protocol::database_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        todo!()
    }

    async fn databases_contains(
        &self,
        request: Request<typedb_protocol::database_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
        todo!()
    }

    async fn databases_create(
        &self,
        request: Request<typedb_protocol::database_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::create::Res>, Status> {
        todo!()
    }

    async fn database_schema(
        &self,
        request: Request<typedb_protocol::database::schema::Req>,
    ) -> Result<Response<typedb_protocol::database::schema::Res>, Status> {
        todo!()
    }

    async fn database_type_schema(
        &self,
        request: Request<typedb_protocol::database::type_schema::Req>,
    ) -> Result<Response<typedb_protocol::database::type_schema::Res>, Status> {
        todo!()
    }

    async fn database_delete(
        &self,
        request: Request<typedb_protocol::database::delete::Req>,
    ) -> Result<Response<typedb_protocol::database::delete::Res>, Status> {
        todo!()
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
