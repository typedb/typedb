/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use typedb_protocol;
use typedb_protocol::connection::pulse::{Req, Res};
use typedb_protocol::transaction::Client;

#[derive(Debug, Default)]
pub(crate) struct TypeDBService {

}

#[tonic::async_trait]
impl typedb_protocol::type_db_server::TypeDb for TypeDBService {
    async fn connection_open(&self, request: Request<typedb_protocol::connection::open::Req>) -> Result<Response<typedb_protocol::connection::open::Res>, Status> {
        todo!()
    }

    async fn connection_pulse(&self, request: Request<Req>) -> Result<Response<Res>, Status> {
        todo!()
    }

    async fn databases_get(&self, request: Request<typedb_protocol::database_manager::get::Req>) -> Result<Response<typedb_protocol::database_manager::get::Res>, Status> {
        todo!()
    }

    async fn databases_all(&self, request: Request<typedb_protocol::database_manager::all::Req>) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        todo!()
    }

    async fn databases_contains(&self, request: Request<typedb_protocol::database_manager::contains::Req>) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
        todo!()
    }

    async fn databases_create(&self, request: Request<typedb_protocol::database_manager::create::Req>) -> Result<Response<typedb_protocol::database_manager::create::Res>, Status> {
        todo!()
    }

    async fn database_schema(&self, request: Request<typedb_protocol::database::schema::Req>) -> Result<Response<typedb_protocol::database::schema::Res>, Status> {
        todo!()
    }

    async fn database_type_schema(&self, request: Request<typedb_protocol::database::type_schema::Req>) -> Result<Response<typedb_protocol::database::type_schema::Res>, Status> {
        todo!()
    }

    async fn database_delete(&self, request: Request<typedb_protocol::database::delete::Req>) -> Result<Response<typedb_protocol::database::delete::Res>, Status> {
        todo!()
    }

    type transactionStream = ();

    async fn transaction(&self, request: Request<Streaming<Client>>) -> Result<Response<Self::transactionStream>, Status> {
        let mut stream = request.into_inner();

        stream.next().await;
    }
}
