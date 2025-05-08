/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

<<<<<<< HEAD
use std::sync::Arc;

use database::{transaction::TransactionRead, Database};
use options::TransactionOptions;
use storage::durability_client::DurabilityClient;

use crate::service::ServiceError;

pub(crate) fn get_database_schema<D: DurabilityClient>(database: Arc<Database<D>>) -> Result<String, ServiceError> {
    let transaction = TransactionRead::open(database, TransactionOptions::default())
        .map_err(|err| ServiceError::FailedToOpenPrerequisiteTransaction {})?;
    let types_syntax = get_types_syntax(&transaction)?;
    let functions_syntax = get_functions_syntax(&transaction)?;

    let schema = match types_syntax.is_empty() & functions_syntax.is_empty() {
        true => String::new(),
        false => format!("{}\n{} {}", typeql::token::Clause::Define, types_syntax, functions_syntax),
    };
    Ok(schema)
}

pub(crate) fn get_database_type_schema<D: DurabilityClient>(
    database: Arc<Database<D>>,
) -> Result<String, ServiceError> {
    let transaction = TransactionRead::open(database, TransactionOptions::default())
        .map_err(|err| ServiceError::FailedToOpenPrerequisiteTransaction {})?;
    let types_syntax = get_types_syntax(&transaction)?;

    let type_schema = match types_syntax.is_empty() {
        true => String::new(),
        false => format!("{}\n{}", typeql::token::Clause::Define, types_syntax),
    };
    Ok(type_schema)
}

fn get_types_syntax<D: DurabilityClient>(transaction: &TransactionRead<D>) -> Result<String, ServiceError> {
    transaction
        .type_manager
        .get_types_syntax(transaction.snapshot())
        .map_err(|err| ServiceError::ConceptReadError { typedb_source: err })
}

fn get_functions_syntax<D: DurabilityClient>(transaction: &TransactionRead<D>) -> Result<String, ServiceError> {
    transaction
        .function_manager
        .get_functions_syntax(transaction.snapshot())
        .map_err(|err| ServiceError::FunctionReadError { typedb_source: err })
=======
use std::{pin::Pin, sync::Arc};

use database::database_manager::DatabaseManager;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use typedb_protocol::{
    self,
    server_manager::all::{Req, Res},
    transaction::{Client, Server},
};

use crate::service::state::ServerState;

#[derive(Debug)]
pub(crate) struct TypeDBService {
    server_state: Arc<ServerState>
}

impl TypeDBService {
    pub(crate) fn new(server_state: Arc<ServerState>) -> Self {
        Self { server_state }
    }

    pub(crate) fn database_manager(&self) -> &DatabaseManager {
        &self.server_state.database_manager()
    }
}

#[tonic::async_trait]
impl typedb_protocol::type_db_server::TypeDb for TypeDBService {
    async fn connection_open(
        &self,
        request: Request<typedb_protocol::connection::open::Req>,
    ) -> Result<Response<typedb_protocol::connection::open::Res>, Status> {
        self.server_state.open_connection(request).await
    }

    async fn servers_all(&self, request: Request<Req>) -> Result<Response<Res>, Status> {
        self.server_state.list_servers(request).await
    }

    async fn databases_all(
        &self,
        request: Request<typedb_protocol::database_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        self.server_state.list_databases(request).await
    }

    async fn databases_get(
        &self,
        request: Request<typedb_protocol::database_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::get::Res>, Status> {
        self.server_state.get_database(request).await
    }

    async fn databases_contains(
        &self,
        request: Request<typedb_protocol::database_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
        self.server_state.database_exists(request).await
    }

    async fn databases_create(
        &self,
        request: Request<typedb_protocol::database_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::create::Res>, Status> {
        self.server_state.create_database(request).await
    }

    async fn database_schema(
        &self,
        request: Request<typedb_protocol::database::schema::Req>,
    ) -> Result<Response<typedb_protocol::database::schema::Res>, Status> {
        self.server_state.get_database_schema(request).await
    }

    async fn database_type_schema(
        &self,
        request: Request<typedb_protocol::database::type_schema::Req>,
    ) -> Result<Response<typedb_protocol::database::type_schema::Res>, Status> {
        self.server_state.list_database_schema_types(request).await
    }

    async fn database_delete(
        &self,
        request: Request<typedb_protocol::database::delete::Req>,
    ) -> Result<Response<typedb_protocol::database::delete::Res>, Status> {
        self.server_state.delete_database(request).await
    }

    async fn users_get(
        &self,
        request: Request<typedb_protocol::user_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::get::Res>, Status> {
        self.server_state.get_user(request).await
    }

    async fn users_all(
        &self,
        request: Request<typedb_protocol::user_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::all::Res>, Status> {
        self.server_state.list_users(request).await
    }

    async fn users_contains(
        &self,
        request: Request<typedb_protocol::user_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::contains::Res>, Status> {
        self.server_state.user_exists(request).await
    }

    async fn users_create(
        &self,
        request: Request<typedb_protocol::user_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::create::Res>, Status> {
        self.server_state.create_users(request).await
    }

    async fn users_update(
        &self,
        request: Request<typedb_protocol::user::update::Req>,
    ) -> Result<Response<typedb_protocol::user::update::Res>, Status> {
        self.server_state.update_user(request).await
    }

    async fn users_delete(
        &self,
        request: Request<typedb_protocol::user::delete::Req>,
    ) -> Result<Response<typedb_protocol::user::delete::Res>, Status> {
        self.server_state.delete_user(request).await
    }

    type transactionStream = Pin<Box<ReceiverStream<Result<Server, Status>>>>;

    async fn transaction(
        &self,
        request: Request<Streaming<Client>>,
    ) -> Result<Response<Self::transactionStream>, Status> {
        self.server_state.open_transaction(request).await
    }
>>>>>>> extend-typedb-service-v3
}
