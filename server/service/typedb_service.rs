/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt::format, net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use database::database_manager::DatabaseManager;
use error::typedb_error;
use resource::constants::server::{AUTHENTICATOR_USERNAME_FIELD, DEFAULT_USER_NAME};
use system::concepts::{Credential, PasswordHash, User};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::{
    self,
    server_manager::all::{Req, Res},
    transaction::{Client, Server},
};
use user::{
    errors::{UserCreateError, UserUpdateError},
    permission_manager::PermissionManager,
    user_manager::UserManager,
};
use uuid::Uuid;

use crate::{
    authenticator_cache::AuthenticatorCache,
    service::{
        error::{IntoGRPCStatus, IntoProtocolErrorMessage, ProtocolError},
        request_parser::{users_create_req, users_update_req},
        response_builders::{
            connection::connection_open_res,
            database::database_delete_res,
            database_manager::{database_all_res, database_contains_res, database_create_res, database_get_res},
            server_manager::servers_all_res,
            user_manager::{
                user_create_res, user_update_res, users_all_res, users_contains_res, users_delete_res, users_get_res,
            },
        },
        transaction_service::TransactionService,
        ConnectionID,
    },
};

#[derive(Debug)]
pub(crate) struct TypeDBService {
    address: SocketAddr,
    database_manager: Arc<DatabaseManager>,
    user_manager: Arc<UserManager>,
    authenticator_cache: Arc<AuthenticatorCache>,
}

impl TypeDBService {
    pub(crate) fn new(
        address: &SocketAddr,
        database_manager: DatabaseManager,
        user_manager: Arc<UserManager>,
        authenticator_cache: Arc<AuthenticatorCache>,
    ) -> Self {
        Self { address: *address, database_manager: Arc::new(database_manager), user_manager, authenticator_cache }
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
            event!(
                Level::TRACE,
                "Successful connection_open from '{}' version '{}'",
                &message.driver_lang,
                &message.driver_version
            );
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

    async fn databases_all(
        &self,
        _request: Request<typedb_protocol::database_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        Ok(Response::new(database_all_res(&self.address, self.database_manager.database_names())))
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
        self.database_manager
            .create_database(message.name.clone())
            .map(|_| Response::new(database_create_res(message.name, &self.address)))
            .map_err(|err| err.into_error_message().into_status())
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

    async fn users_get(
        &self,
        request: Request<typedb_protocol::user_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::get::Res>, Status> {
        let accessor = extract_username_field(request.metadata());
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
    }

    async fn users_all(
        &self,
        request: Request<typedb_protocol::user_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::all::Res>, Status> {
        let accessor = extract_username_field(request.metadata());
        if !PermissionManager::exec_user_all_permitted(accessor.as_str()) {
            return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
        }
        let users = self.user_manager.all();
        Ok(Response::new(users_all_res(users)))
    }

    async fn users_contains(
        &self,
        request: Request<typedb_protocol::user_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::contains::Res>, Status> {
        let contains_req = request.into_inner();
        self.user_manager
            .contains(contains_req.name.as_str())
            .map(|contains| Response::new(users_contains_res(contains)))
            .map_err(|err| err.into_error_message().into_status())
    }

    async fn users_create(
        &self,
        request: Request<typedb_protocol::user_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::create::Res>, Status> {
        let accessor = extract_username_field(request.metadata());
        if !PermissionManager::exec_user_create_permitted(accessor.as_str()) {
            return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
        }
        users_create_req(request)
            .and_then(|(usr, cred)| self.user_manager.create(&usr, &cred))
            .map(|_| Response::new(user_create_res()))
            .map_err(|err| err.into_error_message().into_status())
    }

    async fn users_update(
        &self,
        request: Request<typedb_protocol::user::update::Req>,
    ) -> Result<Response<typedb_protocol::user::update::Res>, Status> {
        let accessor = extract_username_field(request.metadata());
        match users_update_req(request) {
            Ok((username, user_update, credential_update)) => {
                let username = username.as_str();
                if !PermissionManager::exec_user_update_permitted(accessor.as_str(), username) {
                    return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
                }
                match self.user_manager.update(username, &user_update, &credential_update) {
                    Ok(()) => {
                        self.authenticator_cache.invalidate_user(username);
                        Ok(Response::new(user_update_res()))
                    }
                    Err(user_update_err) => Err(user_update_err.into_error_message().into_status()),
                }
            }
            Err(user_update_err) => Err(user_update_err.into_error_message().into_status()),
        }
    }

    async fn users_delete(
        &self,
        request: Request<typedb_protocol::user::delete::Req>,
    ) -> Result<Response<typedb_protocol::user::delete::Res>, Status> {
        let accessor = extract_username_field(request.metadata());
        let delete_req = request.into_inner();
        let username = delete_req.name.as_str();
        if !PermissionManager::exec_user_delete_allowed(accessor.as_str(), username) {
            return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
        }
        let result = self.user_manager.delete(username);
        match result {
            Ok(_) => {
                self.authenticator_cache.invalidate_user(username);
                Ok(Response::new(users_delete_res()))
            }
            Err(e) => Err(e.into_error_message().into_status()),
        }
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

fn extract_username_field(metadata: &MetadataMap) -> String {
    metadata
        .get(AUTHENTICATOR_USERNAME_FIELD)
        .map(|u| u.to_str())
        .expect(format!("Unable to find expected field in the metadata: {}", AUTHENTICATOR_USERNAME_FIELD).as_str())
        .expect(format!("Unable to parse value from the {} field", AUTHENTICATOR_USERNAME_FIELD).as_str())
        .to_string()
}

typedb_error!(
    ServiceError(component = "Server", prefix = "SRV") {
        Unimplemented(1, "Not implemented: {description}", description: String),
        OperationNotPermitted(2, "The user is not permitted to execute the operation"),
        DatabaseDoesNotExist(3, "Database '{name}' does not exist.", name: String),
        UserDoesNotExist(4, "User does not exist"),
    }
);
