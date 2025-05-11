use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use rand::prelude::SliceRandom;
use tokio::sync::mpsc::channel;
use tokio::sync::watch::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tonic::body::BoxBody;
use tonic::metadata::MetadataMap;
use tracing::{event, Level};
use typedb_protocol::server_manager::all::{Req, Res};
use typedb_protocol::transaction::{Client, Server};
use uuid::Uuid;
use concurrency::IntervalRunner;
use database::database_manager::DatabaseManager;
use user::user_manager::UserManager;
use diagnostics::{diagnostics_manager::DiagnosticsManager, Diagnostics};
use diagnostics::metrics::ActionKind;
use resource::constants::server::{DATABASE_METRICS_UPDATE_INTERVAL, SERVER_ID_ALPHABET, SERVER_ID_FILE_NAME, SERVER_ID_LENGTH};
use resource::server_info::ServerInfo;
use system::concepts::Credential;
use system::initialise_system_database;
use user::initialise_default_user;
use user::permission_manager::PermissionManager;
use crate::{
    error::ServerOpenError,
    parameters::config::{Config, DiagnosticsConfig},
    service::grpc::{
        diagnostics::run_with_diagnostics,
        error::{IntoGrpcStatus, IntoProtocolErrorMessage, ProtocolError, ServiceError},
        request_parser::{users_create_req, users_update_req},
        response_builders::{
            authentication::token_create_res,
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
use crate::authentication::authenticate;
use crate::authentication::credential_verifier::CredentialVerifier;
use crate::authentication::token_manager::TokenManager;
use crate::service::grpc::diagnostics::run_with_diagnostics_async;
use crate::service::grpc::response_builders::database_manager::{database_schema_res, database_type_schema_res};
use crate::service::transaction_service::TRANSACTION_REQUEST_BUFFER_SIZE;
use crate::service::typedb_service::{get_database_schema, get_database_type_schema};
use crate::util::resolve_address;

const ERROR_INVALID_CREDENTIAL: &str = "Invalid credential supplied";

#[derive(Debug)]
pub struct ServerState {
    config: Config,
    id: String,
    deployment_id: String,
    pub address: SocketAddr,
    database_manager: Arc<DatabaseManager>,
    user_manager: UserManager,
    credential_verifier: Arc<CredentialVerifier>,
    token_manager: Arc<TokenManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    database_diagnostics_updater: IntervalRunner,
    shutdown_receiver: Receiver<()>,
}

impl ServerState {
    pub async fn new(
        server_info: ServerInfo,
        config: Config,
        deployment_id: Option<String>,
        shutdown_receiver: Receiver<()>,
    ) -> Result<Self, ServerOpenError> {
        let address = resolve_address(config.server.address.clone()).await;
        let storage_directory = &config.storage.data;
        let diagnostics_config = &config.diagnostics;

        Self::may_initialise_storage_directory(storage_directory)?;

        let server_id = Self::may_initialise_server_id(storage_directory)?;

        let deployment_id = deployment_id.unwrap_or(server_id.clone());

        let database_manager = DatabaseManager::new(storage_directory)
            .map_err(|err| ServerOpenError::DatabaseOpen { typedb_source: err })?;
        let system_database = initialise_system_database(&database_manager);

        let user_manager = UserManager::new(system_database);
        initialise_default_user(&user_manager);

        let diagnostics_manager = Arc::new(
            Self::initialise_diagnostics(
                deployment_id.clone(),
                server_id.clone(),
                &server_info,
                diagnostics_config,
                storage_directory.clone(),
                config.is_development_mode,
            ).await
        );

        Ok(Self {
            config,
            id: server_id,
            deployment_id,
            address,
            database_manager: database_manager.clone(),
            user_manager,
            credential_verifier: todo!(),
            token_manager: todo!(),
            diagnostics_manager: diagnostics_manager.clone(),
            database_diagnostics_updater: IntervalRunner::new(
                move || Self::synchronize_database_metrics(diagnostics_manager.clone(), database_manager.clone()),
                DATABASE_METRICS_UPDATE_INTERVAL,
            ),
            shutdown_receiver
        })
    }

    fn may_initialise_storage_directory(storage_directory: &Path) -> Result<(), ServerOpenError> {
        if !storage_directory.exists() {
            Self::create_storage_directory(storage_directory)
        } else if !storage_directory.is_dir() {
            Err(ServerOpenError::NotADirectory { path: storage_directory.to_str().unwrap_or("").to_owned() })
        } else {
            Ok(())
        }
    }

    fn create_storage_directory(storage_directory: &Path) -> Result<(), ServerOpenError> {
        fs::create_dir_all(storage_directory).map_err(|source| ServerOpenError::CouldNotCreateDataDirectory {
            path: storage_directory.to_str().unwrap_or("").to_owned(),
            source: Arc::new(source),
        })?;
        Ok(())
    }

    fn may_initialise_server_id(storage_directory: &Path) -> Result<String, ServerOpenError> {
        let server_id_file = storage_directory.join(SERVER_ID_FILE_NAME);
        if server_id_file.exists() {
            let server_id = fs::read_to_string(&server_id_file)
                .map_err(|source| ServerOpenError::CouldNotReadServerIDFile {
                    path: server_id_file.to_str().unwrap_or("").to_owned(),
                    source: Arc::new(source),
                })?
                .trim()
                .to_owned();
            if server_id.is_empty() {
                Err(ServerOpenError::InvalidServerID { path: server_id_file.to_str().unwrap_or("").to_owned() })
            } else {
                Ok(server_id)
            }
        } else {
            let server_id = Self::generate_server_id();
            assert!(!server_id.is_empty(), "Generated server ID should not be empty");
            fs::write(server_id_file.clone(), &server_id).map_err(|source| {
                ServerOpenError::CouldNotCreateServerIDFile {
                    path: server_id_file.to_str().unwrap_or("").to_owned(),
                    source: Arc::new(source),
                }
            })?;
            Ok(server_id)
        }
    }

    fn generate_server_id() -> String {
        let mut rng = rand::thread_rng();
        (0..SERVER_ID_LENGTH).map(|_| SERVER_ID_ALPHABET.choose(&mut rng).unwrap()).collect()
    }

    fn generate_connection_id(&self) -> ConnectionID {
        Uuid::new_v4().into_bytes()
    }

    async fn initialise_diagnostics(
        deployment_id: String,
        server_id: String,
        server_info: &ServerInfo,
        config: &DiagnosticsConfig,
        storage_directory: PathBuf,
        is_development_mode: bool,
    ) -> DiagnosticsManager {
        let diagnostics = Diagnostics::new(
            deployment_id,
            server_id,
            server_info.distribution.to_owned(),
            server_info.version.to_owned(),
            storage_directory,
            config.is_reporting_metric_enabled,
        );
        let diagnostics_manager = DiagnosticsManager::new(
            diagnostics,
            config.monitoring_port,
            config.is_monitoring_enabled,
            is_development_mode,
        );
        diagnostics_manager.may_start_monitoring().await;
        diagnostics_manager.may_start_reporting().await;

        diagnostics_manager
    }

    fn synchronize_database_metrics(
        diagnostics_manager: Arc<DiagnosticsManager>,
        database_manager: Arc<DatabaseManager>,
    ) {
        let metrics = database_manager
            .databases()
            .values()
            .filter(|database| DatabaseManager::is_user_database(database.name()))
            .map(|database| database.get_metrics())
            .collect();
        diagnostics_manager.submit_database_metrics(metrics);
    }

    pub async fn authentication_token_create(
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

    pub async fn connection_open(
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
                    Err(err.into_status())
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

    pub async fn servers_all(&self, _request: Request<Req>) -> Result<Response<Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::ServersAll, || {
            Ok(Response::new(servers_all_res(&self.address)))
        })
    }

    pub async fn databases_all(
        &self,
        _request: Request<typedb_protocol::database_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::DatabasesAll, || {
            Ok(Response::new(database_all_res(&self.address, self.database_manager.database_names())))
        })
    }

    pub async fn databases_get(
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
    pub async fn databases_contains(
        &self,
        request: Request<typedb_protocol::database_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics(&self.diagnostics_manager, Some(&message.name), ActionKind::DatabasesContains, || {
            Ok(Response::new(database_contains_res(self.database_manager.database(&message.name).is_some())))
        })
    }

    pub async fn databases_create(
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

    pub async fn database_schema(
        &self,
        request: Request<typedb_protocol::database::schema::Req>,
    ) -> Result<Response<typedb_protocol::database::schema::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics(&self.diagnostics_manager, Some(&message.name), ActionKind::DatabaseSchema, || match self
            .database_manager
            .database(&message.name)
        {
            None => Err(ServiceError::DatabaseDoesNotExist { name: message.name.clone() }
                .into_error_message()
                .into_status()),
            Some(database) => Ok(Response::new(database_schema_res(
                get_database_schema(database)
                    .map_err(|typedb_source| typedb_source.into_error_message().into_status())?,
            ))),
        })
    }

    pub async fn database_type_schema(
        &self,
        request: Request<typedb_protocol::database::type_schema::Req>,
    ) -> Result<Response<typedb_protocol::database::type_schema::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics(&self.diagnostics_manager, Some(&message.name), ActionKind::DatabaseTypeSchema, || {
            match self.database_manager.database(&message.name) {
                None => Err(ServiceError::DatabaseDoesNotExist { name: message.name.clone() }
                    .into_error_message()
                    .into_status()),
                Some(database) => Ok(Response::new(database_type_schema_res(
                    get_database_type_schema(database)
                        .map_err(|typedb_source| typedb_source.into_error_message().into_status())?,
                ))),
            }
        })
    }

    pub async fn database_delete(
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

    pub async fn users_get(
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

    pub async fn users_all(
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

    pub async fn users_contains(
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

    pub async fn users_create(
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

    pub async fn users_update(
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

    pub async fn users_delete(
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

    pub async fn transaction(
        &self,
        request: Request<Streaming<Client>>,
    ) -> Result<Response<Pin<Box<ReceiverStream<Result<Server, Status>>>>>, Status> {
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

    pub fn database_manager(&self) -> &DatabaseManager {
        todo!()
    }
}
