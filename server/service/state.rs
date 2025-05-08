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
use diagnostics::diagnostics_manager::run_with_diagnostics;
use diagnostics::metrics::ActionKind;
use resource::constants::server::{AUTHENTICATOR_PASSWORD_FIELD, AUTHENTICATOR_USERNAME_FIELD, DATABASE_METRICS_UPDATE_INTERVAL, SERVER_ID_ALPHABET, SERVER_ID_FILE_NAME, SERVER_ID_LENGTH};
use resource::server_info::ServerInfo;
use system::concepts::Credential;
use system::initialise_system_database;
use user::initialise_default_user;
use user::permission_manager::PermissionManager;
use crate::{
    error::ServerOpenError,
    parameters::config::{Config, DiagnosticsConfig},
    service::authenticator_cache::AuthenticatorCache,
    service::{
        error::{IntoGRPCStatus, IntoProtocolErrorMessage, ProtocolError, ServiceError},
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
    authenticator_cache: Arc<AuthenticatorCache>,
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
                config.server.is_development_mode,
            ).await
        );

        Ok(Self {
            config,
            id: server_id,
            deployment_id,
            address,
            database_manager: database_manager.clone(),
            user_manager,
            authenticator_cache: Arc::new(AuthenticatorCache::new()),
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

    pub async fn open_connection(
        &self,
        request: Request<typedb_protocol::connection::open::Req>,
    ) -> Result<Response<typedb_protocol::connection::open::Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::ConnectionOpen, || {
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
        })
    }

    pub async fn list_servers(&self, _request: Request<Req>) -> Result<Response<Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::ServersAll, || {
            Ok(Response::new(servers_all_res(&self.address)))
        })
    }

    pub async fn list_databases(
        &self,
        _request: Request<typedb_protocol::database_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::DatabasesAll, || {
            Ok(Response::new(database_all_res(&self.address, self.database_manager.database_names())))
        })
    }

    pub async fn get_database(
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

    pub async fn database_exists(
        &self,
        request: Request<typedb_protocol::database_manager::contains::Req>,
    ) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
        let message = request.into_inner();
        run_with_diagnostics(&self.diagnostics_manager, Some(&message.name), ActionKind::DatabasesContains, || {
            Ok(Response::new(database_contains_res(self.database_manager.database(&message.name).is_some())))
        })
    }

    pub async fn create_database(
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

    pub async fn get_database_schema(
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

    pub async fn list_database_schema_types(
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

    pub async fn delete_database(
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

    pub async fn get_user(
        &self,
        request: Request<typedb_protocol::user_manager::get::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::get::Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersGet, || {
            let accessor = Self::extract_username_field(request.metadata());
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
    }

    pub async fn list_users(
        &self,
        request: Request<typedb_protocol::user_manager::all::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::all::Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersAll, || {
            let accessor = Self::extract_username_field(request.metadata());
            if !PermissionManager::exec_user_all_permitted(accessor.as_str()) {
                return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
            }
            let users = self.user_manager.all();
            Ok(Response::new(users_all_res(users)))
        })
    }

    pub async fn user_exists(
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

    pub async fn create_users(
        &self,
        request: Request<typedb_protocol::user_manager::create::Req>,
    ) -> Result<Response<typedb_protocol::user_manager::create::Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersCreate, || {
            let accessor = Self::extract_username_field(request.metadata());
            if !PermissionManager::exec_user_create_permitted(accessor.as_str()) {
                return Err(ServiceError::OperationNotPermitted {}.into_error_message().into_status());
            }
            users_create_req(request)
                .and_then(|(usr, cred)| self.user_manager.create(&usr, &cred))
                .map(|_| Response::new(user_create_res()))
                .map_err(|err| err.into_error_message().into_status())
        })
    }

    pub async fn update_user(
        &self,
        request: Request<typedb_protocol::user::update::Req>,
    ) -> Result<Response<typedb_protocol::user::update::Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersUpdate, || {
            let accessor = Self::extract_username_field(request.metadata());
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
        })
    }

    pub async fn delete_user(
        &self,
        request: Request<typedb_protocol::user::delete::Req>,
    ) -> Result<Response<typedb_protocol::user::delete::Res>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersDelete, || {
            let accessor = Self::extract_username_field(request.metadata());
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
        })
    }

    pub fn database_manager(&self) -> &DatabaseManager {
        todo!()
    }

    pub async fn open_transaction(
        &self,
        request: Request<Streaming<Client>>,
    ) -> Result<Response<Pin<Box<ReceiverStream<Result<Server, Status>>>>>, Status> {
        let request_stream = request.into_inner();
        let (response_sender, response_receiver) = channel(10);
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

    pub fn authenticate(&self, http: http::Request<BoxBody>) -> Result<http::Request<BoxBody>, Status> {
        run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::Authenticate, || {
            let (parts, body) = http.into_parts();

            let metadata = MetadataMap::from_headers(parts.headers.clone());
            let username_metadata = metadata.get(AUTHENTICATOR_USERNAME_FIELD).and_then(|u| u.to_str().ok());
            let password_metadata = metadata.get(AUTHENTICATOR_PASSWORD_FIELD).and_then(|u| u.to_str().ok());

            let username = username_metadata.ok_or(Status::unauthenticated(ERROR_INVALID_CREDENTIAL))?;
            let password = password_metadata.ok_or(Status::unauthenticated(ERROR_INVALID_CREDENTIAL))?;

            match self.authenticator_cache.get_user(username) {
                Some(p) => {
                    if p == password {
                        Ok(http::Request::from_parts(parts, body))
                    } else {
                        Err(Status::unauthenticated(ERROR_INVALID_CREDENTIAL))
                    }
                }
                None => {
                    let Ok(Some((_, Credential::PasswordType { password_hash }))) = self.user_manager.get(username)
                    else {
                        return Err(Status::unauthenticated(ERROR_INVALID_CREDENTIAL));
                    };

                    if password_hash.matches(password) {
                        self.authenticator_cache.cache_user(username, password);
                        Ok(http::Request::from_parts(parts, body))
                    } else {
                        Err(Status::unauthenticated(ERROR_INVALID_CREDENTIAL))
                    }
                }
            }
        })
    }

    fn extract_username_field(metadata: &MetadataMap) -> String {
        metadata
            .get(AUTHENTICATOR_USERNAME_FIELD)
            .map(|u| u.to_str())
            .expect(format!("Unable to find expected field in the metadata: {}", AUTHENTICATOR_USERNAME_FIELD).as_str())
            .expect(format!("Unable to parse value from the {} field", AUTHENTICATOR_USERNAME_FIELD).as_str())
            .to_string()
    }

}
