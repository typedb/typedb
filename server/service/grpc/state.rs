use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use http::Extensions;
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
use database::{Database, DatabaseDeleteError};
use database::database::DatabaseCreateError;
use database::database_manager::DatabaseManager;
use user::user_manager::UserManager;
use diagnostics::{diagnostics_manager::DiagnosticsManager, Diagnostics};
use diagnostics::metrics::ActionKind;
use error::typedb_error;
use resource::constants::server::{DATABASE_METRICS_UPDATE_INTERVAL, SERVER_ID_ALPHABET, SERVER_ID_FILE_NAME, SERVER_ID_LENGTH};
use resource::server_info::ServerInfo;
use storage::durability_client::WALClient;
use system::concepts::{Credential, User};
use system::initialise_system_database;
use user::errors::UserGetError;
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
use crate::authentication::{authenticate, Accessor, AuthenticationError};
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
    user_manager: Arc<UserManager>,
    credential_verifier: Arc<CredentialVerifier>,
    token_manager: Arc<TokenManager>,
    pub diagnostics_manager: Arc<DiagnosticsManager>,
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

        let user_manager = Arc::new(UserManager::new(system_database));
        initialise_default_user(&user_manager);

        let credential_verifier = Arc::new(CredentialVerifier::new(user_manager.clone()));
        let token_manager = Arc::new(
            TokenManager::new(config.server.authentication.token_expiration)
                .map_err(|typedb_source| ServerOpenError::TokenConfiguration { typedb_source })?,
        );

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
            credential_verifier,
            token_manager,
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

    pub async fn authenticate(&self, request: http::Request<BoxBody>) -> Result<http::Request<BoxBody>, Status> {
        run_with_diagnostics_async(self.diagnostics_manager.clone(), None::<&str>, ActionKind::Authenticate, || async {
            authenticate(self.token_manager.clone(), request)
                .await
                .map_err(|typedb_source| typedb_source.into_error_message().into_status())
        })
            .await
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

    pub fn servers_all(&self) -> &SocketAddr {
        &self.address
    }

    pub fn databases_all(&self) -> Vec<String> {
        self.database_manager.database_names()
    }

    pub fn databases_get(
        &self,
        name: String
    ) -> Result<Option<Arc<Database<WALClient>>>, ServiceError> {
        Ok(self.database_manager.database(name.as_str()))
    }

    pub fn databases_contains(&self, name: String) -> bool {
        self.database_manager.database(&name).is_some()
    }

    pub fn databases_create(&self, name: String) -> Result<(), DatabaseCreateError> {
        self.database_manager.create_database(name)
    }

    pub fn database_schema(&self, name: String) -> Result<String, crate::service::ServiceError> {
        match self.database_manager.database(&name) {
            Some(db) => get_database_schema(db),
            None => Err(crate::service::ServiceError::DatabaseDoesNotExist { name })
        }
    }

    pub fn database_type_schema(&self, name: String) -> Result<String, crate::service::ServiceError> {
        match self.database_manager.database(&name) {
            None => Err(crate::service::ServiceError::DatabaseDoesNotExist { name: name.clone() }),
            Some(database) => {
                match get_database_type_schema(database) {
                    Ok(type_schema) => Ok(type_schema),
                    Err(err) => Err(err)
                }
            }
        }
    }

    pub fn database_delete(&self, name: String) -> Result<(), DatabaseDeleteError> {
        self.database_manager.delete_database(name)
    }

    pub fn users_get(
        &self,
        name: String,
        accessor: Accessor
    ) -> Result<User, crate::service::ServiceError> {
        if !PermissionManager::exec_user_get_permitted(accessor.0.as_str(), name.as_str()) {
            return Err(crate::service::ServiceError::OperationNotPermitted {});
        }

        match self.user_manager.get(name.as_str()) {
            Ok(get) => {
                match get {
                    Some((user, _)) => Ok(user),
                    None => Err(crate::service::ServiceError::UserDoesNotExist {}),
                }
            }
            Err(err) => Err(crate::service::ServiceError::UserCannotBeRetrieved { typedb_source: err }),
        }
    }

    pub fn users_all(&self, accessor: Accessor) -> Result<Vec<User>, crate::service::ServiceError> {
        if !PermissionManager::exec_user_all_permitted(accessor.0.as_str()) {
            return Err(crate::service::ServiceError::OperationNotPermitted {});
        }
        Ok(self.user_manager.all())
    }

    pub fn users_contains(&self, name: &str) -> Result<bool, UserGetError> {
        self.user_manager.contains(name)
    }

    pub fn users_create(
        &self,
        user: &User,
        credential: &Credential,
        accessor: Accessor
    ) -> Result<(), crate::service::ServiceError> {
        if !PermissionManager::exec_user_create_permitted(accessor.0.as_str()) {
            return Err(crate::service::ServiceError::OperationNotPermitted {});
        }
        self.user_manager.create(user, credential)
            .map(|user| ())
            .map_err(|err| crate::service::ServiceError::UserCannotBeCreated { typedb_source: err })
    }

    pub async fn users_update(
        &self,
        name: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
        accessor: Accessor
    ) -> Result<(), crate::service::ServiceError> {
        if !PermissionManager::exec_user_update_permitted(accessor.0.as_str(), name) {
            return Err(crate::service::ServiceError::OperationNotPermitted {});
        }
        self.user_manager
            .update(name, &user_update, &credential_update)
            .map_err(|err| crate::service::ServiceError::UserCannotBeUpdated { typedb_source: err })?;
        self.token_manager.invalidate_user(name).await;
        Ok(())
    }

    pub async fn users_delete(
        &self,
        name: &str,
        accessor: Accessor
    ) -> Result<(), crate::service::ServiceError> {
        if !PermissionManager::exec_user_delete_allowed(accessor.0.as_str(), name) {
            return Err(crate::service::ServiceError::OperationNotPermitted {});
        }

        self.user_manager.delete(name)
            .map_err(|err| crate::service::ServiceError::UserCannotBeDeleted { typedb_source: err })?;
        self.token_manager.invalidate_user(name).await;
        Ok(())
    }

    pub async fn transaction(
        &self,
        request_stream: Streaming<Client>,
    ) -> ReceiverStream<Result<Server, Status>> {
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
        stream
    }

    pub fn database_manager(&self) -> &DatabaseManager {
        todo!()
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
