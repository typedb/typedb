/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use std::net::SocketAddr;
use concept::error::ConceptReadError;
use concurrency::IntervalRunner;
use database::{
    database::DatabaseCreateError, database_manager::DatabaseManager, transaction::TransactionRead, Database,
    DatabaseDeleteError,
};
use diagnostics::{diagnostics_manager::DiagnosticsManager, Diagnostics};
use error::typedb_error;
use ir::pipeline::FunctionReadError;
use options::TransactionOptions;
use rand::prelude::SliceRandom;
use resource::{
    constants::server::{DATABASE_METRICS_UPDATE_INTERVAL, SERVER_ID_ALPHABET, SERVER_ID_FILE_NAME, SERVER_ID_LENGTH},
    server_info::ServerInfo,
};
use storage::durability_client::{DurabilityClient, WALClient};
use system::{
    concepts::{Credential, User},
    initialise_system_database,
};
use tokio::sync::watch::Receiver;
use user::{
    errors::{UserCreateError, UserDeleteError, UserGetError, UserUpdateError},
    initialise_default_user,
    permission_manager::PermissionManager,
    user_manager::UserManager,
};

use crate::{
    authentication::{
        credential_verifier::CredentialVerifier, token_manager::TokenManager, Accessor, AuthenticationError,
    },
    error::ServerOpenError,
    parameters::config::{Config, DiagnosticsConfig},
};

pub trait IState {
    fn databases_all(&self) -> Vec<String>;

    fn databases_get(&self, name: &str) -> Option<Arc<Database<WALClient>>>;

    fn databases_contains(&self, name: &str) -> bool;

    fn databases_create(&self, name: impl AsRef<str>) -> Result<(), DatabaseCreateError>;

    fn database_schema(&self, name: String) -> Result<String, StateError>;

    fn database_type_schema(&self, name: String) -> Result<String, StateError>;

    fn database_delete(&self, name: impl AsRef<str>) -> Result<(), DatabaseDeleteError>;

    fn users_get(&self, name: &str, accessor: Accessor) -> Result<User, StateError>;

    fn users_all(&self, accessor: Accessor) -> Result<Vec<User>, StateError>;

    fn users_contains(&self, name: &str) -> Result<bool, UserGetError>;

    fn users_create(
        &self,
        user: &User,
        credential: &Credential,
        accessor: Accessor
    ) -> Result<(), StateError>;

    async fn users_update(
        &self,
        name: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
        accessor: Accessor
    ) -> Result<(), StateError>;

    async fn users_delete(&self, name: &str, accessor: Accessor) -> Result<(), StateError>;

    fn user_verify_password(&self, username: &str, password: &str) -> Result<(), AuthenticationError>;
    
    async fn token_create(
        &self,
        username: String,
        password: String
    ) -> Result<String, AuthenticationError>;
    
    async fn token_get_owner(&self, token: &str) -> Option<String>;
}

#[derive(Debug)]
pub struct ServerState {
    pub database_manager: Arc<DatabaseManager>,
    user_manager: Arc<UserManager>,
    credential_verifier: Arc<CredentialVerifier>,
    token_manager: Arc<TokenManager>,
    pub diagnostics_manager: Arc<DiagnosticsManager>,
    _database_diagnostics_updater: IntervalRunner,
    pub shutdown_receiver: Receiver<()>,
}

impl ServerState {
    pub async fn new(
        server_info: ServerInfo,
        config: Config,
        deployment_id: Option<String>,
        shutdown_receiver: Receiver<()>,
    ) -> Result<Self, ServerOpenError> {
        let storage_directory = &config.storage.data_directory;
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
                config.development_mode.enabled,
            )
                .await,
        );

        Ok(Self {
            database_manager: database_manager.clone(),
            user_manager,
            credential_verifier,
            token_manager,
            diagnostics_manager: diagnostics_manager.clone(),
            _database_diagnostics_updater: IntervalRunner::new(
                move || Self::synchronize_database_metrics(diagnostics_manager.clone(), database_manager.clone()),
                DATABASE_METRICS_UPDATE_INTERVAL,
            ),
            shutdown_receiver,
        })
    }

    fn may_initialise_storage_directory(storage_directory: &Path) -> Result<(), ServerOpenError> {
        debug_assert!(storage_directory.is_absolute());
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
            config.reporting.report_metrics,
        );
        let diagnostics_manager = DiagnosticsManager::new(
            diagnostics,
            config.monitoring.port,
            config.monitoring.enabled,
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

    pub fn get_database_schema<D: DurabilityClient>(database: Arc<Database<D>>) -> Result<String, StateError> {
        let transaction = TransactionRead::open(database, TransactionOptions::default())
            .map_err(|err| StateError::FailedToOpenPrerequisiteTransaction {})?;
        let types_syntax = Self::get_types_syntax(&transaction)?;
        let functions_syntax = Self::get_functions_syntax(&transaction)?;

        let schema = match types_syntax.is_empty() & functions_syntax.is_empty() {
            true => String::new(),
            false => format!("{}\n{} {}", typeql::token::Clause::Define, types_syntax, functions_syntax),
        };
        Ok(schema)
    }

    pub fn get_functions_syntax<D: DurabilityClient>(transaction: &TransactionRead<D>) -> Result<String, StateError> {
        transaction
            .function_manager
            .get_functions_syntax(transaction.snapshot())
            .map_err(|err| StateError::FunctionReadError { typedb_source: err })
    }

    pub(crate) fn get_database_type_schema<D: DurabilityClient>(
        database: Arc<Database<D>>,
    ) -> Result<String, StateError> {
        let transaction = TransactionRead::open(database, TransactionOptions::default())
            .map_err(|err| StateError::FailedToOpenPrerequisiteTransaction {})?;
        let types_syntax = Self::get_types_syntax(&transaction)?;

        let type_schema = match types_syntax.is_empty() {
            true => String::new(),
            false => format!("{}\n{}", typeql::token::Clause::Define, types_syntax),
        };
        Ok(type_schema)
    }

    pub fn get_types_syntax<D: DurabilityClient>(transaction: &TransactionRead<D>) -> Result<String, StateError> {
        transaction
            .type_manager
            .get_types_syntax(transaction.snapshot())
            .map_err(|err| StateError::ConceptReadError { typedb_source: err })
    }

    pub fn database_manager(&self) -> &DatabaseManager {
        &self.database_manager
    }
}

impl IState for ServerState {

    fn databases_all(&self) -> Vec<String> {
        self.database_manager.database_names()
    }

    fn databases_get(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        self.database_manager.database(name)
    }

    fn databases_contains(&self, name: &str) -> bool {
        self.database_manager.database(name).is_some()
    }

    fn databases_create(&self, name: impl AsRef<str>) -> Result<(), DatabaseCreateError> {
        self.database_manager.create_database(name)
    }

    fn database_schema(&self, name: String) -> Result<String, StateError> {
        match self.database_manager.database(&name) {
            Some(db) => Self::get_database_schema(db),
            None => Err(StateError::DatabaseDoesNotExist { name }),
        }
    }

    fn database_type_schema(&self, name: String) -> Result<String, StateError> {
        match self.database_manager.database(&name) {
            None => Err(StateError::DatabaseDoesNotExist { name: name.clone() }),
            Some(database) => match Self::get_database_type_schema(database) {
                Ok(type_schema) => Ok(type_schema),
                Err(err) => Err(err),
            },
        }
    }

    fn database_delete(&self, name: impl AsRef<str>) -> Result<(), DatabaseDeleteError> {
        self.database_manager.delete_database(name)
    }

    fn users_get(&self, name: &str, accessor: Accessor) -> Result<User, StateError> {
        if !PermissionManager::exec_user_get_permitted(accessor.0.as_str(), name) {
            return Err(StateError::OperationNotPermitted {});
        }

        match self.user_manager.get(name) {
            Ok(get) => match get {
                Some((user, _)) => Ok(user),
                None => Err(StateError::UserDoesNotExist {}),
            },
            Err(err) => Err(StateError::UserCannotBeRetrieved { typedb_source: err }),
        }
    }

    fn users_all(&self, accessor: Accessor) -> Result<Vec<User>, StateError> {
        if !PermissionManager::exec_user_all_permitted(accessor.0.as_str()) {
            return Err(StateError::OperationNotPermitted {});
        }
        Ok(self.user_manager.all())
    }

    fn users_contains(&self, name: &str) -> Result<bool, UserGetError> {
        self.user_manager.contains(name)
    }

    fn users_create(&self, user: &User, credential: &Credential, accessor: Accessor) -> Result<(), StateError> {
        if !PermissionManager::exec_user_create_permitted(accessor.0.as_str()) {
            return Err(StateError::OperationNotPermitted {});
        }
        self.user_manager
            .create(user, credential)
            .map(|user| ())
            .map_err(|err| StateError::UserCannotBeCreated { typedb_source: err })
    }

    async fn users_update(
        &self,
        name: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
        accessor: Accessor,
    ) -> Result<(), StateError> {
        if !PermissionManager::exec_user_update_permitted(accessor.0.as_str(), name) {
            return Err(StateError::OperationNotPermitted {});
        }
        self.user_manager
            .update(name, &user_update, &credential_update)
            .map_err(|err| StateError::UserCannotBeUpdated { typedb_source: err })?;
        self.token_manager.invalidate_user(name).await;
        Ok(())
    }

    async fn users_delete(&self, name: &str, accessor: Accessor) -> Result<(), StateError> {
        if !PermissionManager::exec_user_delete_allowed(accessor.0.as_str(), name) {
            return Err(StateError::OperationNotPermitted {});
        }

        self.user_manager.delete(name).map_err(|err| StateError::UserCannotBeDeleted { typedb_source: err })?;
        self.token_manager.invalidate_user(name).await;
        Ok(())
    }

    fn user_verify_password(&self, username: &str, password: &str) -> Result<(), AuthenticationError> {
        self.credential_verifier.verify_password(username, password)
    }

    async fn token_create(&self, username: String, password: String) -> Result<String, AuthenticationError> {
        self.user_verify_password(&username, &password)?;
        Ok(self.token_manager.new_token(username).await)
    }

    async fn token_get_owner(&self, token: &str) -> Option<String> {
        self.token_manager.get_valid_token_owner(token).await
    }
}

typedb_error! {
    pub StateError(component = "State", prefix = "SRV") {
        Unimplemented(1, "Not implemented: {description}", description: String),
        OperationNotPermitted(2, "The user is not permitted to execute the operation"),
        DatabaseDoesNotExist(3, "Database '{name}' does not exist.", name: String),
        UserDoesNotExist(4, "User does not exist"),
        UserCannotBeRetrieved(8, "Unable to retrieve user", typedb_source: UserGetError),
        UserCannotBeCreated(9, "Unable to create user", typedb_source: UserCreateError),
        UserCannotBeUpdated(10, "Unable to update user", typedb_source: UserUpdateError),
        UserCannotBeDeleted(11, "Unable to delete user", typedb_source: UserDeleteError),
        FailedToOpenPrerequisiteTransaction(5, "Failed to open transaction, which is a prerequisite for the operation."),
        ConceptReadError(6, "Error reading concepts", typedb_source: Box<ConceptReadError>),
        FunctionReadError(7, "Error reading functions", typedb_source: FunctionReadError),
    }
}
