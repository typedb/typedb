/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, fs, io, path::PathBuf};
use std::sync::Arc;
use user::user_manager::UserManager;
use database::{database_manager::DatabaseManager, DatabaseOpenError};
use resource::constants::server::GRPC_CONNECTION_KEEPALIVE;
use system::concepts::{Credential, PasswordHash, User};
use system::initialise_system_database;
use user::initialise_default_user;
use crate::{parameters::config::Config, service::typedb_service::TypeDBService};
use crate::authenticator::Authenticator;

#[derive(Debug)]
pub struct Server {
    data_directory: PathBuf,
    user_manager: Arc<UserManager>,
    typedb_service: Option<TypeDBService>,
    config: Config,
}

impl Server {
    pub fn open(config: Config) -> Result<Self, ServerOpenError> {
        let storage_directory = &config.storage.data;
        if !storage_directory.exists() {
            Self::create_storage_directory(storage_directory)?;
        } else if !storage_directory.is_dir() {
            return Err(ServerOpenError::NotADirectory { path: storage_directory.to_owned() });
        }
        let database_manager = DatabaseManager::new(storage_directory)
            .map_err(|err| ServerOpenError::DatabaseOpenError { source: err })?;
        let system_db = initialise_system_database(&database_manager);
        let user_manager = Arc::new(UserManager::new(system_db));
        initialise_default_user(&user_manager);
        let typedb_service = TypeDBService::new(
            &config.server.address, database_manager, user_manager.clone()
        );
        Ok(Self {
            data_directory: storage_directory.to_owned(),
            user_manager,
            typedb_service: Some(typedb_service),
            config
        })
    }

    pub fn database_manager(&self) -> &DatabaseManager {
        self.typedb_service.as_ref().unwrap().database_manager()
    }

    pub async fn serve(mut self) -> Result<(), tonic::transport::Error> {
        let service = typedb_protocol::type_db_server::TypeDbServer::new(self.typedb_service.take().unwrap());
        println!("Ready...");
        let authenticator = Arc::new(Authenticator::new(self.user_manager.clone()));
        tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(GRPC_CONNECTION_KEEPALIVE))
            .layer(tonic::service::interceptor(move |req| authenticator.authenticate(req)))
            .add_service(service)
            .serve(self.config.server.address)
            .await
    }
    fn create_storage_directory(storage_directory: &PathBuf) -> Result<(), ServerOpenError> {
        fs::create_dir_all(storage_directory).map_err(|error|
            ServerOpenError::CouldNotCreateDataDirectory {
                path: storage_directory.to_owned(),
                source: error
            }
        )?;
        Ok(())
    }

}

#[derive(Debug)]
pub enum ServerOpenError {
    NotADirectory { path: PathBuf },
    CouldNotCreateDataDirectory { path: PathBuf, source: io::Error },
    DatabaseOpenError { source: DatabaseOpenError },
}

impl Error for ServerOpenError {}

impl fmt::Display for ServerOpenError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
