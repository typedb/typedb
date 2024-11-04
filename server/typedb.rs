/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, fs, io, path::PathBuf};

use database::{database_manager::DatabaseManager, DatabaseOpenError};
use resource::constants::server::GRPC_CONNECTION_KEEPALIVE;

use crate::{parameters::config::Config, service::typedb_service::TypeDBService};

#[derive(Debug)]
pub struct Server {
    data_directory: PathBuf,
    typedb_service: Option<TypeDBService>,
    config: Config,
}

impl Server {
    pub fn open(config: Config) -> Result<Self, ServerOpenError> {
        use ServerOpenError::{CouldNotCreateDataDirectory, NotADirectory};
        let storage_directory = &config.storage.data;

        if !storage_directory.exists() {
            fs::create_dir_all(storage_directory)
                .map_err(|error| CouldNotCreateDataDirectory { path: storage_directory.to_owned(), source: error })?;
        } else if !storage_directory.is_dir() {
            return Err(NotADirectory { path: storage_directory.to_owned() });
        }

        let database_manager = DatabaseManager::new(storage_directory)
            .map_err(|err| ServerOpenError::DatabaseOpenError { source: err })?;
        let data_directory = storage_directory.to_owned();

        let typedb_service = TypeDBService::new(&config.server.address, database_manager);

        Ok(Self { data_directory, typedb_service: Some(typedb_service), config })
    }

    pub fn database_manager(&self) -> &DatabaseManager {
        self.typedb_service.as_ref().unwrap().database_manager()
    }

    pub async fn serve(mut self) -> Result<(), tonic::transport::Error> {
        let service = typedb_protocol::type_db_server::TypeDbServer::new(self.typedb_service.take().unwrap());
        println!("Ready!");
        tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(GRPC_CONNECTION_KEEPALIVE))
            .add_service(service)
            .serve(self.config.server.address)
            .await
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
