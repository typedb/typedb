/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf}
    ,
};

use database::database_manager::DatabaseManager;
use database::DatabaseOpenError;

use crate::service::typedb_service::TypeDBService;

#[derive(Debug)]
pub struct Server {
    data_directory: PathBuf,
    typedb_service: Option<TypeDBService>,
}

impl Server {
    pub fn open(data_directory: impl AsRef<Path>) -> Result<Self, ServerOpenError> {
        use ServerOpenError::{CouldNotCreateDataDirectory, NotADirectory};
        let data_directory = data_directory.as_ref();

        if !data_directory.exists() {
            fs::create_dir_all(data_directory)
                .map_err(|error| CouldNotCreateDataDirectory { path: data_directory.to_owned(), source: error })?;
        } else if !data_directory.is_dir() {
            return Err(NotADirectory { path: data_directory.to_owned() });
        }

        let database_manager = DatabaseManager::new(data_directory)
            .map_err(|err| ServerOpenError::DatabaseOpenError { source: err })?;
        let data_directory = data_directory.to_owned();

        let typedb_service = TypeDBService::new(database_manager);

        Ok(Self { data_directory, typedb_service: Some(typedb_service) })
    }

    pub fn database_manager(&self) -> &DatabaseManager {
        self.typedb_service.as_ref().unwrap().database_manager()
    }

    pub async fn serve(self) -> Result<(), Box<dyn Error>> {
        // let address = "localhost:1729".parse().unwrap();

        // TODO: could also construct in Server and await here only
        // Server::builder()
            // .http2_keepalive_interval()
            // .add_service(self.typedb_service.take().unwrap())
            // .serve(address)
            // .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum ServerOpenError {
    NotADirectory { path: PathBuf },
    CouldNotCreateDataDirectory { path: PathBuf, source: io::Error },
    DatabaseOpenError { source: DatabaseOpenError }
}

impl Error for ServerOpenError {}

impl fmt::Display for ServerOpenError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
