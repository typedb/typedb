/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, fs, io, path::PathBuf, sync::Arc};

use database::{database_manager::DatabaseManager, DatabaseOpenError};
use resource::constants::server::GRPC_CONNECTION_KEEPALIVE;
use system::initialise_system_database;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use user::{initialise_default_user, user_manager::UserManager};

use crate::{
    authenticator::Authenticator,
    authenticator_cache::AuthenticatorCache,
    parameters::config::{Config, EncryptionConfig},
    service::typedb_service::TypeDBService,
};

#[derive(Debug)]
pub struct Server {
    data_directory: PathBuf,
    user_manager: Arc<UserManager>,
    authenticator_cache: Arc<AuthenticatorCache>,
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
        let authenticator_cache = Arc::new(AuthenticatorCache::new());
        initialise_default_user(&user_manager);
        let typedb_service = TypeDBService::new(
            &config.server.address,
            database_manager,
            user_manager.clone(),
            authenticator_cache.clone(),
        );
        println!("Storage directory: {:?}", storage_directory);
        Ok(Self {
            data_directory: storage_directory.to_owned(),
            user_manager,
            authenticator_cache,
            typedb_service: Some(typedb_service),
            config,
        })
    }

    pub fn database_manager(&self) -> &DatabaseManager {
        self.typedb_service.as_ref().unwrap().database_manager()
    }

    pub async fn serve(mut self) -> Result<(), tonic::transport::Error> {
        let service = typedb_protocol::type_db_server::TypeDbServer::new(self.typedb_service.take().unwrap());
        let authenticator = Authenticator::new(self.user_manager.clone(), self.authenticator_cache.clone());
        println!("Ready!");
        Self::create_tonic_server(&self.config.server.encryption)
            .layer(&authenticator)
            .add_service(service)
            .serve(self.config.server.address)
            .await
    }

    fn create_tonic_server(encryption_config: &EncryptionConfig) -> tonic::transport::Server {
        let mut tonic_server = tonic::transport::Server::builder();
        if encryption_config.enabled {
            let cert = fs::read_to_string(encryption_config.cert.clone().unwrap()).unwrap();
            let cert_key = fs::read_to_string(encryption_config.cert_key.clone().unwrap()).unwrap();
            let mut tls_config = ServerTlsConfig::new().identity(Identity::from_pem(cert, cert_key));
            if encryption_config.root_ca.is_some() {
                let root_ca = fs::read_to_string(encryption_config.root_ca.clone().unwrap()).unwrap();
                tls_config = tls_config.client_ca_root(Certificate::from_pem(root_ca)).client_auth_optional(true)
            }
            tonic_server = tonic_server.tls_config(tls_config).unwrap();
        }
        tonic_server.http2_keepalive_interval(Some(GRPC_CONNECTION_KEEPALIVE))
    }

    fn create_storage_directory(storage_directory: &PathBuf) -> Result<(), ServerOpenError> {
        fs::create_dir_all(storage_directory).map_err(|error| ServerOpenError::CouldNotCreateDataDirectory {
            path: storage_directory.to_owned(),
            source: error,
        })?;
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
