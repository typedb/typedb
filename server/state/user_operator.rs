/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt::Debug,
    sync::{Arc, RwLock as StdRwLock},
};

use async_trait::async_trait;
use database::database_manager::DatabaseManager;
use resource::constants::server::DEFAULT_USER_NAME;
use system::concepts::{Credential, User};
use user::{permission_manager::PermissionManager, user_manager::UserManager};

use super::TransactionOperator;
use crate::{
    authentication::{credential_verifier::CredentialVerifier, token_manager::TokenManager, Accessor},
    error::{arc_server_state_err, ArcServerStateError, LocalServerStateError},
    system_init::SYSTEM_DB,
};

#[async_trait]
pub trait UserOperator: Debug + Send + Sync {
    async fn all(&self, accessor: Accessor) -> Result<Vec<User>, ArcServerStateError>;

    async fn contains(&self, accessor: Accessor, name: &str) -> Result<bool, ArcServerStateError>;

    async fn get(&self, accessor: Accessor, name: &str) -> Result<User, ArcServerStateError>;

    async fn create(&self, accessor: Accessor, user: User, credential: Credential) -> Result<(), ArcServerStateError>;

    async fn update(
        &self,
        accessor: Accessor,
        username: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
    ) -> Result<(), ArcServerStateError>;

    async fn delete(&self, accessor: Accessor, username: &str) -> Result<(), ArcServerStateError>;

    async fn verify_password(&self, username: &str, password: &str) -> Result<(), ArcServerStateError>;

    async fn token_create(&self, username: String, password: String) -> Result<String, ArcServerStateError>;

    async fn token_get_owner(&self, token: &str) -> Option<String>;

    fn manager(&self) -> Result<Arc<UserManager>, ArcServerStateError>;

    fn is_initialised(&self) -> bool;
}

#[derive(Debug)]
pub struct LocalUserOperator {
    database_manager: Arc<DatabaseManager>,
    token_manager: Arc<TokenManager>,
    user_manager: StdRwLock<Option<Arc<UserManager>>>,
    credential_verifier: StdRwLock<Option<Arc<CredentialVerifier>>>,
    transaction_operator: Arc<dyn TransactionOperator>,
}

impl LocalUserOperator {
    pub fn new(
        database_manager: Arc<DatabaseManager>,
        token_manager: Arc<TokenManager>,
        transaction_operator: Arc<dyn TransactionOperator>,
    ) -> Self {
        Self {
            database_manager,
            token_manager,
            user_manager: StdRwLock::new(None),
            credential_verifier: StdRwLock::new(None),
            transaction_operator,
        }
    }

    fn try_load_system_managers(&self) {
        if let Some(system_db) = self.database_manager.database_unrestricted(SYSTEM_DB) {
            let user_manager = Arc::new(UserManager::new(system_db));
            let credential_verifier = Arc::new(CredentialVerifier::new(user_manager.clone()));
            *self.user_manager.write().unwrap() = Some(user_manager);
            *self.credential_verifier.write().unwrap() = Some(credential_verifier);
        }
    }

    fn get_user_manager(&self) -> Result<Arc<UserManager>, LocalServerStateError> {
        if let Some(um) = self.user_manager.read().unwrap().clone() {
            return Ok(um);
        }
        self.try_load_system_managers();
        self.user_manager.read().unwrap().clone().ok_or(LocalServerStateError::NotInitialised {})
    }

    fn get_credential_verifier(&self) -> Result<Arc<CredentialVerifier>, LocalServerStateError> {
        if let Some(cv) = self.credential_verifier.read().unwrap().clone() {
            return Ok(cv);
        }
        self.try_load_system_managers();
        self.credential_verifier.read().unwrap().clone().ok_or(LocalServerStateError::NotInitialised {})
    }
}

#[async_trait]
impl UserOperator for LocalUserOperator {
    async fn all(&self, accessor: Accessor) -> Result<Vec<User>, ArcServerStateError> {
        if !PermissionManager::exec_user_all_permitted(accessor.as_str()) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        match self.get_user_manager() {
            Ok(user_manager) => Ok(user_manager.all()),
            Err(err) => Err(Arc::new(err)),
        }
    }

    async fn contains(&self, accessor: Accessor, name: &str) -> Result<bool, ArcServerStateError> {
        if !PermissionManager::exec_user_get_permitted(accessor.as_str(), name) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        match self.get_user_manager() {
            Ok(user_manager) => match user_manager.contains(name) {
                Ok(bool) => Ok(bool),
                Err(typedb_source) => Err(Arc::new(LocalServerStateError::UserCannotBeRetrieved { typedb_source })),
            },
            Err(err) => Err(Arc::new(err)),
        }
    }

    async fn get(&self, accessor: Accessor, name: &str) -> Result<User, ArcServerStateError> {
        if !PermissionManager::exec_user_get_permitted(accessor.as_str(), name) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        match self.get_user_manager() {
            Ok(user_manager) => match user_manager.get(name) {
                Ok(get) => match get {
                    Some((user, _)) => Ok(user),
                    None => Err(Arc::new(LocalServerStateError::UserNotFound {})),
                },
                Err(typedb_source) => Err(Arc::new(LocalServerStateError::UserCannotBeRetrieved { typedb_source })),
            },
            Err(err) => Err(Arc::new(err)),
        }
    }

    async fn create(&self, accessor: Accessor, user: User, credential: Credential) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_create_permitted(accessor.as_str()) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = self.get_user_manager().map_err(arc_server_state_err)?;
        user_manager
            .create(&user, &credential)
            .map_err(|typedb_source| arc_server_state_err(LocalServerStateError::UserCannotBeCreated { typedb_source }))
    }

    async fn update(
        &self,
        accessor: Accessor,
        username: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
    ) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_update_permitted(accessor.as_str(), username) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = self.get_user_manager().map_err(arc_server_state_err)?;
        user_manager.update(username, &user_update, &credential_update).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::UserCannotBeUpdated { typedb_source })
        })?;

        self.token_manager.invalidate_user(username).await;
        self.transaction_operator.close_by_owner(username).await;
        Ok(())
    }

    async fn delete(&self, accessor: Accessor, username: &str) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_delete_allowed(accessor.as_str(), username) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = self.get_user_manager().map_err(arc_server_state_err)?;
        user_manager.delete(username).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::UserCannotBeDeleted { typedb_source })
        })?;

        self.token_manager.invalidate_user(username).await;
        self.transaction_operator.close_by_owner(username).await;
        Ok(())
    }

    async fn verify_password(&self, username: &str, password: &str) -> Result<(), ArcServerStateError> {
        if !self.is_initialised() {
            return Err(Arc::new(LocalServerStateError::NotInitialised {}));
        }
        match self.get_credential_verifier() {
            Ok(credential_verifier) => match credential_verifier.verify_password(username, password) {
                Ok(()) => Ok(()),
                Err(typedb_source) => Err(Arc::new(LocalServerStateError::AuthenticationError { typedb_source })),
            },
            Err(err) => Err(Arc::new(err)),
        }
    }

    async fn token_create(&self, username: String, password: String) -> Result<String, ArcServerStateError> {
        self.verify_password(&username, &password).await?;
        Ok(self.token_manager.new_token(username).await)
    }

    async fn token_get_owner(&self, token: &str) -> Option<String> {
        self.token_manager.get_valid_token_owner(token).await
    }

    fn manager(&self) -> Result<Arc<UserManager>, ArcServerStateError> {
        self.get_user_manager().map_err(arc_server_state_err)
    }

    fn is_initialised(&self) -> bool {
        let Some(system_db) = self.database_manager.database_unrestricted(SYSTEM_DB) else {
            return false;
        };
        let user_manager = UserManager::new(system_db);
        user_manager.contains(DEFAULT_USER_NAME).unwrap_or(false)
    }
}
