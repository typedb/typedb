/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, RwLock as StdRwLock},
};

use async_trait::async_trait;
use database::{database_manager::DatabaseManager, transaction::TransactionId};
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot};
use system::concepts::{Credential, User};
use tokio::sync::{mpsc::Sender, RwLock};
use user::{permission_manager::PermissionManager, user_manager::UserManager};

use crate::{
    authentication::{credential_verifier::CredentialVerifier, token_manager::TokenManager, Accessor},
    error::{arc_server_state_err, ArcServerStateError, LocalServerStateError},
    system_init::SYSTEM_DB,
};

use super::TransactionInfo;

#[async_trait]
pub trait ServerUserManager: Debug + Send + Sync {
    async fn users_all(&self, accessor: Accessor) -> Result<Vec<User>, ArcServerStateError>;

    async fn users_contains(&self, accessor: Accessor, name: &str) -> Result<bool, ArcServerStateError>;

    async fn users_get(&self, accessor: Accessor, name: &str) -> Result<User, ArcServerStateError>;

    async fn users_create(
        &self,
        accessor: Accessor,
        user: User,
        credential: Credential,
    ) -> Result<(), ArcServerStateError>;

    async fn users_update(
        &self,
        accessor: Accessor,
        username: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
    ) -> Result<(), ArcServerStateError>;

    async fn users_delete(&self, accessor: Accessor, username: &str) -> Result<(), ArcServerStateError>;

    async fn user_verify_password(&self, username: &str, password: &str) -> Result<(), ArcServerStateError>;

    async fn token_create(&self, username: String, password: String) -> Result<String, ArcServerStateError>;

    async fn token_get_owner(&self, token: &str) -> Option<String>;

    fn manager(&self) -> Option<Arc<UserManager>>;

    async fn load(&self);
}

#[derive(Debug)]
pub struct LocalServerUserManager {
    database_manager: Arc<DatabaseManager>,
    token_manager: Arc<TokenManager>,
    user_manager: StdRwLock<Option<Arc<UserManager>>>,
    credential_verifier: StdRwLock<Option<Arc<CredentialVerifier>>>,
    transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
}

impl LocalServerUserManager {
    pub fn new(
        database_manager: Arc<DatabaseManager>,
        token_manager: Arc<TokenManager>,
        transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
    ) -> Self {
        Self {
            database_manager,
            token_manager,
            user_manager: StdRwLock::new(None),
            credential_verifier: StdRwLock::new(None),
            transactions,
        }
    }

    fn get_user_manager(&self) -> Result<Arc<UserManager>, LocalServerStateError> {
        match self.user_manager.read().unwrap().clone() {
            Some(user_manager) => Ok(user_manager),
            None => Err(LocalServerStateError::NotInitialised {}),
        }
    }

    fn get_credential_verifier(&self) -> Result<Arc<CredentialVerifier>, LocalServerStateError> {
        match self.credential_verifier.read().unwrap().clone() {
            Some(credential_verifier) => Ok(credential_verifier),
            None => Err(LocalServerStateError::NotInitialised {}),
        }
    }

    async fn close_user_transactions(&self, username: &str) {
        let transactions = self.transactions.read().await;
        for (_, TransactionInfo { owner, close_sender, .. }) in transactions.iter() {
            if username == owner {
                let _ = close_sender.send(()).await;
            }
        }
    }
}

#[async_trait]
impl ServerUserManager for LocalServerUserManager {
    async fn users_all(&self, accessor: Accessor) -> Result<Vec<User>, ArcServerStateError> {
        if !PermissionManager::exec_user_all_permitted(accessor.as_str()) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        match self.get_user_manager() {
            Ok(user_manager) => Ok(user_manager.all()),
            Err(err) => Err(Arc::new(err)),
        }
    }

    async fn users_contains(&self, accessor: Accessor, name: &str) -> Result<bool, ArcServerStateError> {
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

    async fn users_get(&self, accessor: Accessor, name: &str) -> Result<User, ArcServerStateError> {
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

    async fn users_create(
        &self,
        accessor: Accessor,
        user: User,
        credential: Credential,
    ) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_create_permitted(accessor.as_str()) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = self.manager().ok_or(LocalServerStateError::NotInitialised {})?;

        let (mut transaction_profile, commit_intent) = user_manager
            .create(&user, &credential)
            .map_err(|(_, typedb_source)| LocalServerStateError::UserCannotBeCreated { typedb_source })?;

        let commit_profile = transaction_profile.commit_profile();
        let database = self.database_manager.database_unrestricted(SYSTEM_DB).unwrap();
        database.data_commit_with_snapshot(commit_intent.write_snapshot, commit_profile).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabaseDataCommitFailed { typedb_source })
        })
    }

    async fn users_update(
        &self,
        accessor: Accessor,
        username: &str,
        user_update: Option<User>,
        credential_update: Option<Credential>,
    ) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_update_permitted(accessor.as_str(), username) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = self.manager().ok_or(LocalServerStateError::NotInitialised {})?;

        let (mut transaction_profile, commit_intent_result) =
            user_manager.update(username, &user_update, &credential_update);

        let commit_intent = commit_intent_result
            .map_err(|typedb_source| LocalServerStateError::UserCannotBeUpdated { typedb_source })?;

        let commit_profile = transaction_profile.commit_profile();
        let database = self.database_manager.database_unrestricted(SYSTEM_DB).unwrap();
        database.data_commit_with_snapshot(commit_intent.write_snapshot, commit_profile).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabaseDataCommitFailed { typedb_source })
        })?;

        self.token_manager.invalidate_user(username).await;
        self.close_user_transactions(username).await;
        Ok(())
    }

    async fn users_delete(&self, accessor: Accessor, username: &str) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_delete_allowed(accessor.as_str(), username) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = self.manager().ok_or(LocalServerStateError::NotInitialised {})?;

        let (mut transaction_profile, commit_intent) = user_manager
            .delete(username)
            .map_err(|(_, typedb_source)| LocalServerStateError::UserCannotBeDeleted { typedb_source })?;

        let commit_profile = transaction_profile.commit_profile();
        let database = self.database_manager.database_unrestricted(SYSTEM_DB).unwrap();
        database.data_commit_with_snapshot(commit_intent.write_snapshot, commit_profile).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabaseDataCommitFailed { typedb_source })
        })?;

        self.token_manager.invalidate_user(username).await;
        self.close_user_transactions(username).await;
        Ok(())
    }

    async fn user_verify_password(&self, username: &str, password: &str) -> Result<(), ArcServerStateError> {
        match self.get_credential_verifier() {
            Ok(credential_verifier) => match credential_verifier.verify_password(username, password) {
                Ok(()) => Ok(()),
                Err(typedb_source) => Err(Arc::new(LocalServerStateError::AuthenticationError { typedb_source })),
            },
            Err(err) => Err(Arc::new(err)),
        }
    }

    async fn token_create(&self, username: String, password: String) -> Result<String, ArcServerStateError> {
        self.user_verify_password(&username, &password).await?;
        Ok(self.token_manager.new_token(username).await)
    }

    async fn token_get_owner(&self, token: &str) -> Option<String> {
        self.token_manager.get_valid_token_owner(token).await
    }

    fn manager(&self) -> Option<Arc<UserManager>> {
        self.user_manager.read().unwrap().clone()
    }

    async fn load(&self) {
        let system_database = self.database_manager.database_unrestricted(SYSTEM_DB).unwrap();
        let user_manager = Arc::new(UserManager::new(system_database));
        let credential_verifier = Arc::new(CredentialVerifier::new(user_manager.clone()));
        *self.user_manager.write().unwrap() = Some(user_manager);
        *self.credential_verifier.write().unwrap() = Some(credential_verifier);
    }
}
