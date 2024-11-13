use std::sync::Arc;
use system::concepts::{Credential, User};
use system::repositories::{credential_repository, user_repository};
use system::transaction_manager::TransactionManager;
use crate::errors::{UserCreateError, UserDeleteError};
use database::Database;
use storage::durability_client::WALClient;

#[derive(Debug)]
pub struct UserManager {
    transaction_manager: TransactionManager
}

impl UserManager {
    pub fn new(system_db: Arc<Database<WALClient>>) -> Self {
        UserManager {
            transaction_manager: TransactionManager::new(system_db.clone()),
        }
    }

    pub fn all(&self) -> Vec<User> {
        self.transaction_manager.read_transaction(|tx| {
            user_repository::list(&tx)
        })
    }

    pub fn get(&self, name: &str) -> Option<User> {
        self.transaction_manager.read_transaction(|tx| {
            user_repository::get(&tx, name)
        })
    }

    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    pub fn create(&self, user: &User, credential: &Credential) -> Result<(), UserCreateError> {
        self.transaction_manager.write_transaction(|tx| {
            user_repository::create(&tx, &user);
            credential_repository::create(&tx, &user.name, credential);
        }).map_err(|e| UserCreateError::Unexpected {})
    }

    pub fn delete(&self, username: &str) -> Result<(), UserDeleteError> {
        self.transaction_manager.write_transaction(|tx| {
            user_repository::delete(&tx, username);
            credential_repository::delete_by_user(&tx, username);
        }).map_err(|_| UserDeleteError::Unexpected {})
    }
}
