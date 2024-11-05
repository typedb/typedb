use std::sync::Arc;
use system::concepts::{Credential, User};
use system::repositories::{CredentialRepository, UserRepository};
use system::transaction_helper::{read_transaction, write_transaction};
use crate::errors::{UserCreateError, UserDeleteError};
use database::Database;
use storage::durability_client::WALClient;

#[derive(Debug)]
pub struct UserManager {
    system_db: Arc<Database<WALClient>>,
    user_repository: UserRepository,
    credential_repository: CredentialRepository
}

impl UserManager {
    pub fn new(system_db: Arc<Database<WALClient>>) -> Self {
        UserManager {
            system_db,
            user_repository: UserRepository::new(),
            credential_repository: CredentialRepository::new()
        }
    }

    pub fn all(&self) -> Vec<User> {
        read_transaction(self.system_db.clone(), |tx| {
            self.user_repository.list(&tx)
        })
    }

    pub fn get(&self, name: &str) -> Option<User> {
        read_transaction(self.system_db.clone(), |tx| {
            self.user_repository.get(&tx, name)
        })
    }

    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    pub fn create(&self, user: &User, credential: &Credential) -> Result<(), UserCreateError> {
        write_transaction(self.system_db.clone(), |tx| {
            self.user_repository.create(&tx, &user);
            self.credential_repository.create(&tx, &user.name, credential);
        }).map_err(|e| UserCreateError::Unexpected {})
    }

    pub fn delete(&self, username: &str) -> Result<(), UserDeleteError> {
        write_transaction(self.system_db.clone(), |tx| {
            self.user_repository.delete(&tx, username);
            self.credential_repository.delete_by_user(&tx, username);
        }).map_err(|_| UserDeleteError::Unexpected {})
    }
}
