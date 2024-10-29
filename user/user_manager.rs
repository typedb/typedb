use system::concepts::{Credential, User, Password};
use system::repositories::{CredentialRepository, UserRepository};
use system::transaction_helper::{read_transaction, write_transaction};
use crate::errors::UserCreateError;

#[derive(Debug)]
pub struct UserManager {
    user_repository: UserRepository,
    credential_repository: CredentialRepository
}

impl UserManager {
    pub fn new() -> Self {
        UserManager {
            user_repository: UserRepository::new(),
            credential_repository: CredentialRepository::new()
        }
    }

    pub fn all(&self) -> Vec<User> {
        read_transaction(|tx| {
            self.user_repository.list(&tx)
        })
    }

    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    pub fn get(&self, name: &str) -> Option<User> {
        read_transaction(|tx| {
            self.user_repository.get(&tx, name)
        })
    }

    pub fn create(&self, user: &User, credential: &Credential) -> Result<(), UserCreateError>{
        write_transaction(|tx| {
            self.user_repository.create(&tx, &user);
            self.credential_repository.create(&tx, &user.name, credential);
        }).map_err(|_| todo!())
    }
}

