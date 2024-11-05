use crate::concepts::{Credential, User};
use database::transaction::{TransactionRead, TransactionWrite};
use storage::durability_client::WALClient;

#[derive(Debug)]
pub struct UserRepository {}

impl UserRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub fn list(&self, tx: &TransactionRead<WALClient>) -> Vec<User> {
        println!("listing users");
        todo!()
    }

    pub fn get(&self, tx: &TransactionRead<WALClient>, username: &str) -> Option<User> {
        println!("getting user {}", username);
        todo!()
    }

    pub fn create(&self, tx: &TransactionWrite<WALClient>, user: &User) {
        println!("creating user {:?}", user);
        todo!()
    }

    pub fn update(
        &self, tx: &TransactionWrite<WALClient>, username: &str, update: &User
    ) {
        todo!()
    }

    pub fn delete(&self, tx: &TransactionWrite<WALClient>, username: &str) {
        println!("deleting user {}", username);
        todo!()
    }
}

#[derive(Debug)]
pub struct CredentialRepository {}

impl CredentialRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create(
        &self, tx: &TransactionWrite<WALClient>, username: &str, credential: &Credential
    ) {
        println!("creating credential for user {}: {:?}", username, credential);
        todo!()
    }

    pub fn update(
        &self, tx: &TransactionWrite<WALClient>, username: &str, credential: &Credential
    ) {
        todo!()
    }

    pub fn delete_by_user(&self, tx: &&TransactionWrite<WALClient>, username: &str) -> Vec<Credential> {
        println!("deleting credentials for user {}", username);
        todo!()
    }
}