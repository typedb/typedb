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
        todo!()
    }

    pub fn get(&self, tx: &TransactionRead<WALClient>, username: &str) -> Option<User> {
        todo!()
    }

    pub fn create(&self, tx: &TransactionWrite<WALClient>, user: &User) {
        todo!()
    }

    pub fn update(
        &self, tx: &TransactionWrite<WALClient>, username: &str, update: &User
    ) {
        todo!()
    }

    pub fn delete(&self, tx: &TransactionWrite<WALClient>, username: &str) {
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
        todo!()
    }

    pub fn update(
        &self, tx: &TransactionWrite<WALClient>, username: &str, credential: &Credential
    ) {
        todo!()
    }

    pub fn get(username: &str) {
    }
}