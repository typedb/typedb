pub mod user_repository {
    use crate::concepts::User;
    use database::transaction::{TransactionRead, TransactionWrite};
    use storage::durability_client::WALClient;

    pub fn list(tx: &TransactionRead<WALClient>) -> Vec<User> {
        println!("listing users");
        todo!()
    }

    pub fn get(tx: &TransactionRead<WALClient>, username: &str) -> Option<User> {
        println!("getting user {}", username);
        todo!()
    }

    pub fn create(tx: &TransactionWrite<WALClient>, user: &User) {
        println!("creating user {:?}", user);
        todo!()
    }

    pub fn update(
        tx: &TransactionWrite<WALClient>, username: &str, update: &User
    ) {
        todo!()
    }

    pub fn delete(tx: &TransactionWrite<WALClient>, username: &str) {
        println!("deleting user {}", username);
        todo!()
    }
}

pub mod credential_repository {
    use crate::concepts::Credential;
    use database::transaction::TransactionWrite;
    use storage::durability_client::WALClient;

    pub fn create(
        tx: &TransactionWrite<WALClient>, username: &str, credential: &Credential
    ) {
        println!("creating credential for user {}: {:?}", username, credential);
        todo!()
    }

    pub fn update(
        tx: &TransactionWrite<WALClient>, username: &str, credential: &Credential
    ) {
        todo!()
    }

    pub fn delete_by_user(tx: &&TransactionWrite<WALClient>, username: &str) -> Vec<Credential> {
        println!("deleting credentials for user {}", username);
        todo!()
    }
}