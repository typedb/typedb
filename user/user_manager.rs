use std::sync::Arc;

use database::Database;
use storage::durability_client::WALClient;
use system::{
    concepts::{Credential, User},
    repositories::user_repository,
    util::transaction_util::TransactionUtil,
};

use crate::errors::{UserCreateError, UserDeleteError, UserUpdateError};

#[derive(Debug)]
pub struct UserManager {
    transaction_manager: TransactionUtil,
}

impl UserManager {
    pub fn new(system_db: Arc<Database<WALClient>>) -> Self {
        UserManager { transaction_manager: TransactionUtil::new(system_db.clone()) }
    }

    pub fn all(&self) -> Vec<User> {
        self.transaction_manager.read_transaction(|tx| user_repository::list(tx))
    }

    pub fn get(&self, username: &str) -> Option<(User, Credential)> {
        self.transaction_manager.read_transaction(|tx| user_repository::get(tx, username))
    }

    pub fn contains(&self, username: &str) -> bool {
        self.get(username).is_some()
    }

    pub fn create(&self, user: &User, credential: &Credential) -> Result<(), UserCreateError> {
        let commit =
            self.transaction_manager.write_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, db, tx_opts| {
                let snapshot = user_repository::create(
                    Arc::into_inner(snapshot).unwrap(),
                    &type_mgr,
                    thing_mgr.clone(),
                    &fn_mgr,
                    user,
                    credential,
                );
                ((), snapshot)
            });
        commit.map_err(|e| UserCreateError::Unexpected {})
    }

    pub fn update(
        &self,
        username: &str,
        user: &Option<User>,
        credential: &Option<Credential>,
    ) -> Result<(), UserUpdateError> {
        let commit =
            self.transaction_manager.write_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, db, tx_opts| {
                let snapshot = user_repository::update(
                    Arc::into_inner(snapshot).unwrap(),
                    &type_mgr,
                    thing_mgr.clone(),
                    &fn_mgr,
                    username,
                    user,
                    credential,
                );
                ((), snapshot)
            });
        commit.map_err(|e| UserUpdateError::Unexpected {})
    }

    pub fn delete(&self, username: &str) -> Result<(), UserDeleteError> {
        let commit =
            self.transaction_manager.write_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, db, tx_opts| {
                let snapshot = user_repository::delete(
                    Arc::into_inner(snapshot).unwrap(),
                    &type_mgr,
                    thing_mgr.clone(),
                    &fn_mgr,
                    username,
                );
                ((), snapshot)
            });
        commit.map_err(|e| UserDeleteError::Unexpected {})
    }
}
