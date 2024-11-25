use crate::errors::{UserCreateError, UserDeleteError, UserUpdateError};
use database::Database;
use std::sync::Arc;
use storage::durability_client::WALClient;
use system::concepts::{Credential, User};
use system::repositories::user_repository;
use system::util::transaction_util::TransactionUtil;

#[derive(Debug)]
pub struct UserManager {
    transaction_manager: TransactionUtil
}

impl UserManager {
    pub fn new(system_db: Arc<Database<WALClient>>) -> Self {
        UserManager {
            transaction_manager: TransactionUtil::new(system_db.clone()),
        }
    }

    pub fn all(&self) -> Vec<User> {
        self.transaction_manager.read_transaction(|tx| {
            user_repository::list(tx)
        })
    }

    pub fn get(&self, username: &str) -> Option<(User, Credential)> {
        self.transaction_manager.read_transaction(|tx| {
            user_repository::get(tx, username)
        })
    }

    pub fn contains(&self, username: &str) -> bool {
        self.get(username).is_some()
    }

    pub fn create(&self, user: &User, credential: &Credential) -> Result<(), UserCreateError> {
        let commit = self.transaction_manager.write_transaction(
            |snapshot,
             type_mgr,
             thing_mgr,
             fn_mgr,
             db,
             tx_opts| {
            let snapshot = user_repository::create(
                Arc::into_inner(snapshot).unwrap(),
                &type_mgr,
                thing_mgr.clone(),
                &fn_mgr,
                user,
                credential
            );
            ((), snapshot)
        });
        commit.map_err(|e| UserCreateError::Unexpected {})
    }

    pub fn update(&self, username: &str, user: &User, credential: &Credential) -> Result<(), UserUpdateError> {
        let commit = self.transaction_manager.write_transaction(
            |snapshot,
             type_mgr,
             thing_mgr,
             fn_mgr,
             db,
             tx_opts| {
                let snapshot = user_repository::update(
                    Arc::into_inner(snapshot).unwrap(),
                    &type_mgr,
                    thing_mgr.clone(),
                    &fn_mgr,
                    username,
                    user,
                    credential
                );
                ((), snapshot)
            });
        commit.map_err(|e| UserUpdateError::Unexpected {})
    }

    pub fn delete(&self, username: &str) -> Result<(), UserDeleteError> {
        let commit = self.transaction_manager.write_transaction(
            |snapshot,
             type_mgr,
             thing_mgr,
             fn_mgr,
             db,
             tx_opts| {
                let snapshot = user_repository::delete(
                    Arc::into_inner(snapshot).unwrap(),
                    &type_mgr,
                    thing_mgr.clone(),
                    &fn_mgr,
                    username
                );
                ((), snapshot)
            });
        commit.map_err(|e| UserDeleteError::Unexpected {})
    }
}
