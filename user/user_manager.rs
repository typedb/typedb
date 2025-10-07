/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::{transaction::DataCommitIntent, Database};
use resource::{constants::server::DEFAULT_USER_NAME, profile::TransactionProfile};
use storage::durability_client::WALClient;
use system::{
    concepts::{Credential, User},
    repositories::{user_repository, user_repository::SystemDBError},
    util::transaction_util::TransactionUtil,
};

use crate::errors::{UserCreateError, UserDeleteError, UserGetError, UserUpdateError};

#[derive(Debug)]
pub struct UserManager {
    transaction_util: TransactionUtil,
}

impl UserManager {
    pub fn new(system_db: Arc<Database<WALClient>>) -> Self {
        UserManager { transaction_util: TransactionUtil::new(system_db.clone()) }
    }

    pub fn all(&self) -> Vec<User> {
        self.transaction_util.read_transaction(user_repository::list)
    }

    pub fn get(&self, username: &str) -> Result<Option<(User, Credential)>, UserGetError> {
        self.transaction_util.read_transaction(|tx| {
            user_repository::get(tx, username).map_err(|query_error| match query_error {
                SystemDBError::IllegalQueryInput { .. } => UserGetError::IllegalUsername {},
                SystemDBError::EmptyUpdate { .. } => UserGetError::Unexpected {},
            })
        })
    }

    pub fn contains(&self, username: &str) -> Result<bool, UserGetError> {
        self.get(username).map(|opt| opt.is_some())
    }

    pub fn create(
        &self,
        user: &User,
        credential: &Credential,
    ) -> (TransactionProfile, Result<DataCommitIntent<WALClient>, UserCreateError>) {
        let (transaction_profile, create_result) = self.transaction_util.write_transaction(
            |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                user_repository::create(snapshot, &type_mgr, thing_mgr.clone(), &fn_mgr, &query_mgr, user, credential)
            },
        );
        let create_result = match create_result {
            Ok(tuple) => Ok(tuple),
            Err(_query_error) => Err(UserCreateError::IllegalUsername {}),
        };
        (transaction_profile, create_result)
    }

    pub fn update(
        &self,
        username: &str,
        user: &Option<User>,
        credential: &Option<Credential>,
    ) -> (TransactionProfile, Result<DataCommitIntent<WALClient>, UserUpdateError>) {
        let (transaction_profile, update_result) = self.transaction_util.write_transaction(
            |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                user_repository::update(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    &fn_mgr,
                    &query_mgr,
                    username,
                    user,
                    credential,
                )
            },
        );
        let update_result =
            update_result.map(|tuple| tuple).map_err(|_query_error| UserUpdateError::IllegalUsername {});
        (transaction_profile, update_result)
    }

    pub fn delete(
        &self,
        username: &str,
    ) -> (Option<TransactionProfile>, Result<DataCommitIntent<WALClient>, UserDeleteError>) {
        if username == DEFAULT_USER_NAME {
            return (None, Err(UserDeleteError::DefaultUserCannotBeDeleted {}));
        }
        match self.contains(username) {
            Ok(contains) => {
                if !contains {
                    return (None, Err(UserDeleteError::UserNotFound {}));
                }
            }
            Err(user_get_err) => {
                return match user_get_err {
                    UserGetError::IllegalUsername { .. } => (None, Err(UserDeleteError::IllegalUsername {})),
                    UserGetError::Unexpected { .. } => (None, Err(UserDeleteError::Unexpected {})),
                }
            }
        }

        let (transaction_profile, delete_result) = self.transaction_util.write_transaction(
            |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                user_repository::delete(snapshot, &type_mgr, thing_mgr.clone(), &fn_mgr, &query_mgr, username)
            },
        );

        let delete_result =
            delete_result.map(|tuple| tuple).map_err(|_query_error| UserDeleteError::IllegalUsername {});

        (Some(transaction_profile), delete_result)
    }
}
