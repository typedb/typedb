/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::Database;
use database::transaction::{DatabaseDropGuard, TransactionWrite};
use resource::constants::server::DEFAULT_USER_NAME;
use resource::profile::{CommitProfile, TransactionProfile};
use storage::durability_client::WALClient;
use storage::snapshot::WriteSnapshot;
use system::{
    concepts::{Credential, User},
    repositories::{user_repository, user_repository::SystemDBError},
    util::transaction_util::TransactionUtil,
};

use crate::errors::{UserCreateError, UserDeleteError, UserGetError, UserUpdateError};

#[derive(Debug)]
pub struct UserManager {
    transaction_util: TransactionUtil,
    system_db: Arc<Database<WALClient>>
}

impl UserManager {
    pub fn new(system_db: Arc<Database<WALClient>>) -> Self {
        UserManager { transaction_util: TransactionUtil::new(system_db.clone()), system_db }
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

    pub fn create(&self, user: &User, credential: &Credential) -> (TransactionProfile, Result<(DatabaseDropGuard<WALClient>, WriteSnapshot<WALClient>), UserCreateError>) {
        let (transaction_profile, create_result) = self
            .transaction_util
            .write_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _dbb, _tx_opts| {
                user_repository::create(snapshot, &type_mgr, thing_mgr.clone(), &fn_mgr, &query_mgr, user, credential)
            });
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
    ) -> Result<(), UserUpdateError> {
        let update_result = self
            .transaction_util
            .write_transaction_commit(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
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
            })
            .1;
        match update_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_query_error)) => Err(UserUpdateError::IllegalUsername {}),
            Err(_commit_error) => Err(UserUpdateError::Unexpected {}),
        }
    }

    pub fn delete(&self, username: &str) -> Result<(), UserDeleteError> {
        if username == DEFAULT_USER_NAME {
            return Err(UserDeleteError::DefaultUserCannotBeDeleted {});
        }
        match self.contains(username) {
            Ok(contains) => {
                if !contains {
                    return Err(UserDeleteError::UserNotFound {});
                }
            }
            Err(user_get_err) => {
                return match user_get_err {
                    UserGetError::IllegalUsername { .. } => Err(UserDeleteError::IllegalUsername {}),
                    UserGetError::Unexpected { .. } => Err(UserDeleteError::Unexpected {}),
                }
            }
        }
        let delete_result = self
            .transaction_util
            .write_transaction_commit(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                user_repository::delete(snapshot, &type_mgr, thing_mgr.clone(), &fn_mgr, &query_mgr, username)
            })
            .1;
        match delete_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_query_error)) => Err(UserDeleteError::IllegalUsername {}),
            Err(_commit_error) => Err(UserDeleteError::Unexpected {}),
        }
    }
}
