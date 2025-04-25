/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::Database;
use resource::constants::server::DEFAULT_USER_NAME;
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

    pub fn create(&self, user: &User, credential: &Credential) -> Result<(), UserCreateError> {
        match self.contains(&user.name) {
            Ok(contains) => {
                if contains {
                    return Err(UserCreateError::UserAlreadyExist {});
                }
            }
            Err(user_get_err) => {
                return match user_get_err {
                    UserGetError::IllegalUsername { .. } => Err(UserCreateError::IllegalUsername {}),
                    UserGetError::Unexpected { .. } => Err(UserCreateError::Unexpected {}),
                }
            }
        }
        let create_result = self
            .transaction_util
            .write_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _dbb, _tx_opts| {
                user_repository::create(
                    Arc::into_inner(snapshot).unwrap(),
                    &type_mgr,
                    thing_mgr.clone(),
                    &fn_mgr,
                    &query_mgr,
                    user,
                    credential,
                )
            })
            .1;
        match create_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_query_error)) => Err(UserCreateError::IllegalUsername {}),
            Err(_commit_error) => Err(UserCreateError::Unexpected {}),
        }
    }

    pub fn update(
        &self,
        username: &str,
        user: &Option<User>,
        credential: &Option<Credential>,
    ) -> Result<(), UserUpdateError> {
        let update_result = self
            .transaction_util
            .write_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                user_repository::update(
                    Arc::into_inner(snapshot).unwrap(),
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
                    return Err(UserDeleteError::UserDoesNotExist {});
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
            .write_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                user_repository::delete(
                    Arc::into_inner(snapshot).unwrap(),
                    &type_mgr,
                    thing_mgr.clone(),
                    &fn_mgr,
                    &query_mgr,
                    username,
                )
            })
            .1;
        match delete_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_query_error)) => Err(UserDeleteError::IllegalUsername {}),
            Err(_commit_error) => Err(UserDeleteError::Unexpected {}),
        }
    }
}
