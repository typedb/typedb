/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use storage::snapshot::CommittableSnapshot;
use user::{
    errors::{UserCreateError, UserDeleteError, UserUpdateError},
    permission_manager::PermissionManager,
};

use crate::{
    authentication::Accessor,
    error::{ArcServerStateError, LocalServerStateError},
    state::ArcServerState,
    system_init::SYSTEM_DB,
};

pub struct TypeDBService;

impl TypeDBService {
    pub async fn create_user(
        server_state: &ArcServerState,
        accessor: Accessor,
        user: system::concepts::User,
        credential: system::concepts::Credential,
    ) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_create_permitted(accessor.as_str()) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = server_state.user_manager().await.ok_or(LocalServerStateError::NotInitialised {})?;

        let create_result = user_manager.create(&user, &credential);

        let (mut transaction_profile, commit_intent) = create_result
            .map_err(|(_, typedb_source)| LocalServerStateError::UserCannotBeCreated { typedb_source })?;

        let commit_profile = transaction_profile.commit_profile();
        let commit_record = commit_intent.write_snapshot.finalise(commit_profile).map_err(|_error| {
            LocalServerStateError::UserCannotBeCreated { typedb_source: UserCreateError::Unexpected {} }
        })?;

        if let Some(commit_record) = commit_record {
            let commit_profile = transaction_profile.commit_profile();
            server_state.database_data_commit(SYSTEM_DB, commit_record, commit_profile).await?;
        }

        Ok(())
    }

    pub async fn update_user(
        server_state: &ArcServerState,
        accessor: Accessor,
        username: &str,
        user_update: Option<system::concepts::User>,
        credential_update: Option<system::concepts::Credential>,
    ) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_update_permitted(accessor.as_str(), username) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = server_state.user_manager().await.ok_or(LocalServerStateError::NotInitialised {})?;

        let (mut transaction_profile, commit_intent_result) =
            user_manager.update(username, &user_update, &credential_update);

        let commit_intent = commit_intent_result
            .map_err(|typedb_source| LocalServerStateError::UserCannotBeUpdated { typedb_source })?;

        let commit_profile = transaction_profile.commit_profile();
        let commit_record = commit_intent.write_snapshot.finalise(commit_profile).map_err(|_error| {
            LocalServerStateError::UserCannotBeUpdated { typedb_source: UserUpdateError::Unexpected {} }
        })?;

        if let Some(commit_record) = commit_record {
            let commit_profile = transaction_profile.commit_profile();
            server_state.database_data_commit(SYSTEM_DB, commit_record, commit_profile).await?;
        }

        Ok(())
    }

    pub async fn delete_user(
        server_state: &ArcServerState,
        accessor: Accessor,
        username: &str,
    ) -> Result<(), ArcServerStateError> {
        if !PermissionManager::exec_user_delete_allowed(accessor.as_str(), username) {
            return Err(Arc::new(LocalServerStateError::OperationNotPermitted {}));
        }

        let user_manager = server_state.user_manager().await.ok_or(LocalServerStateError::NotInitialised {})?;

        let (transaction_profile_opt, commit_intent_result) = user_manager.delete(username);

        let mut transaction_profile = transaction_profile_opt
            .ok_or(LocalServerStateError::UserCannotBeDeleted { typedb_source: UserDeleteError::Unexpected {} })?;

        let commit_intent = commit_intent_result
            .map_err(|error| LocalServerStateError::UserCannotBeDeleted { typedb_source: error })?;

        let commit_profile = transaction_profile.commit_profile();
        let commit_record = commit_intent.write_snapshot.finalise(commit_profile).map_err(|_error| {
            LocalServerStateError::UserCannotBeDeleted { typedb_source: UserDeleteError::Unexpected {} }
        })?;

        if let Some(commit_record) = commit_record {
            let commit_profile = transaction_profile.commit_profile();
            server_state.database_data_commit(SYSTEM_DB, commit_record, commit_profile).await?;
        }

        Ok(())
    }
}
