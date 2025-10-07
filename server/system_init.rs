/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use database::{transaction::SchemaCommitError, Database};
use resource::{
    constants::server::{DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD},
    internal_database_prefix,
    profile::TransactionProfile,
};
use storage::{durability_client::WALClient, isolation_manager::CommitRecord, snapshot::CommittableSnapshot};
use system::{
    concepts::{Credential, PasswordHash, User},
    repositories::SCHEMA,
    util::transaction_util::TransactionUtil,
};

use crate::{
    error::{ArcServerStateError, LocalServerStateError},
    state::ServerState,
};

pub const SYSTEM_DB: &str = concat!(internal_database_prefix!(), "system");

pub async fn initialise_system_database(
    server_state: &dyn ServerState,
) -> Result<Arc<Database<WALClient>>, ArcServerStateError> {
    server_state.databases_create(SYSTEM_DB).await?;
    let db = server_state.database_manager().await.database_unrestricted(SYSTEM_DB).expect("todo");
    initialise_system_database_schema(db.clone(), server_state).await?;
    Ok(db)
}

pub async fn get_system_database_schema_commit_record(
    db: Arc<Database<WALClient>>,
) -> Result<(TransactionProfile, Option<CommitRecord>), ArcServerStateError> {
    let tx_util = TransactionUtil::new(db);
    let (mut transaction_profile, finalise_result) =
        tx_util.schema_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr| {
            let query = typeql::parse_query(SCHEMA)
                .unwrap_or_else(|_| {
                    panic!("Unexpected error occurred when parsing the schema for the {} database.", SYSTEM_DB)
                })
                .into_structure()
                .into_schema();
            query_mgr.execute_schema(snapshot, type_mgr, thing_mgr, fn_mgr, query, SCHEMA).unwrap_or_else(|_| {
                panic!("Unexpected error occurred when defining the schema for the {} database.", SYSTEM_DB)
            });
        });
    let mut commit_profile = transaction_profile.commit_profile();
    let commit_intent =
        finalise_result.map_err(|error| LocalServerStateError::DatabaseSchemaCommitFailed { typedb_source: error })?;
    let commit_record_opt = commit_intent.schema_snapshot.finalise(&mut commit_profile).map_err(|error| {
        LocalServerStateError::DatabaseSchemaCommitFailed {
            typedb_source: SchemaCommitError::SnapshotError { typedb_source: error },
        }
    })?;
    Ok((transaction_profile, commit_record_opt))
}

async fn initialise_system_database_schema(
    db: Arc<Database<WALClient>>,
    server_state: &dyn ServerState,
) -> Result<(), ArcServerStateError> {
    if let (mut transaction_profile, Some(commit_record)) = get_system_database_schema_commit_record(db).await? {
        server_state.database_schema_commit(SYSTEM_DB, commit_record, transaction_profile.commit_profile()).await?;
    }
    Ok(())
}

pub async fn get_default_user_commit_record(
    user_manager: &user::user_manager::UserManager,
) -> Result<(TransactionProfile, Option<CommitRecord>), LocalServerStateError> {
    let (transaction_profile_opt, finalise_result) = user_manager.create(
        &User::new(DEFAULT_USER_NAME.to_string()),
        &Credential::PasswordType { password_hash: PasswordHash::from_password(DEFAULT_USER_PASSWORD) },
    );

    let mut transaction_profile = transaction_profile_opt
        .ok_or(LocalServerStateError::UserCannotBeCreated {
            typedb_source: UserCreateError::Unexpected { }
        })?;

    let mut commit_profile = transaction_profile.commit_profile();
    let commit_intent =
        finalise_result.map_err(|error| LocalServerStateError::UserCannotBeCreated { typedb_source: error })?;
    let commit_record_opt = commit_intent.write_snapshot.finalise(&mut commit_profile).map_err(|error| {
        LocalServerStateError::DatabaseSchemaCommitFailed {
            typedb_source: SchemaCommitError::SnapshotError { typedb_source: error },
        }
    })?;
    Ok((transaction_profile, commit_record_opt))
}

pub async fn initialise_default_user(
    user_manager: &user::user_manager::UserManager,
    server_state: &dyn ServerState,
) -> Result<(), ArcServerStateError> {
    if let (mut transaction_profile, Some(commit_record)) = get_default_user_commit_record(user_manager).await? {
        server_state.database_data_commit(SYSTEM_DB, commit_record, &mut transaction_profile.commit_profile()).await?;
    }
    Ok(())
}
