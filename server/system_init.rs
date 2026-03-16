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
use storage::{durability_client::WALClient, record::CommitRecord, snapshot::CommittableSnapshot};
use system::{
    concepts::{Credential, PasswordHash, User},
    repositories::SCHEMA,
    util::transaction_util::TransactionUtil,
};
use user::{errors::UserCreateError, user_manager::UserManager};

use crate::{
    error::{ArcServerStateError, LocalServerStateError},
    state::ServerState,
};

pub const SYSTEM_DB: &str = concat!(internal_database_prefix!(), "system");

pub async fn initialise_system_database(
    server_state: &ServerState,
) -> Result<Arc<Database<WALClient>>, ArcServerStateError> {
    server_state.databases().databases_create_unrestricted(SYSTEM_DB).await?;
    let db = server_state
        .databases()
        .manager()
        .database_unrestricted(SYSTEM_DB)
        .expect("Critical: system database is absent");
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
    let commit_intent =
        finalise_result.map_err(|error| LocalServerStateError::DatabaseSchemaCommitFailed { typedb_source: error })?;
    let commit_record_opt = commit_intent
        .schema_snapshot
        .has_changes()
        .then(|| commit_intent.schema_snapshot.into_commit_record())
        .map(|(_drop_guard, record)| record);
    Ok((transaction_profile, commit_record_opt))
}

async fn initialise_system_database_schema(
    db: Arc<Database<WALClient>>,
    server_state: &ServerState,
) -> Result<(), ArcServerStateError> {
    if let (mut transaction_profile, Some(commit_record)) = get_system_database_schema_commit_record(db).await? {
        server_state
            .databases()
            .database_schema_commit(SYSTEM_DB, commit_record, transaction_profile.commit_profile())
            .await?;
    }
    Ok(())
}

pub fn initialise_default_user(user_manager: &UserManager) -> Result<(), UserCreateError> {
    match user_manager.create(
        &User::new(DEFAULT_USER_NAME.to_string()),
        &Credential::PasswordType { password_hash: PasswordHash::from_password(DEFAULT_USER_PASSWORD) },
    ) {
        Ok(()) | Err(UserCreateError::UserAlreadyExist { .. }) => Ok(()),
        Err(err) => Err(err),
    }
}
