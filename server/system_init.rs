/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use database::{transaction::SchemaCommitIntent, Database};
use resource::{
    constants::server::{DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD},
    internal_database_prefix,
    profile::TransactionProfile,
};
use storage::durability_client::WALClient;
use system::{
    concepts::{Credential, PasswordHash, User},
    repositories::SCHEMA,
    util::transaction_util::TransactionUtil,
};

use crate::{
    error::{arc_server_state_err, ArcServerStateError, LocalServerStateError},
    state::ServerState,
};

pub const SYSTEM_DB: &str = concat!(internal_database_prefix!(), "system");

pub async fn initialise_system_database(server_state: &ServerState) -> Result<(), ArcServerStateError> {
    if server_state.databases().manager().database_unrestricted(SYSTEM_DB).is_some() {
        return Ok(());
    }
    server_state.databases().databases_create_unrestricted(SYSTEM_DB).await?;
    Ok(())
}

pub async fn initialise_system_database_schema(server_state: &ServerState) -> Result<(), ArcServerStateError> {
    let db = server_state
        .databases()
        .manager()
        .database_unrestricted(SYSTEM_DB)
        .expect("Critical: system database must exist before schema initialisation");
    let (mut transaction_profile, commit_intent_opt) = get_system_database_schema_commit_intent(db)?;
    if let Some(commit_intent) = commit_intent_opt {
        let commit_profile = transaction_profile.take_commit_profile();
        let (commit_profile, result) =
            server_state.databases().database_schema_commit(commit_intent, commit_profile).await;
        transaction_profile.set_commit_profile(commit_profile);
        result?;
    }
    Ok(())
}

pub async fn initialise_default_user(server_state: &ServerState) -> Result<(), ArcServerStateError> {
    let user_manager = server_state.users().manager()?;
    let exists = user_manager.contains(DEFAULT_USER_NAME).map_err(|typedb_source| {
        arc_server_state_err(LocalServerStateError::UserCannotBeRetrieved { typedb_source })
    })?;
    if !exists {
        user_manager
            .create(
                &User::new(DEFAULT_USER_NAME.to_string()),
                &Credential::PasswordType { password_hash: PasswordHash::from_password(DEFAULT_USER_PASSWORD) },
            )
            .map_err(|typedb_source| {
                arc_server_state_err(LocalServerStateError::UserCannotBeCreated { typedb_source })
            })?;
    }
    Ok(())
}

pub fn get_system_database_schema_commit_intent(
    db: Arc<Database<WALClient>>,
) -> Result<(TransactionProfile, Option<SchemaCommitIntent<WALClient>>), ArcServerStateError> {
    let tx_util = TransactionUtil::new(db);
    let (transaction_profile, finalise_result) =
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
    let commit_intent_opt = if commit_intent.has_changes() { Some(commit_intent) } else { None };
    Ok((transaction_profile, commit_intent_opt))
}
