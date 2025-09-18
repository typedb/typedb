use database::Database;
use std::sync::Arc;
use database::transaction::SchemaCommitError;
use resource::constants::server::{DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD};
use resource::internal_database_prefix;
use resource::profile::{CommitProfile, TransactionProfile};
use storage::durability_client::WALClient;
use storage::isolation_manager::CommitRecord;
use storage::snapshot::CommittableSnapshot;
use system::concepts::{Credential, PasswordHash, User};
use system::repositories::SCHEMA;
use system::util::transaction_util::TransactionUtil;
use crate::state::{ServerState, ServerStateError};

pub const SYSTEM_DB: &str = concat!(internal_database_prefix!(), "system");

pub async fn initialise_system_database(server_state: &dyn ServerState) -> Result<Arc<Database<WALClient>>, ServerStateError> {
    server_state.databases_create(SYSTEM_DB).await?;
    let db = server_state.database_manager().await.database_unrestricted(SYSTEM_DB).expect("todo");
    initialise_system_database_schema(db.clone(), server_state).await?;
    Ok(db)
}

pub async fn get_system_database_schema_commit_record(db: Arc<Database<WALClient>>) -> Result<(TransactionProfile, Option<CommitRecord>), ServerStateError> {
    let tx_util = TransactionUtil::new(db);
    let (mut transaction_profile, finalise_result) = tx_util.schema_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr| {
        let query = typeql::parse_query(SCHEMA)
            .unwrap_or_else(|_| {
                panic!("Unexpected error occurred when parsing the schema for the {} database.", SYSTEM_DB)
            })
            .into_structure()
            .into_schema();
        query_mgr.execute_schema(snapshot, type_mgr, thing_mgr, fn_mgr, query, SCHEMA).unwrap_or_else(
            |_| {
                panic!("Unexpected error occurred when defining the schema for the {} database.", SYSTEM_DB)
            },
        );
    });
    let mut commit_profile = transaction_profile.commit_profile();
    let (_, snapshot) = finalise_result
        .map_err(|error| ServerStateError::DatabaseSchemaCommitFailed { typedb_source: error })?;
    let commit_record_opt = snapshot.finalise(&mut commit_profile)
        .map_err(|error| ServerStateError::DatabaseSchemaCommitFailed { typedb_source: SchemaCommitError::SnapshotError { typedb_source: error }})?;
    return Ok((transaction_profile, commit_record_opt));
}

async fn initialise_system_database_schema(db: Arc<Database<WALClient>>, server_state: &dyn ServerState) -> Result<(), ServerStateError> {
    if let (mut transaction_profile, Some(commit_record)) = get_system_database_schema_commit_record(db).await? {
        server_state.database_schema_commit(SYSTEM_DB, commit_record, transaction_profile.commit_profile()).await?;
    }
    Ok(())
}

pub async fn initialise_default_user(user_manager: &user::user_manager::UserManager, server_state: &dyn ServerState) -> Result<(), ServerStateError> {
    let (mut transaction_profile, finalise_result) = user_manager.create2(
        &User::new(DEFAULT_USER_NAME.to_string()),
        &Credential::PasswordType { password_hash: PasswordHash::from_password(DEFAULT_USER_PASSWORD) },
    );
    let mut commit_profile = transaction_profile.commit_profile();
    let (_, snapshot) = finalise_result
        .map_err(|error| ServerStateError::UserCannotBeCreated { typedb_source: error })?;
    let commit_record_opt = snapshot.finalise(&mut commit_profile)
        .map_err(|error| ServerStateError::DatabaseSchemaCommitFailed { typedb_source: SchemaCommitError::SnapshotError { typedb_source: error }})?;;
    if let Some(commit_record) = commit_record_opt {
        server_state.users_create2(commit_record, &mut commit_profile).await?;
    }
    Ok(())
}