/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{Arc, Mutex};

use cucumber::codegen::anyhow::Error;
use database::{
    transaction::{DataCommitError, SchemaCommitError, TransactionRead, TransactionSchema, TransactionWrite},
    Database,
};
use macro_rules_attribute::apply;
use options::TransactionOptions;
use server::{typedb, typedb::Server};
use storage::durability_client::WALClient;
use test_utils::assert_matches;

use crate::{
    connection::BehaviourConnectionTestExecutionError,
    generic_step,
    params::{self, check_boolean},
    ActiveTransaction, Context,
};

async fn database_open_transaction(database: Arc<Database<WALClient>>, tx_type: String) -> ActiveTransaction {
    match tx_type.as_str() {
        "read" => ActiveTransaction::Read(TransactionRead::open(database, TransactionOptions::default())),
        "write" => ActiveTransaction::Write(TransactionWrite::open(database, TransactionOptions::default())),
        "schema" => ActiveTransaction::Schema(TransactionSchema::open(database, TransactionOptions::default())),
        _ => unreachable!("Unrecognised transaction type"),
    }
}

#[apply(generic_step)]
#[step(expr = "connection open {word} transaction for database: {word}")]
pub async fn connection_open_transaction(context: &mut Context, tx_type: String, db_name: String) {
    assert!(context.transaction().is_none(), "Existing transaction must be closed first");
    let server = context.server().unwrap().lock().unwrap();
    let database = server.database_manager().database(&db_name).unwrap();
    let tx = database_open_transaction(database.clone(), tx_type).await;
    drop(server);
    context.set_transaction(tx);
}

#[apply(generic_step)]
#[step(expr = "transaction is open: {boolean}")]
pub async fn transaction_is_open(context: &mut Context, is_open: params::Boolean) {
    check_boolean!(is_open, context.transaction().is_some());
}

#[apply(generic_step)]
#[step(expr = "transaction has type: {word}")]
pub async fn transaction_has_type(context: &mut Context, tx_type: String) {
    match tx_type.as_str() {
        "read" => assert_matches!(context.transaction().unwrap(), ActiveTransaction::Read(_)),
        "write" => assert_matches!(context.transaction().unwrap(), ActiveTransaction::Write(_)),
        "schema" => assert_matches!(context.transaction().unwrap(), ActiveTransaction::Schema(_)),
        _ => unreachable!("Unrecognised transaction type"),
    };
}

#[apply(generic_step)]
#[step(expr = "transaction commits{may_error}")]
pub async fn transaction_commits(context: &mut Context, may_error: params::MayError) {
    match context.take_transaction().unwrap() {
        ActiveTransaction::Read(_) => {
            may_error.check::<(), BehaviourConnectionTestExecutionError>(Err(
                BehaviourConnectionTestExecutionError::CannotCommitReadTransaction,
            ));
        }
        ActiveTransaction::Write(tx) => {
            if let Some(error) = may_error.check(tx.commit()) {
                if let DataCommitError::ConceptWriteErrors { source: errors, .. } = error {
                    for error in errors {
                        may_error.check_concept_write_without_read_errors::<()>(&Err(error))
                    }
                } else {
                    panic!("Unexpected write commit error: {:?}", error)
                }
            }
        }
        ActiveTransaction::Schema(tx) => {
            if let Some(error) = may_error.check(tx.commit()) {
                if let SchemaCommitError::ConceptWrite { errors, .. } = error {
                    for error in errors {
                        may_error.check_concept_write_without_read_errors::<()>(&Err(error))
                    }
                } else {
                    panic!("Unexpected schema commit error: {:?}", error)
                }
            }
        }
    }
}

#[apply(generic_step)]
#[step(expr = "transaction rollbacks{may_error}")]
pub async fn transaction_rollbacks(context: &mut Context, may_error: params::MayError) {
    match context.take_transaction().unwrap() {
        ActiveTransaction::Read(_) => {
            may_error.check::<(), BehaviourConnectionTestExecutionError>(Err(
                BehaviourConnectionTestExecutionError::CannotRollbackReadTransaction,
            ));
        }
        ActiveTransaction::Write(mut tx) => tx.rollback(),
        ActiveTransaction::Schema(mut tx) => tx.rollback(),
    }
}

#[apply(generic_step)]
#[step(expr = "transaction closes")]
pub async fn transaction_closes(context: &mut Context) {
    context.close_transaction()
}

#[apply(generic_step)]
#[step(expr = "open transactions in parallel of type:")]
pub async fn open_transactions_in_parallel(context: &mut Context) {
    todo!()
}

#[apply(generic_step)]
#[step(expr = "transactions in parallel are null: {boolean}")]
pub async fn transactions_in_parallel_are_null(context: &mut Context, are_null: params::Boolean) {
    todo!()
}

#[apply(generic_step)]
#[step(expr = "transactions in parallel are open: {boolean}")]
pub async fn transactions_in_parallel_are_open(context: &mut Context, are_open: params::Boolean) {
    todo!()
}

#[apply(generic_step)]
#[step(expr = "transactions in parallel have type:")]
pub async fn transactions_in_parallel_have_type(context: &mut Context) {
    todo!()
}
