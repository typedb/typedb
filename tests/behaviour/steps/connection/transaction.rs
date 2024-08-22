/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::error::ConceptWriteError;
use database::transaction::{DataCommitError, SchemaCommitError, TransactionRead, TransactionSchema, TransactionWrite};
use macro_rules_attribute::apply;
use options::TransactionOptions;

use crate::{
    assert::assert_matches,
    generic_step,
    params::{check_boolean, Boolean, MayError},
    ActiveTransaction, Context,
};

#[apply(generic_step)]
#[step(expr = "connection open {word} transaction for database: {word}")]
pub async fn connection_open_transaction(context: &mut Context, tx_type: String, db_name: String) {
    assert!(context.transaction().is_none(), "Existing transaction must be closed first");
    let server = context.server().unwrap().lock().unwrap();
    let database = server.database_manager().database(&db_name).unwrap();
    let tx = match tx_type.as_str() {
        "read" => ActiveTransaction::Read(TransactionRead::open(database.clone(), TransactionOptions::default())),
        "write" => ActiveTransaction::Write(TransactionWrite::open(database.clone(), TransactionOptions::default())),
        "schema" => ActiveTransaction::Schema(TransactionSchema::open(database.clone(), TransactionOptions::default())),
        _ => unreachable!("Unrecognised transaction type"),
    };
    drop(server);
    context.set_transaction(tx);
}

#[apply(generic_step)]
#[step(expr = "transaction is open: {boolean}")]
pub async fn transaction_is_open(context: &mut Context, is_open: Boolean) {
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
pub async fn transaction_commits(context: &mut Context, may_error: MayError) {
    match context.take_transaction().unwrap() {
        ActiveTransaction::Read(_) => {}
        ActiveTransaction::Write(tx) => {
            if let Some(error) = may_error.check(&tx.commit()) {
                if let DataCommitError::ConceptWriteErrors { source, .. } = error {
                    source
                        .iter()
                        .for_each(|error| may_error.check_concept_write_without_read_errors::<()>(&Err(error.clone())))
                } else {
                    panic!("Unexpected write commit error: {:?}", error)
                }
            }
        }
        ActiveTransaction::Schema(tx) => {
            if let Some(schema_commit_error) = may_error.check(&tx.commit()) {
                match schema_commit_error {
                    SchemaCommitError::ConceptWrite { errors } => errors
                        .iter()
                        .for_each(|error| may_error.check_concept_write_without_read_errors::<()>(&Err(error.clone()))),
                    _ => {}
                }
            }
        }
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
pub async fn transations_in_parallel_are_null(context: &mut Context, are_null: Boolean) {
    todo!()
}

#[apply(generic_step)]
#[step(expr = "transactions in parallel are open: {boolean}")]
pub async fn transactions_in_parallel_are_open(context: &mut Context, are_open: Boolean) {
    todo!()
}

#[apply(generic_step)]
#[step(expr = "transactions in parallel have type:")]
pub async fn transactions_in_parallel_have_type(context: &mut Context) {
    todo!()
}
