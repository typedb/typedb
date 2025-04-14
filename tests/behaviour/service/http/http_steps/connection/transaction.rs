/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::VecDeque, time::Duration};

use cucumber::{gherkin::Step, given, then, when};
use futures::{future::join_all, FutureExt};
use itertools::Either;
use macro_rules_attribute::apply;
use params::{self, check_boolean};

use crate::{
    generic_step,
    message::{transactions_close, transactions_commit, transactions_open, transactions_rollback},
    util::iter_table,
    Context, HttpContext,
};

#[apply(generic_step)]
#[step(expr = "connection open {word} transaction for database: {word}{may_error}")]
pub async fn connection_open_transaction_for_database(
    context: &mut Context,
    transaction_type: String,
    database_name: String,
    may_error: params::MayError,
) {
    context.cleanup_transactions().await;
    may_error.check(context.push_transaction(
        transactions_open(&context.http_context, &database_name, &transaction_type, &context.transaction_options).await,
    ));
}

#[apply(generic_step)]
#[step(expr = "connection open transaction(s) for database: {word}, of type:")]
async fn connection_open_transactions_for_database(context: &mut Context, database_name: String, step: &Step) {
    for transaction_type in iter_table(step) {
        context
            .push_transaction(
                transactions_open(
                    &context.http_context,
                    &database_name,
                    &transaction_type,
                    &context.transaction_options,
                )
                .await,
            )
            .unwrap();
    }
}

#[apply(generic_step)]
#[step(expr = "connection open transaction(s) in parallel for database: {word}, of type:")]
pub async fn connection_open_transactions_in_parallel(context: &mut Context, database_name: String, step: &Step) {
    let transactions: VecDeque<String> = join_all(iter_table(step).map(|transaction_type| {
        transactions_open(&context.http_context, &database_name, transaction_type, &context.transaction_options)
    }))
    .await
    .into_iter()
    .map(|result| result.unwrap().transaction_id.to_string())
    .collect();
    context.set_transactions(transactions).await;
}

#[apply(generic_step)]
#[step(expr = "in background, connection open {word} transaction for database: {word}{may_error}")]
pub async fn in_background_connection_open_transaction_for_database(
    context: &mut Context,
    transaction_type: String,
    database_name: String,
    may_error: params::MayError,
) {
    in_background!(context, |background| {
        may_error.check(context.push_background_transaction(
            transactions_open(&background, &database_name, &transaction_type, &context.transaction_options).await,
        ));
    });
}

#[apply(generic_step)]
#[step(expr = "transaction is open: {boolean}")]
#[step(expr = "transactions( in parallel) are open: {boolean}")]
pub async fn transaction_is_open(_context: &mut Context, _is_open: params::Boolean) {
    // no op: cannot check in http
}

#[apply(generic_step)]
#[step(expr = "transaction has type: {word}")]
pub async fn transaction_has_type(_context: &mut Context, _transaction_type: String) {
    // no op: cannot check in http
}

#[apply(generic_step)]
#[step(expr = "transactions( in parallel) have type:")]
pub async fn transactions_have_type(_context: &mut Context) {
    // no op: cannot check in http
}

#[apply(generic_step)]
#[step(expr = "transaction commits{may_error}")]
pub async fn transaction_commits(context: &mut Context, may_error: params::MayError) {
    let transaction = context.take_transaction();
    may_error.check(transactions_commit(&context.http_context, &transaction).await);
}

#[apply(generic_step)]
#[step(expr = "transaction closes")]
pub async fn transaction_closes(context: &mut Context) {
    let transaction = context.take_transaction();
    transactions_close(&context.http_context, &transaction).await.expect("Expected transaction close")
}

#[apply(generic_step)]
#[step(expr = "transaction rollbacks{may_error}")]
pub async fn transaction_rollbacks(context: &mut Context, may_error: params::MayError) {
    let transaction = context.transaction();
    may_error.check(transactions_rollback(&context.http_context, &transaction).await);
}

#[apply(generic_step)]
#[step(expr = "set transaction option transaction_timeout_millis to: {int}")]
pub async fn set_transaction_option_transaction_timeout_millis(context: &mut Context, value: u64) {
    context.init_transaction_options_if_needed();
    context.transaction_options.as_mut().unwrap().transaction_timeout_millis = Some(value);
}

#[apply(generic_step)]
#[step(expr = "set transaction option schema_lock_acquire_timeout_millis to: {int}")]
pub async fn set_transaction_option_schema_lock_acquire_timeout_millis(context: &mut Context, value: u64) {
    context.init_transaction_options_if_needed();
    context.transaction_options.as_mut().unwrap().schema_lock_acquire_timeout_millis = Some(value);
}
