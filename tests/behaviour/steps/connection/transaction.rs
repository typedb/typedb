/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use macro_rules_attribute::apply;
use database::transaction::{TransactionRead, TransactionWrite, TransactionSchema};

use crate::{generic_step, util, Context, ActiveTransaction};

#[apply(generic_step)]
#[step(expr = "connection opens {word} transaction for: {word}")]
pub async fn connection_open_transaction(context: &mut Context, tx_type: String, db_name: String) {
    let db = context.databases().get(&db_name).unwrap();
    let tx = match tx_type.as_str() {
        "read" => ActiveTransaction::Read(TransactionRead::open(db.clone())),
        "write" => ActiveTransaction::Write(TransactionWrite::open(db.clone())),
        "schema" => ActiveTransaction::Schema(TransactionSchema::open(db.clone())),
        _ => unreachable!("Unrecognised transaction type")
    };
    context.set_transaction(tx);
}

#[apply(generic_step)]
#[step(expr = "transaction commits")]
pub async fn transaction_commits(context: &mut Context) {
    match context.take_transaction().unwrap() {
        ActiveTransaction::Read(_) => panic!("Commit on read transaction"),
        ActiveTransaction::Write(tx) => tx.commit(),
        ActiveTransaction::Schema(tx) => tx.commit(),
    }.unwrap()
}
