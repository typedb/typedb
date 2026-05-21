/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::{
    Database,
    transaction::{TransactionError, TransactionId, TransactionRead, TransactionSchema, TransactionWrite},
};
use diagnostics::metrics::LoadKind;
use options::TransactionOptions;
use serde::{Deserialize, Serialize};
use storage::durability_client::WALClient;
use tokio::task::spawn_blocking;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum TransactionType {
    Read,
    Write,
    Schema,
}

#[derive(Debug)]
pub enum Transaction {
    Read(TransactionRead<WALClient>),
    Write(TransactionWrite<WALClient>),
    Schema(TransactionSchema<WALClient>),
}

macro_rules! with_readable_transaction {
    ($match_:expr, |$transaction:ident| $block:block) => {{
        match $match_ {
            Transaction::Read($transaction) => $block
            Transaction::Write($transaction) => $block
            Transaction::Schema($transaction) => $block
        }
    }}
}
pub(crate) use with_readable_transaction;

impl Transaction {
    pub fn id(&self) -> TransactionId {
        match self {
            Transaction::Read(transaction) => transaction.id(),
            Transaction::Write(transaction) => transaction.id(),
            Transaction::Schema(transaction) => transaction.id(),
        }
    }

    pub fn type_(&self) -> TransactionType {
        match self {
            Transaction::Read(_) => TransactionType::Read,
            Transaction::Write(_) => TransactionType::Write,
            Transaction::Schema(_) => TransactionType::Schema,
        }
    }

    pub fn load_kind(&self) -> LoadKind {
        match self {
            Transaction::Read(_) => LoadKind::ReadTransactions,
            Transaction::Write(_) => LoadKind::WriteTransactions,
            Transaction::Schema(_) => LoadKind::SchemaTransactions,
        }
    }

    pub fn database_name(&self) -> &str {
        with_readable_transaction!(self, |transaction| { transaction.database.name() })
    }

    pub fn close(self) {
        match self {
            Transaction::Read(transaction) => transaction.close(),
            Transaction::Write(transaction) => transaction.close(),
            Transaction::Schema(transaction) => transaction.close(),
        }
    }
}

pub async fn open_transaction_blocking(
    database: Arc<Database<WALClient>>,
    transaction_type: TransactionType,
    options: TransactionOptions,
) -> Result<Transaction, TransactionError> {
    spawn_blocking(move || match transaction_type {
        TransactionType::Read => TransactionRead::open(database, options).map(Transaction::Read),
        TransactionType::Write => TransactionWrite::open(database, options).map(Transaction::Write),
        TransactionType::Schema => TransactionSchema::open(database, options).map(Transaction::Schema),
    })
    .await
    .unwrap()
}
