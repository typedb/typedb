/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::Formatter;
use durability::wal::WAL;

pub enum ActiveTransaction {
    Read(database::transaction::TransactionRead<WAL>),
    Write(database::transaction::TransactionWrite<WAL>),
    Schema(database::transaction::TransactionSchema<WAL>),
}

impl std::fmt::Debug for ActiveTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            ActiveTransaction::Read(_) => "Read",
            ActiveTransaction::Write(_) => "Write",
            ActiveTransaction::Schema(_) => "Schema"
        })
    }
}

#[macro_export]
macro_rules! tx_as_read {
    ($tx:ident, $block:block) => {
        match $tx {
            ActiveTransaction::Read($tx) => { $block },
            ActiveTransaction::Write($tx) => { $block },
            ActiveTransaction::Schema($tx) => { $block },
        }
    };
}

#[macro_export]
macro_rules! tx_as_write {
    ($tx:ident, $block:block) => {
        match $tx {
            ActiveTransaction::Read(_) => panic!("Using Read transaction as Write"),
            ActiveTransaction::Write($tx) => { $block },
            ActiveTransaction::Schema($tx) => { $block },
        }
    }
}

#[macro_export]
macro_rules! tx_as_schema {
    ($tx:ident, $block:block) => {
        match $tx {
            ActiveTransaction::Read(_) => panic!("Using Read transaction as Schema"),
            ActiveTransaction::Write(_) => panic!("Using Write transaction as Schema"),
            ActiveTransaction::Schema($tx) => { $block },
        }
    };
}
