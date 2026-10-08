/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::typedb_workload::{QueryDescriptor, RunDescriptor};

pub const ROWS_SMALL: usize = 100;
pub const ROWS_MEDIUM: usize = 1_000;
pub const ROWS_LARGE: usize = 5_000;
pub const THREADS_SERIAL: usize = 1;
pub const THREADS_THREADED: usize = 4;
pub const THREADS_PARALLEL: usize = 16;
pub const TRANSACTIONS_FEW: usize = 100;
pub const TRANSACTIONS_MANY: usize = 1_000;
pub const TRANSACTIONS_TONS: usize = 20_000;

pub const SERIAL_FEW_LARGE: RunDescriptor = RunDescriptor {
    parallelism: THREADS_SERIAL,
    total_txns: TRANSACTIONS_FEW,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_LARGE,
};
pub const SERIAL_MANY_SMALL: RunDescriptor = RunDescriptor {
    parallelism: THREADS_SERIAL,
    total_txns: TRANSACTIONS_MANY,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_SMALL,
};
pub const SERIAL_MANY_MEDIUM: RunDescriptor = RunDescriptor {
    parallelism: THREADS_SERIAL,
    total_txns: TRANSACTIONS_MANY,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_MEDIUM,
};
pub const SERIAL_TONS_MEDIUM: RunDescriptor = RunDescriptor {
    parallelism: THREADS_SERIAL,
    total_txns: TRANSACTIONS_TONS,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_MEDIUM,
};
pub const SERIAL_TONS_LARGE: RunDescriptor = RunDescriptor {
    parallelism: THREADS_SERIAL,
    total_txns: TRANSACTIONS_TONS,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_LARGE,
};
pub const PARALLEL_MANY_SMALL: RunDescriptor = RunDescriptor {
    parallelism: THREADS_PARALLEL,
    total_txns: TRANSACTIONS_MANY,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_SMALL,
};
pub const PARALLEL_MANY_LARGE: RunDescriptor = RunDescriptor {
    parallelism: THREADS_PARALLEL,
    total_txns: TRANSACTIONS_MANY,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_LARGE,
};
pub const PARALLEL_MANY_MEDIUM: RunDescriptor = RunDescriptor {
    parallelism: THREADS_PARALLEL,
    total_txns: TRANSACTIONS_MANY,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_MEDIUM,
};
pub const PARALLEL_TONS_MEDIUM: RunDescriptor = RunDescriptor {
    parallelism: THREADS_PARALLEL,
    total_txns: TRANSACTIONS_TONS,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_MEDIUM,
};
pub const PARALLEL_TONS_LARGE: RunDescriptor = RunDescriptor {
    parallelism: THREADS_PARALLEL,
    total_txns: TRANSACTIONS_TONS,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_LARGE,
};

pub fn standardised_name(query_descriptor: &QueryDescriptor, run_descriptor: &RunDescriptor) -> String {
    let desc = &query_descriptor.name;
    let txn = match run_descriptor.total_txns {
        TRANSACTIONS_FEW => "few".to_owned(),
        TRANSACTIONS_MANY => "many".to_owned(),
        TRANSACTIONS_TONS => "tons".to_owned(),
        other => format!("txn[{other}]"),
    };
    let parallelism = match run_descriptor.parallelism {
        THREADS_SERIAL => "serial".to_owned(),
        THREADS_THREADED => "threaded".to_owned(),
        THREADS_PARALLEL => "parallel".to_owned(),
        other => format!("threads[{other}]"),
    };
    let rows = match run_descriptor.n_rows_per_query {
        ROWS_SMALL => "small".to_owned(),
        ROWS_MEDIUM => "medium".to_owned(),
        ROWS_LARGE => "large".to_owned(),
        other => format!("rows[{other}]"),
    };
    let query = match run_descriptor.n_queries_per_tx {
        1 => "".to_owned(),
        other => format!("_queries[{other}]"),
    };
    format!("{parallelism}_{desc}_{txn}_{rows}{query}")
}
