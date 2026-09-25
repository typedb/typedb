/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lib_benchmark::benchmark::RunDescriptor;

pub const ROWS_SMALL: usize = 100;
pub const ROWS_MEDIUM: usize = 1_000;
pub const ROWS_LARGE: usize = 10_000;

pub const THREADS_SERIAL: usize = 1;
pub const THREADS_THREADED: usize = 4;
pub const THREADS_PARALLEL: usize = 16;

pub const TRANSACTIONS_FEW: usize = 100;
pub const TRANSACTIONS_MANY: usize = 1_000;

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

pub const PARALLEL_MANY_SMALL: RunDescriptor = RunDescriptor {
    parallelism: THREADS_THREADED,
    total_txns: TRANSACTIONS_MANY,
    n_queries_per_tx: 1,
    n_rows_per_query: ROWS_SMALL,
};

pub const PARALLEL_MANY_LARGE: RunDescriptor = RunDescriptor {
    parallelism: THREADS_THREADED,
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
