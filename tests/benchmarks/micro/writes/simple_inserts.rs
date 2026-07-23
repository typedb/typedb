/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use criterion::Criterion;
use lib_benchmark::templates::{SimpleBenchmark, no_given_rows, no_initial_data, query_in_write_tx};

use crate::TransactionInsertBenchmark;

const SCHEMA: &'static str = r#"
define
    attribute name, value string;
    entity person, owns name;
"#;

fn person_only() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "person_only",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        prepare_iter_fn: no_given_rows(),
        benchmark_fn: query_in_write_tx("insert $x isa person;"),
    }
}

pub(crate) fn run_all(c: &mut Criterion) {
    let mut g = c.benchmark_group("simple_inserts");
    g.sample_size(20);
    person_only().run_benchmark(&mut g);
}
