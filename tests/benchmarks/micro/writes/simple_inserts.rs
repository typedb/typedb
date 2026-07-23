/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use criterion::Criterion;
use lib_benchmark::templates::{
    SimpleBenchmark, given_rows_with, n_empty_given_rows, no_given_rows, no_initial_data, query_in_write_tx,
};

use crate::TransactionInsertBenchmark;

pub(crate) fn run_all(c: &mut Criterion) {
    let mut g = c.benchmark_group("simple_inserts");
    g.sample_size(20);
    entities_one().run_benchmark(&mut g);
    entities_thousand().run_benchmark(&mut g);
    ownerships_thousand_names_short().run_benchmark(&mut g);
    ownerships_thousand_names_long().run_benchmark(&mut g);
}

const SCHEMA: &'static str = r#"
define
    attribute name, value string;
    entity person, owns name;
"#;

fn entities_one() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__entities_one",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        prepare_iter_fn: no_given_rows(),
        benchmark_fn: query_in_write_tx("insert $x isa person;"),
    }
}

fn entities_thousand() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__entities_thousand",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        prepare_iter_fn: n_empty_given_rows(1000),
        benchmark_fn: query_in_write_tx("given ; insert $x isa person;"),
    }
}

fn ownerships_thousand_names_short() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__ownerships_thousand_short_names",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        prepare_iter_fn: given_rows_with(1000, vec!["name".to_owned()], |rng| vec![rng.entry_string(5)]),
        benchmark_fn: query_in_write_tx("given $name: string; insert $x isa person, has name == $name;"),
    }
}

fn ownerships_thousand_names_long() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__ownerships_thousand_long_names",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        prepare_iter_fn: given_rows_with(1000, vec!["name".to_owned()], |rng| vec![rng.entry_string(50)]),
        benchmark_fn: query_in_write_tx("given $name: string; insert $x isa person, has name == $name;"),
    }
}
