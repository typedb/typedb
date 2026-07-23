/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use criterion::Criterion;
use lib_benchmark::templates::run_simple_benchmark;

use crate::InsertBenchmark;

const SCHEMA: &'static str = r#"
define
    attribute name, value string;
    entity person, owns name;
"#;

pub(crate) fn run_all(c: &mut Criterion) {
    let mut g = c.benchmark_group("simple_inserts");
    g.sample_size(20);
    run_simple_benchmark(&mut g, InsertBenchmark::new("person_only", SCHEMA, "insert $x isa person;", None))
}
