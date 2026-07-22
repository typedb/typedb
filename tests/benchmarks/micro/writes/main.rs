/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use criterion::{Criterion, criterion_group, criterion_main};
use database::transaction::TransactionWrite;
use lib_benchmark::{
    Config, Context,
    profiler::FlamegraphProfiler,
    utils::{ResultCounter, unpack_result},
};
use options::TransactionOptions;


const SCHEMA: &str = r#"
define
    attribute name, value string;
    entity person, owns name;
"#;

fn bench_insert_person_only(c: &mut Criterion) {
    let query = "insert $_ isa person;";
    let context = Context::init(Config::default());
    let db = context.create_database("insert_person_only").unwrap();

    lib_benchmark::create_schema(db.clone(), SCHEMA);
    c.bench_function("bench_insert_person_only", |b| {
        b.iter(|| {
            let tx = TransactionWrite::open(db.clone(), TransactionOptions::default()).unwrap();
            let (query_result, tx) =
                unpack_result(lib_benchmark::execute_write_query_in::<_, ResultCounter>(tx, query, None));
            query_result.unwrap();
            lib_benchmark::commit(tx).unwrap();
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_insert_person_only(c);
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(FlamegraphProfiler::new(100))
}

criterion_group!(
    name = benches;
    config = profiled();
    targets = criterion_benchmark
);

criterion_main!(benches);
