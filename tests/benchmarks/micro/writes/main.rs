/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use itertools::Itertools;

use criterion::Criterion;
use lib_benchmark::{
    profiler::FlamegraphProfiler,
    templates::{SimpleBenchmark, TxQueryProfile, TypeDBMicroBenchmark, sanity_check},
};
use query::given_rows::GivenRowsSimple;

mod simple_inserts;

pub type TransactionInsertBenchmark = TypeDBMicroBenchmark<Option<GivenRowsSimple>, TxQueryProfile>;

fn criterion_benchmark(c: &mut Criterion) {
    sanity_check().run_with_criterion(&mut c.benchmark_group("sanity_check").sample_size(10));
    simple_inserts::run_all(c);
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(FlamegraphProfiler::new(100))
}

fn criterion_main() {
    let mut criterion = profiled().configure_from_args();
    criterion_benchmark(&mut criterion);
    criterion.final_summary()
}

fn simple_main() {
    let args = std::env::args().collect::<Vec<_>>();
    debug_assert!(args.len() > 2 && args[1].as_str() == "--simple");
    if args.len() > 3 {
        // Accept args[2] as filter
        let filter = args[2];

    }
}

fn main() {
    // TODO: Can switch between others
    if Some("--simple") == std::env::args().skip(1).next().as_ref().map(|x| x.as_str()) {
        simple_main();
    } else {
        criterion_main();
    }
}
