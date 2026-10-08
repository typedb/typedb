/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use database::Database;
use storage::durability_client::WALClient;

use crate::{
    Config, Context,
    typedb_workload::{MultiTxMultiQueryProfile, reports::QueryFocusedReport},
};

/// See `BenchmarkRunner` implementations for the order in which these functions are called.
pub trait SimpleBenchmark {
    type RunInput;
    type IterOutput;
    type Report: SimpleReport<Self::IterOutput>;
    /// Create given_rows if needed
    fn name(&self) -> &'_ str;

    fn init_context(&self) -> Context {
        Context::init(Config::default())
    }

    /// Run before any iteration or batch.
    fn before_all(&self, _context: &mut Context) {}

    fn create_database(&self, context: &mut Context) -> Arc<Database<WALClient>> {
        context.recreate_database(self.name()).unwrap()
    }

    /// Load schema & data
    fn prepare_database(&self, context: &Context, database: Arc<Database<WALClient>>);

    fn warm_up(&self, context: &Context, database: Arc<Database<WALClient>>);
    /// Create given_rows if needed
    fn prepare_run(&self, context: &Context, database: Arc<Database<WALClient>>) -> Self::RunInput;

    /// The actual iteration which gets timed over and over again.
    fn run_benchmark(
        &self,
        context: &Context,
        database: Arc<Database<WALClient>>,
        input: Self::RunInput,
    ) -> Self::IterOutput;
}

impl SimpleReport<MultiTxMultiQueryProfile> for QueryFocusedReport {
    fn report(reports: &[MultiTxMultiQueryProfile]) {
        for r in reports {
            QueryFocusedReport::from_ref(r).write_and_print(r.name.as_str());
        }
    }
}

pub trait SimpleReport<T> {
    fn report(reports: &[T])
    where
        Self: Sized;
}

impl SimpleReport<()> for () {
    fn report(_reports: &[()]) {
        println!("DONE. [Report was (), which is a nop dummy].")
    }
}
