/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use database::Database;
use storage::durability_client::WALClient;

use crate::database::{Config, TypeDB};

/// See `BenchmarkRunner` implementations for the order in which these functions are called.
pub trait SimpleBenchmark {
    type RunInput;
    type IterOutput;
    type Report: SimpleReport<Self::IterOutput>;

    fn name(&self) -> &'_ str;

    fn init_typedb(&self) -> TypeDB {
        TypeDB::init(Config::default())
    }

    /// Run before any iteration or batch.
    fn before_all(&self, _typedb: &mut TypeDB) {}

    fn create_database(&self, typedb: &mut TypeDB) -> Arc<Database<WALClient>> {
        typedb.recreate_database(self.name()).unwrap()
    }

    /// Load schema & data
    fn prepare_database(&self, typedb: &TypeDB, database: Arc<Database<WALClient>>);

    fn warm_up(&self, typedb: &TypeDB, database: Arc<Database<WALClient>>);

    /// Create given_rows if needed, so we don't include that in the timing.
    fn prepare_run(&self, typedb: &TypeDB, database: Arc<Database<WALClient>>) -> Self::RunInput;

    /// The actual iteration which gets timed.
    fn run_benchmark(
        &self,
        typedb: &TypeDB,
        database: Arc<Database<WALClient>>,
        input: Self::RunInput,
    ) -> Self::IterOutput;
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
