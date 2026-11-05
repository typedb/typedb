/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc};

use database::{
    Database,
    transaction::{TransactionRead, TransactionWrite},
};
use options::TransactionOptions;
use query::given_rows::{GivenRowEntry, GivenRowsSimple};
use storage::durability_client::WALClient;

use crate::{
    benchmark::{SimpleBenchmark, SimpleReport},
    database::TypeDB,
    query::{QueryAnswer, execute_write_query_in, unpack_result},
    transaction::UnifiedTransactionView,
    typedb_workload::{
        CountResults, QueryDescriptor, RunDescriptor, TxType,
        datagen::RandomDataGen,
        profiles::MultiTxMultiQueryProfile,
        reports::QueryFocusedReport,
        workload::{WorkLoad, WorkloadInputBatches},
    },
};

pub type PreloadDataFn = Box<dyn Fn(Arc<Database<WALClient>>)>;
pub type WarmupFn = Box<dyn Fn(Arc<Database<WALClient>>)>;
pub type PrepareRunFn<IN> = Box<dyn Fn(Arc<Database<WALClient>>) -> IN>;
pub type BenchmarkedFn<IN, OUT> = Box<dyn Fn(Arc<Database<WALClient>>, IN) -> OUT>;
pub type DbgAssertFn = Arc<dyn Fn(usize) + Send + Sync>;
const RUN_DBG_ASSERTIONS: bool = false;

// Easy usage
pub type TypeDBQueryWorkloadBenchmark = TypeDBWorkloadBenchmark<QueryFocusedReport>;

pub struct TypeDBMicroBenchmark<IN, OUT, REPORT: SimpleReport<OUT>> {
    pub name: String,
    pub schema: String,
    pub preload_data_fn: Option<PreloadDataFn>,
    pub warmup_fn: Option<WarmupFn>,
    pub prepare_run_fn: PrepareRunFn<IN>,
    pub benchmark_fn: BenchmarkedFn<IN, OUT>,
    pub _report: PhantomData<REPORT>,
}

impl<IN, OUT, REPORT: SimpleReport<OUT>> SimpleBenchmark for TypeDBMicroBenchmark<IN, OUT, REPORT> {
    type RunInput = IN;
    type IterOutput = OUT;
    type Report = REPORT;

    fn name(&self) -> &'_ str {
        self.name.as_str()
    }

    fn prepare_database(&self, _typedb: &TypeDB, database: Arc<Database<WALClient>>) {
        crate::database::create_schema(database.clone(), self.schema.as_str());
        if let Some(preload_fn) = &self.preload_data_fn {
            preload_fn(database.clone());
        }
        #[cfg(debug_assertions)]
        eprintln!("---\n- WARNING: In debug mode, we don't flush the database.\n---");
        #[cfg(not(debug_assertions))]
        database.benchmark_only_flush();
    }

    fn warm_up(&self, _typedb: &TypeDB, database: Arc<Database<WALClient>>) {
        if let Some(warmup_fn) = &self.warmup_fn {
            warmup_fn(database.clone())
        }
    }

    fn prepare_run(&self, _typedb: &TypeDB, database: Arc<Database<WALClient>>) -> Self::RunInput {
        (self.prepare_run_fn)(database)
    }

    fn run_benchmark(
        &self,
        _typedb: &TypeDB,
        database: Arc<Database<WALClient>>,
        input: Self::RunInput,
    ) -> Self::IterOutput {
        (self.benchmark_fn)(database, input)
    }
}

pub type TypeDBWorkloadBenchmark<Report> =
    TypeDBMicroBenchmark<Arc<WorkloadInputBatches>, MultiTxMultiQueryProfile, Report>;
impl<Report: SimpleReport<MultiTxMultiQueryProfile>> TypeDBWorkloadBenchmark<Report> {
    pub fn new(
        name: impl Into<String>,
        schema: impl Into<String>,
        preload_data_fn: Option<PreloadDataFn>,
        query_descriptor: QueryDescriptor,
        run_descriptor: RunDescriptor,
        dbg_assert: Option<DbgAssertFn>,
    ) -> Self {
        let tx_type = query_descriptor.tx_type;

        let name: String = name.into();
        let schema: String = schema.into();

        let warmup_fn = Some(WorkLoad::warmup_fn_for(&query_descriptor));
        let dbg_assert = match RUN_DBG_ASSERTIONS {
            true => dbg_assert,
            false => None,
        };
        let workload = WorkLoad { query_descriptor, run_descriptor, dbg_assert };
        let prepare_run_fn = workload.prepare_fn();
        let benchmark_fn = match tx_type {
            TxType::Read => workload.runner::<TransactionRead<WALClient>>(name.clone()),
            TxType::Write => workload.runner::<TransactionWrite<WALClient>>(name.clone()),
        };
        Self { name, schema, preload_data_fn, warmup_fn, prepare_run_fn, benchmark_fn, _report: PhantomData }
    }

    pub fn make_preload_data_fn(
        query: String,
        variables: Vec<String>,
        produce_row: fn(usize, &mut RandomDataGen) -> Vec<GivenRowEntry>,
        n_total_rows: usize,
        n_rows_per_query: usize,
    ) -> PreloadDataFn {
        Box::new(move |database: Arc<Database<WALClient>>| {
            let mut rng = RandomDataGen::new();
            let mut completed = 0;
            while completed < n_total_rows {
                let this_batch = n_rows_per_query.min(n_total_rows - completed);
                let rows = (0..this_batch).map(|i| produce_row(completed + i, &mut rng)).collect();
                let given_rows = GivenRowsSimple { variables: variables.clone(), rows };
                let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
                let (result, tx) = unpack_result(execute_write_query_in::<_, CountResults>(
                    tx,
                    query.as_str(),
                    Some(given_rows),
                    false,
                ));
                let QueryAnswer { answer: n_answer_rows, .. } = result.unwrap();
                assert_eq!(n_answer_rows, this_batch);
                tx.commit().unwrap();
                completed += this_batch;
            }
        })
    }
}
