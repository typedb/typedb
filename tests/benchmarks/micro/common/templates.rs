/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{sync::Arc, time::Duration};

use database::{Database, transaction::{TransactionRead, TransactionWrite}};
use options::TransactionOptions;
use query::given_rows::{GivenRowEntry, GivenRowsSimple};
use resource::profile::{QueryProfile, TransactionProfile};
use storage::durability_client::WALClient;

use crate::{
    Config, Context, QueryAnswer, commit,
    datagen::RandomDataGen,
    execute_read_query_in, execute_write_query_in,
    utils::{CountResults, unpack_result},
};

/// See `BenchmarkRunner` implementations for the order in which these functions are called.
pub trait SimpleBenchmark {
    type IterInput;
    type IterOutput: SimpleReport;

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

    /// Create given_rows if needed
    fn prepare_iter(&self, context: &Context, database: Arc<Database<WALClient>>) -> Self::IterInput;

    /// The actual iteration which gets timed over and over again.
    fn run_iter(
        &self,
        context: &Context,
        database: Arc<Database<WALClient>>,
        input: Self::IterInput,
    ) -> Self::IterOutput;
}

pub type PreloadDataFn = Box<dyn Fn(Arc<Database<WALClient>>)>;
pub type PrepareIterFn<IN> = Box<dyn Fn(Arc<Database<WALClient>>) -> IN>;
pub type BenchmarkedFn<IN, OUT> = Box<dyn Fn(Arc<Database<WALClient>>, IN) -> OUT>;

pub trait SimpleReport {
    fn report(reports: &[Self])
    where
        Self: Sized;
}

pub struct TypeDBMicroBenchmark<IN, OUT: SimpleReport> {
    pub name: &'static str,
    pub schema: &'static str,
    pub preload_data_fn: Option<PreloadDataFn>,
    pub prepare_iter_fn: PrepareIterFn<IN>,
    pub benchmark_fn: BenchmarkedFn<IN, OUT>,
}

impl<IN, OUT: SimpleReport> SimpleBenchmark for TypeDBMicroBenchmark<IN, OUT> {
    type IterInput = IN;
    type IterOutput = OUT;

    fn name(&self) -> &'_ str {
        self.name
    }

    fn prepare_database(&self, _context: &Context, database: Arc<Database<WALClient>>) {
        crate::create_schema(database.clone(), self.schema);
        if let Some(preload_fn) = &self.preload_data_fn {
            preload_fn(database.clone())
        }
    }

    fn prepare_iter(&self, _context: &Context, database: Arc<Database<WALClient>>) -> Self::IterInput {
        (self.prepare_iter_fn)(database)
    }

    fn run_iter(
        &self,
        _context: &Context,
        database: Arc<Database<WALClient>>,
        input: Self::IterInput,
    ) -> Self::IterOutput {
        (self.benchmark_fn)(database, input)
    }
}

pub fn sanity_check() -> TypeDBMicroBenchmark<(), ()> {
    // Just to ensure the reported time is just the benchmark_fn
    TypeDBMicroBenchmark {
        name: "sanity_check",
        schema: "define entity person;",
        preload_data_fn: Some(Box::new(|_| std::thread::sleep(Duration::from_millis(40)))),
        prepare_iter_fn: Box::new(|_| std::thread::sleep(Duration::from_millis(20))),
        benchmark_fn: Box::new(|_, _| std::thread::sleep(Duration::from_millis(10))),
    }
}

// Util return
pub struct TxQueryProfile {
    pub tx_profile: Option<TransactionProfile>,
    pub query_profile: Arc<QueryProfile>,
}

pub struct MultiQueryTxProfile {
    pub tx_profile: TransactionProfile,
    pub query_profiles: Vec<Arc<QueryProfile>>,
    pub time_elapsed: Duration,
}

pub struct MultiTxMultiQueryProfile {
    pub profiles: Vec<MultiQueryTxProfile>,
}

// Initial data
pub fn no_initial_data() -> Option<PreloadDataFn> {
    None
}

// prepare_iter
pub fn no_given_rows() -> PrepareIterFn<Option<GivenRowsSimple>> {
    Box::new(|_: Arc<Database<WALClient>>| None)
}

pub fn n_empty_given_rows(n: usize) -> PrepareIterFn<Option<GivenRowsSimple>> {
    Box::new(move |_: Arc<Database<WALClient>>| {
        let variables = Vec::new();
        let mut rows = Vec::with_capacity(n);
        rows.resize(n, Vec::new());
        Some(GivenRowsSimple { variables, rows })
    })
}

pub fn given_rows_with(
    n_rows: usize,
    variables: Vec<String>,
    gen_row: fn(&mut RandomDataGen) -> Vec<GivenRowEntry>,
) -> PrepareIterFn<Option<GivenRowsSimple>> {
    Box::new(move |_: Arc<Database<WALClient>>| {
        let variables = variables.clone();
        let mut rows = Vec::with_capacity(n_rows);
        let mut rng = RandomDataGen::new();
        let gen_row = move || gen_row(&mut rng);
        rows.resize_with(n_rows, gen_row);
        Some(GivenRowsSimple { variables, rows })
    })
}

// // SimpleReport implementations
impl SimpleReport for TxQueryProfile {
    fn report(reports: &[Self]) {
        if reports.is_empty() {
            return;
        }

    }
 }

// fn stats_mean(values: &[f64]) -> f64 {
//     if values.is_empty() {
//         return 0.0;
//     }
//     values.iter().sum::<f64>() / values.len() as f64
// }
//
// fn stats_stddev(values: &[f64], mean: f64) -> f64 {
//     if values.len() < 2 {
//         return 0.0;
//     }
//     let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
//     variance.sqrt()
// }
//
// impl SimpleReport for () {
//     fn report(_: &[Self]) {}
// }
//
// impl SimpleReport for TxQueryProfile {
//     fn report(reports: &[Self]) {
//         if reports.is_empty() {
//             return;
//         }
//         let query_us: Vec<f64> =
//             reports.iter().map(|r| r.query_profile.total_duration().as_nanos() as f64 / 1000.0).collect();
//         let commit_us: Vec<f64> = reports
//             .iter()
//             .map(|r| r.tx_profile.as_ref().map_or(0.0, |tp| tp.commit_total().as_nanos() as f64 / 1000.0))
//             .collect();
//         let total_us: Vec<f64> = query_us.iter().zip(&commit_us).map(|(q, c)| q + c).collect();
//
//         let mean_q = stats_mean(&query_us);
//         let mean_c = stats_mean(&commit_us);
//         let mean_t = stats_mean(&total_us);
//         let std_q = stats_stddev(&query_us, mean_q);
//         let std_c = stats_stddev(&commit_us, mean_c);
//         let std_t = stats_stddev(&total_us, mean_t);
//
//         println!("=== TxQueryProfile ({} sample(s)) ===", reports.len());
//         println!("{:>10}  {:>14}  {:>14}  {:>14}", "", "Query (µs)", "Commit (µs)", "Total (µs)");
//         println!("{:>10}  {:>14.1}  {:>14.1}  {:>14.1}", "Mean", mean_q, mean_c, mean_t);
//         if reports.len() > 1 {
//             println!("{:>10}  {:>14.1}  {:>14.1}  {:>14.1}", "StdDev", std_q, std_c, std_t);
//         }
//     }
// }
//
// const MAX_TABLE_ROWS: usize = 25;
//
// impl SimpleReport for MultiTxMultiQueryProfile {
//     fn report(reports: &[Self]) {
//         if reports.is_empty() {
//             return;
//         }
//
//         let n_txns = reports.iter().map(|r| r.profiles.len()).max().unwrap_or(0);
//         let n_samples = reports.len();
//
//         // Average per-txn timing across samples
//         let mut per_txn_query_us = vec![0.0f64; n_txns];
//         let mut per_txn_commit_us = vec![0.0f64; n_txns];
//         let mut per_txn_sample_counts = vec![0usize; n_txns];
//
//         for report in reports {
//             for (i, tx) in report.profiles.iter().enumerate() {
//                 let q_us: f64 =
//                     tx.query_profiles.iter().map(|q| q.total_duration().as_nanos() as f64 / 1000.0).sum();
//                 let c_us = tx.tx_profile.commit_total().as_nanos() as f64 / 1000.0;
//                 per_txn_query_us[i] += q_us;
//                 per_txn_commit_us[i] += c_us;
//                 per_txn_sample_counts[i] += 1;
//             }
//         }
//         for i in 0..n_txns {
//             if per_txn_sample_counts[i] > 0 {
//                 per_txn_query_us[i] /= per_txn_sample_counts[i] as f64;
//                 per_txn_commit_us[i] /= per_txn_sample_counts[i] as f64;
//             }
//         }
//         let per_txn_total_us: Vec<f64> =
//             per_txn_query_us.iter().zip(&per_txn_commit_us).map(|(q, c)| q + c).collect();
//
//         let mean_q = stats_mean(&per_txn_query_us);
//         let mean_c = stats_mean(&per_txn_commit_us);
//         let mean_t = stats_mean(&per_txn_total_us);
//         let std_q = stats_stddev(&per_txn_query_us, mean_q);
//         let std_c = stats_stddev(&per_txn_commit_us, mean_c);
//         let std_t = stats_stddev(&per_txn_total_us, mean_t);
//
//         // Bucket large n_txns so the table stays readable
//         let bucket_size = n_txns.div_ceil(MAX_TABLE_ROWS).max(1);
//         let n_buckets = n_txns.div_ceil(bucket_size);
//
//         let mut bkt_query = vec![0.0f64; n_buckets];
//         let mut bkt_commit = vec![0.0f64; n_buckets];
//         let mut bkt_count = vec![0usize; n_buckets];
//         for i in 0..n_txns {
//             let b = i / bucket_size;
//             bkt_query[b] += per_txn_query_us[i];
//             bkt_commit[b] += per_txn_commit_us[i];
//             bkt_count[b] += 1;
//         }
//         for b in 0..n_buckets {
//             if bkt_count[b] > 0 {
//                 bkt_query[b] /= bkt_count[b] as f64;
//                 bkt_commit[b] /= bkt_count[b] as f64;
//             }
//         }
//         let bkt_total: Vec<f64> = bkt_query.iter().zip(&bkt_commit).map(|(q, c)| q + c).collect();
//
//         println!("=== MultiTxMultiQueryProfile: {} txns, {} sample(s) ===", n_txns, n_samples);
//         println!("{:>12}  {:>12}  {:>12}  {:>12}  {:>12}", "Txn", "Query (µs)", "Commit (µs)", "Total (µs)", "ΔMean (µs)");
//         println!("{}", "-".repeat(68));
//         for b in 0..n_buckets {
//             let start_txn = b * bucket_size + 1;
//             let end_txn = ((b + 1) * bucket_size).min(n_txns);
//             let label = if start_txn == end_txn {
//                 format!("{}", start_txn)
//             } else {
//                 format!("{}-{}", start_txn, end_txn)
//             };
//             let delta = bkt_total[b] - mean_t;
//             println!("{:>12}  {:>12.1}  {:>12.1}  {:>12.1}  {:>+12.1}", label, bkt_query[b], bkt_commit[b], bkt_total[b], delta);
//         }
//         println!("{}", "-".repeat(68));
//         println!("{:>12}  {:>12.1}  {:>12.1}  {:>12.1}", "Mean", mean_q, mean_c, mean_t);
//         println!("{:>12}  {:>12.1}  {:>12.1}  {:>12.1}", "StdDev", std_q, std_c, std_t);
//
//         let avg_wall = reports.iter().map(|r| r.time_elapsed.as_secs_f64()).sum::<f64>() / n_samples as f64;
//         println!("Wall time: {:.3}s", avg_wall);
//     }
// }
//
// // prepare_iter helpers
// pub fn prepare_nothing() -> PrepareIterFn<()> {
//     Box::new(|_: Arc<Database<WALClient>>| ())
// }
//
// // queries
// pub fn query_in_read_tx(query: &str) -> BenchmarkedFn<(), TxQueryProfile> {
//     let query_owned = query.to_owned();
//     Box::new(move |database: Arc<Database<WALClient>>, _: ()| {
//         let tx = TransactionRead::open(database, TransactionOptions::default()).unwrap();
//         let (query_result, tx) =
//             unpack_result(execute_read_query_in::<_, CountResults>(tx, query_owned.as_str(), None, true));
//         let QueryAnswer { profile: query_profile, answer: _ } = query_result.unwrap();
//         let tx_profile = commit(tx).unwrap();
//         TxQueryProfile { tx_profile: Some(tx_profile), query_profile }
//     })
// }

pub fn query_in_write_tx(query: &str) -> BenchmarkedFn<Option<GivenRowsSimple>, TxQueryProfile> {
    let query_owned = query.to_owned();
    Box::new(move |database: Arc<Database<WALClient>>, given_rows: Option<GivenRowsSimple>| {
        let tx = TransactionWrite::open(database, TransactionOptions::default()).unwrap();
        let (query_result, tx) =
            unpack_result(execute_write_query_in::<_, CountResults>(tx, query_owned.as_str(), given_rows, true));
        let QueryAnswer { profile: query_profile, answer: _ } = query_result.unwrap();
        let tx_profile = commit(tx).unwrap();
        TxQueryProfile { tx_profile: Some(tx_profile), query_profile }
    })
}
