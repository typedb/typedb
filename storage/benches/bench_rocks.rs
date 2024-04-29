/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */



pub mod bench_rocks_impl;

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use rand::{thread_rng, rngs::ThreadRng, Rng};
use bench_rocks_impl::{
    rocks_database::{create_non_transactional_db},
};

const N_DATABASES: usize = 1;
const N_COL_FAMILIES_PER_DB: usize = 1;

const KEY_SIZE: usize = 64;
const VALUE_SIZE: usize = 0;


pub trait RocksDatabase : std::marker::Sync + std::marker::Send {
    fn open_batch(&self) -> impl RocksWriteBatch;
}

pub trait RocksWriteBatch {
    fn put<const KEY_SIZE: usize, const VALUE_SIZE: usize>(&mut self, database_index: usize, key: [u8; KEY_SIZE], value: [u8; VALUE_SIZE]);
    fn commit(self) -> Result<(), speedb::Error>;
}


pub struct BenchmarkResult {
    pub batch_timings: Vec<Duration>,
    pub total_time: Duration,
}

impl BenchmarkResult {
    fn print_report(&self, runner: &BenchmarkRunner) {
        println!("-- Report for benchmark ---");
        println!("threads = {}, batches={}, batch_size={} ---", runner.n_threads, runner.n_batches, runner.batch_size);
        println!("key-size: {KEY_SIZE}; value_size: {VALUE_SIZE}");
        println!("Batch timings (ns):");
        println!("- - - - - - - -");
        self.batch_timings.iter().enumerate().for_each(|(batch_id, time)| {
            println!("{:8}: {:12}", batch_id, time.as_nanos());
        });
        println!("- - - - - - - -");

        let n_keys: usize = runner.n_batches * runner.batch_size;
        let data_size_mb : f64 = ((n_keys * (KEY_SIZE + VALUE_SIZE)) as f64) / ((1024 * 1024) as f64) ;
        println!("Summary:");
        println!("Total time: {:12} ns; total_keys: {:10}; data_size: {:8} MB\nrate: {:.2} keys/s = {:.2} MB/s ",
                 self.total_time.as_nanos(), n_keys, data_size_mb,
                 n_keys as f64 / self.total_time.as_secs_f64(), data_size_mb / self.total_time.as_secs_f64(),
        );
    }
}

pub struct BenchmarkRunner {
    n_threads: u16,
    n_batches: usize,
    batch_size: usize,
}

impl BenchmarkRunner {
    const VALUE_EMPTY :[u8;0] = [];
    fn run(&self, database_arc: &impl RocksDatabase) -> BenchmarkResult {
        debug_assert_eq!(1, N_DATABASES, "I've not bothered implementing multiple databases");
        let batch_timings: Vec<RwLock<Duration>> = (0..self.n_batches).into_iter().map(|_| RwLock::new(Duration::from_secs(0))).collect();
        let batch_counter = AtomicUsize::new(0);
        let benchmark_start_instant = Instant::now();
        thread::scope(|s| {
            for _ in 0..self.n_threads {
                s.spawn(|| {
                    let mut rng = thread_rng();
                    loop {
                        let batch_number = batch_counter.fetch_add(1, Ordering::Relaxed);
                        if batch_number >= self.n_batches { break; }

                        let mut write_batch = database_arc.open_batch();
                        let batch_start_instant = Instant::now();
                        for _ in 0..self.batch_size {
                            let (k, v) = Self::generate_key_value(&mut rng);
                            write_batch.put(0, k, v);
                        }
                        write_batch.commit().unwrap();
                        let mut duration_for_batch = batch_timings.get(batch_number).unwrap().write().unwrap();
                        *duration_for_batch = batch_start_instant.elapsed();
                        // println!("Thread completed batch {}", batch_number)
                    }
                });
            }
        });
        assert!(batch_counter.load(Ordering::Relaxed) >= self.n_batches);
        let total_time = benchmark_start_instant.elapsed();
        BenchmarkResult {
            batch_timings : batch_timings.iter().map(|x| x.read().unwrap().clone()).collect(),
            total_time
        }
    }

    fn generate_key_value(rng: &mut ThreadRng) -> ([u8; crate::KEY_SIZE], [u8; crate::VALUE_SIZE]) {
        let mut key : [u8; crate::KEY_SIZE] = [0; crate::KEY_SIZE];
        rng.fill(&mut key);
        (key, Self::VALUE_EMPTY)
    }
}

fn get_arg_as<T: std::str::FromStr>(args:&HashMap<String, String>, key: &str) -> Result<T, String> {
    match args.get(&key.to_string()) {
        None => Err(format!("Pass {key} as arg")),
        Some(value) => value.parse().map_err(|_| format!("Error parsing value for {key}"))
    }
}

fn main() {
    let args : HashMap<String, String> = std::env::args()
        .filter_map(|arg| arg.split_once("=").map(|(s1, s2)| (s1.to_string(), s2.to_string())))
        .collect();

    let database = create_non_transactional_db::<N_DATABASES>().unwrap();
    let benchmarker = BenchmarkRunner {
        n_threads: get_arg_as::<u16>(&args, "threads").unwrap(),
        n_batches: get_arg_as::<usize>(&args, "batches").unwrap(),
        batch_size: get_arg_as::<usize>(&args, "batch_size").unwrap()
    };
    let report = benchmarker.run(&database);
    println!("Done");
    report.print_report(&benchmarker);
}
