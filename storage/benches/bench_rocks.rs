/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */



pub mod bench_rocks_impl;

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

pub struct BenchmarkRunner {
    n_threads: u16,
    n_batches: usize,
    n_keys_per_batch: u64,
}

impl BenchmarkRunner {
    const VALUE_EMPTY :[u8;0] = [];
    fn run(&self, database_arc: &impl RocksDatabase) -> BenchmarkResult {
        debug_assert_eq!(1, N_DATABASES, "I've not bothered implementing multiple databases");
        let batch_timings: Vec<RwLock<Duration>> = (0..self.n_batches).into_iter().map({|_| RwLock::new(Duration::from_secs(0)) }).collect();
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
                        for _ in 0..self.n_keys_per_batch {
                            let (k, v) = Self::generate_key_value(&mut rng);
                            write_batch.put(0, k, v);
                        }
                        write_batch.commit().unwrap();
                        let mut duration_for_batch = batch_timings.get(batch_number).unwrap().write().unwrap();
                        *duration_for_batch = batch_start_instant.elapsed();
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

fn main() {
    let database = create_non_transactional_db::<N_DATABASES>().unwrap();

    let benchmarker = BenchmarkRunner { n_threads: 1, n_batches: 5, n_keys_per_batch: 5 };
    benchmarker.run(&database);
    println!("Done");
}
