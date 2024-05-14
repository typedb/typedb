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
use itertools::Itertools;
use rand::random;
use rand_core::RngCore;
use xoshiro::Xoshiro256Plus;
use crate::bench_rocks_impl::rocks_database::{create_typedb, rocks};

const N_DATABASES: usize = 1;

const KEY_SIZE: usize = 64;
const VALUE_SIZE: usize = 0;


pub trait RocksDatabase : Sync + Send {
    fn open_batch(&self) -> impl RocksWriteBatch;
}

pub trait RocksWriteBatch {
    type CommitError: std::fmt::Debug;
    fn put(&mut self, database_index: usize, key: [u8; KEY_SIZE]);
    fn commit(self) -> Result<(), Self::CommitError>;
}

pub struct BenchmarkResult {
    pub batch_timings: Vec<Duration>,
    pub total_time: Duration,
}

impl BenchmarkResult {
    fn print_report(&self, args: &CLIArgs, runner: &BenchmarkRunner) {
        println!("-- Report for benchmark: {} ---", args.database);
        println!("threads = {}, batches={}, batch_size={} ---", runner.n_threads, runner.n_batches, runner.batch_size);
        println!("key-size: {KEY_SIZE}; value_size: {VALUE_SIZE}");
        println!("cli_args: [{}]", args.for_report());
        // println!("- - - Batch timings (ns): - - -");
        // self.batch_timings.iter().enumerate().for_each(|(batch_id, time)| {
        //     println!("{:8}: {:12}", batch_id, time.as_nanos());
        // });
        let n_keys: usize = runner.n_batches * runner.batch_size;
        let data_size_mb : f64 = ((n_keys * (KEY_SIZE + VALUE_SIZE)) as f64) / ((1024 * 1024) as f64) ;
        println!("Summary:");
        println!("Total time: {:12} ms; total_keys: {:10}; data_size: {:8} MB\nrate: {:.2} keys/s = {:.2} MB/s ",
                 self.total_time.as_secs_f64() * 1000.0, n_keys, data_size_mb,
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
                    let mut in_rng = Xoshiro256Plus::from_seed_u64(random());
                    loop {
                        let batch_number = batch_counter.fetch_add(1, Ordering::Relaxed);
                        if batch_number >= self.n_batches { break; }
                        let mut write_batch = database_arc.open_batch();
                        let batch_start_instant = Instant::now();
                        for _ in 0..self.batch_size {
                            let (k,_) = Self::generate_key_value(&mut in_rng);
                            write_batch.put(0, k.clone());
                        }
                        write_batch.commit().unwrap();
                        let batch_stop =  batch_start_instant.elapsed();
                        let mut duration_for_batch = batch_timings.get(batch_number).unwrap().write().unwrap();
                        *duration_for_batch = batch_stop;
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

    fn generate_key_value(rng: &mut Xoshiro256Plus) -> ([u8; KEY_SIZE], [u8; VALUE_SIZE]) {
        // Rust's inbuilt ThreadRng is secure and slow. Xoshiro is significantly faster.
        // This ~(50 GB/s) is faster than generating 64 random bytes (~6 GB/s) or loading pre-generated (~18 GB/s).
        let mut key : [u8; KEY_SIZE] = [0; KEY_SIZE];
        let mut z = rng.next_u64();
        key[0..8].copy_from_slice(&z.to_le_bytes());
        z = u64::rotate_left(z, 1); // Rotation beats the compression.
        key[8..16].copy_from_slice(&z.to_le_bytes());
        z = u64::rotate_left(z, 1);
        key[16..24].copy_from_slice(&z.to_le_bytes());
        z = u64::rotate_left(z, 1);
        key[24..32].copy_from_slice(&z.to_le_bytes());
        z = u64::rotate_left(z, 1);
        key[32..40].copy_from_slice(&z.to_le_bytes());
        z = u64::rotate_left(z, 1);
        key[40..48].copy_from_slice(&z.to_le_bytes());
        z = u64::rotate_left(z, 1);
        key[48..56].copy_from_slice(&z.to_le_bytes());
        z = u64::rotate_left(z, 1);
        key[56..64].copy_from_slice(&z.to_le_bytes());
        (key , Self::VALUE_EMPTY)
    }
}

#[derive(Default)]
struct CLIArgs {
    database: String,

    n_threads: u16,
    n_batches: usize,
    batch_size: usize,

    rocks_disable_wal: Option<bool>,
    rocks_set_sync: Option<bool>,   // Needs WAL, fsync on write.
    rocks_write_buffer_mb: Option<usize>, // Size of memtable per column family. Useful for getting a no-op timing.
}

impl CLIArgs {
    const VALID_ARGS : [&'static str; 7] = [
        "database", "threads", "batches", "batch_size",
        "rocks_disable_wal", "rocks_set_sync", "rocks_write_buffer_mb"
    ];
    fn get_arg_as<T: std::str::FromStr>(args:&HashMap<String, String>, key: &str, required: bool) -> Result<Option<T>, String> {
        match args.get(&key.to_owned()) {
            None => { if required { Err(format!("Pass {key} as arg")) } else { Ok(None) } },
            Some(value) => Ok(Some(value.parse().map_err(|_| format!("Error parsing value for {key}"))?))
        }
    }
    fn parse_args() -> Result<CLIArgs, String> {
        let arg_map: HashMap<String, String> = std::env::args()
            .filter_map(|arg| arg.split_once("=").map(|(s1, s2)| (s1.to_string(), s2.to_string())))
            .collect();
        let invalid_keys = arg_map.keys().filter(|key| !Self::VALID_ARGS.contains(&key.as_str())).join(",");
        if ! invalid_keys.is_empty() {
            return Err(format!("Invalid keys: {invalid_keys}"));
        }

        let mut args = CLIArgs::default();
        args.database = Self::get_arg_as::<String>(&arg_map, "database", true)?.unwrap();
        args.n_threads = Self::get_arg_as::<u16>(&arg_map, "threads", true)?.unwrap();
        args.n_batches = Self::get_arg_as::<usize>(&arg_map, "batches", true)?.unwrap();
        args.batch_size = Self::get_arg_as::<usize>(&arg_map, "batch_size", true)?.unwrap();

        args.rocks_disable_wal = Self::get_arg_as::<bool>(&arg_map, "rocks_disable_wal", false)?;
        args.rocks_set_sync = Self::get_arg_as::<bool>(&arg_map, "rocks_set_sync", false)?;
        args.rocks_write_buffer_mb = Self::get_arg_as::<usize>(&arg_map, "rocks_write_buffer_mb", false)?;

        Ok(args)
    }

    fn for_report(&self) -> String {
        let mut s = "".to_string();
        if let Some(val) = self.rocks_disable_wal { s.push_str(format!("rocks_disable_wal={val}").as_str()); }
        if let Some(val) = self.rocks_set_sync { s.push_str(format!("rocks_set_sync={val}").as_str()); }
        if let Some(val) = self.rocks_write_buffer_mb { s.push_str(format!("rocks_write_buffer_mb={val}").as_str()); }
        s
    }
}

fn run_for(args: &CLIArgs, database: &impl RocksDatabase) {
    let benchmarker = BenchmarkRunner {
        n_threads: args.n_threads,
        n_batches: args.n_batches,
        batch_size: args.batch_size,
    };

    let report = benchmarker.run(database);
    report.print_report(&args, &benchmarker);
}


fn main() {
    let args = CLIArgs::parse_args().unwrap();
    match args.database.as_str() {
        "rocks" => run_for(&args, &rocks::<N_DATABASES>(&args).unwrap()),
        "typedb" => run_for(&args, &create_typedb::<N_DATABASES>().unwrap()),
        _ => panic!("Unrecognised argument for database. Supported: rocks, typedb")
    }
}
