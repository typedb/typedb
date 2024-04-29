/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


// pub struct BencmarkOptions { }  // impl BencmarkOptions {} // If we want to group the constants
const N_DATABASES: usize = 1;
const N_COL_FAMILIES_PER_DB: usize = 1;

const KEY_SIZE: usize = 64;
const VALUE_SIZE: usize = 0;


pub trait RocksDatabase {
    fn open_batch(&self) -> impl RocksWriteBatch;
}

pub trait RocksWriteBatch {
    fn put<const KEY_SIZE: usize, const VALUE_SIZE: usize>(&mut self, database_index: usize, key: [u8; KEY_SIZE], value: [u8; VALUE_SIZE]);
    fn commit(self) -> Result<(), speedb::Error>;
}

mod benchmark_runner {

    use crate::{RocksDatabase, RocksWriteBatch};
    const VALUE_EMPTY :[u8;0] = [];
    pub struct ThreadedBenchmark {
    }

    fn generate_key_value() -> ([u8; crate::KEY_SIZE], [u8; crate::VALUE_SIZE]) {
        ([0x12; crate::KEY_SIZE], VALUE_EMPTY)// TODO
    }


    impl ThreadedBenchmark {
        pub fn run(n_threads: usize, database: &impl RocksDatabase) {
            let N_BATCHES = 1;
            let N_KEYS_PER_BATCH = 1;
            for i in 0..N_BATCHES {
                let mut batch = database.open_batch();
                for j in 0..N_KEYS_PER_BATCH {
                    let (k, v) = generate_key_value();
                    batch.put(0, k, v);
                }
                batch.commit().unwrap();
            }
        }
    }

}

pub mod bench_rocks_impl;

use benchmark_runner::{ThreadedBenchmark};
use bench_rocks_impl::rocks_database::create_non_transactional_db;

fn main() {
    let database = create_non_transactional_db::<N_DATABASES>().unwrap();
    ThreadedBenchmark::run(1, &database);
}

