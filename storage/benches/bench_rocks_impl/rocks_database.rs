/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use speedb::{Options, WriteOptions};
use non_transactional_database::NonTransactionalDatabase;

fn database_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true); // TODO
    opts
}

fn write_options() -> WriteOptions {
    WriteOptions::default() // TODO
}

pub fn create_non_transactional_db<const N_DATABASES: usize>() -> Result<NonTransactionalDatabase<N_DATABASES>, speedb::Error> {
    non_transactional_database::NonTransactionalDatabase::<N_DATABASES>::setup(database_options(), write_options())
}


mod non_transactional_database {
    // if we implement transactional rocks, extract trait.
    use test_utils::create_tmp_dir;
    use speedb::{Options, DB, WriteBatch, WriteOptions};
    use std::iter::zip;
    use crate::{RocksDatabase, RocksWriteBatch};


    pub struct NonTransactionalDatabase<const N_DATABASES: usize> {
        databases: [DB; crate::N_DATABASES],
        write_options: WriteOptions,
    }

    impl<const N_DATABASES: usize> NonTransactionalDatabase<N_DATABASES> {
        pub(in crate::bench_rocks_impl::rocks_database) fn setup(options: Options, write_options: WriteOptions) -> Result<Self, speedb::Error> {
            let databases = core::array::from_fn(|_| {
                DB::open(&options, &create_tmp_dir()).unwrap()
            });
            Ok(Self { databases, write_options })
        }
    }

    impl<const N_DATABASES: usize> RocksDatabase for NonTransactionalDatabase<N_DATABASES> {
        fn open_batch(&self) -> impl RocksWriteBatch {
            let write_batches = core::array::from_fn(|_| { WriteBatch::default() });
            NonTransactionalWriteBatch { database: self, write_batches }
        }
    }

    pub struct NonTransactionalWriteBatch<'this, const N_DATABASES: usize> {
        database: &'this NonTransactionalDatabase<N_DATABASES>,
        write_batches: [WriteBatch; N_DATABASES],
    }

    impl<'this, const N_DATABASES: usize> RocksWriteBatch for NonTransactionalWriteBatch<'this, N_DATABASES> {
        fn put<const KEY_SIZE: usize, const VALUE_SIZE: usize>(&mut self, database_index: usize, key: [u8; KEY_SIZE], value: [u8; VALUE_SIZE]) {
            self.write_batches[database_index].put(key, value)
        }

        fn commit(self) -> Result<(), speedb::Error> {
            let write_options = &self.database.write_options;
            for (db, write_batch) in zip(&self.database.databases, self.write_batches) {
                db.write_opt(write_batch, write_options)?
            }
            Ok(())
        }
    }
}
