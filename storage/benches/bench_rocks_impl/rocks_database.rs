/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use speedb::{Options, WriteOptions};
use non_transactional_database::NonTransactionalDatabase;
use storage::StorageRecoverError;
use crate::bench_rocks_impl::rocks_database::typedb_database::TypeDBDatabase;

fn database_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true); // TODO
    opts
}

fn write_options() -> WriteOptions {
    WriteOptions::default() // TODO
}

pub fn create_non_transactional_db<const N_DATABASES: usize>() -> Result<NonTransactionalDatabase<N_DATABASES>, speedb::Error> {
    NonTransactionalDatabase::<N_DATABASES>::setup(database_options(), write_options())
}

pub fn create_typedb<const N_DATABASES: usize>() -> Result<TypeDBDatabase<N_DATABASES>, StorageRecoverError> {
    TypeDBDatabase::<N_DATABASES>::setup()
}


mod non_transactional_database {
    // if we implement transactional rocks, extract trait.
    use test_utils::create_tmp_dir;
    use speedb::{Options, DB, WriteBatch, WriteOptions};
    use std::iter::zip;
    use crate::{RocksDatabase, RocksWriteBatch};
    use crate::bench_rocks_impl::rocks_database::{database_options, non_transactional_database, write_options};


    pub struct NonTransactionalDatabase<const N_DATABASES: usize> {
        databases: [DB; crate::N_DATABASES],
        write_options: WriteOptions,
    }

    impl<const N_DATABASES: usize> NonTransactionalDatabase<N_DATABASES> {
        pub(super) fn setup(options: Options, write_options: WriteOptions) -> Result<Self, speedb::Error> {
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
        type CommitError = speedb::Error;
        fn put(&mut self, database_index: usize, key: [u8; crate::KEY_SIZE]) {
            self.write_batches[database_index].put(key, [])
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

mod typedb_database {
    use std::sync::Arc;
    use bytes::byte_array::ByteArray;
    use durability::wal::WAL;
    use storage::key_value::StorageKeyArray;
    use storage::{KeyspaceSet, MVCCStorage, StorageRecoverError};
    use storage::keyspace::KeyspaceId;
    use storage::snapshot::{CommittableSnapshot, SnapshotError, WritableSnapshot, WriteSnapshot};
    use test_utils::create_tmp_dir;
    use crate::{KEY_SIZE, RocksDatabase, RocksWriteBatch};

    pub struct TypeDBDatabase<const N_DATABASES: usize> {
        storage : Arc<MVCCStorage<WAL>>,
    }

    impl<const N_DATABASES:usize> TypeDBDatabase<N_DATABASES> {
        pub(super) fn setup() -> Result<Self, StorageRecoverError> {
            let name = "bench_rocks__typedb";
            let path = create_tmp_dir().join(name);
            Ok(Self { storage: Arc::new(MVCCStorage::<WAL>::recover::<BenchKeySpace>(name, &path)?) })
        }
    }

    impl<const N_DATABASES:usize> RocksDatabase for TypeDBDatabase<N_DATABASES> {

        fn open_batch(&self) -> impl RocksWriteBatch {
            TypeDBSnapshot { snapshot: self.storage.clone().open_snapshot_write() }
        }
    }

    pub struct TypeDBSnapshot {
        snapshot: WriteSnapshot<WAL>,
    }

    impl TypeDBSnapshot {
        const KEYSPACES: [BenchKeySpace; 1]  = [BenchKeySpace { id: 0 }];
        const KEYSPACE_NAMES: [&'static str; 1]  = ["BenchKeySpace[1]"];
    }
    impl RocksWriteBatch for TypeDBSnapshot {
        type CommitError = SnapshotError;
        fn put(&mut self, database_index: usize, key: [u8; KEY_SIZE]) {
            debug_assert_eq!(0,database_index, "Not implemented for multiple databases");
            self.snapshot.put(StorageKeyArray::new(Self::KEYSPACES[0], ByteArray::inline(key, KEY_SIZE)))
        }

        fn commit(self) -> Result<(), Self::CommitError> {
            self.snapshot.commit()
        }
    }

    #[derive(Copy, Clone)]
    pub struct BenchKeySpace {
        // I guess this is what we'll parameterise by N_DATABASES
        id: u8,
    }

    impl KeyspaceSet for BenchKeySpace {
        fn iter() -> impl Iterator<Item=Self> {
            TypeDBSnapshot::KEYSPACES.into_iter()
        }

        fn id(&self) -> KeyspaceId {
            KeyspaceId(self.id)
        }

        fn name(&self) -> &'static str {
            &TypeDBSnapshot::KEYSPACE_NAMES[self.id as usize]
        }
    }
}
