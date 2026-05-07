/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{path::Path, sync::Arc};

use durability::wal::WAL;
use resource::constants::common::MB;
use storage::{
    MVCCStorage, StorageOpenError,
    durability_client::WALClient,
    keyspace::{KeyspaceSet, storage_resources::RocksResources},
    recovery::checkpoint::{CheckpointReader, CheckpointWriter},
};

pub mod mock_snapshot;

pub fn create_rocks_resources() -> RocksResources {
    // Small but non-zero limits sufficient for unit tests.
    RocksResources::new(64 * MB as usize, 64 * MB as usize)
}

#[macro_export]
macro_rules! test_keyspace_set {
    {$($variant:ident => $id:literal : $name: literal),* $(,)?} => {
        #[derive(Clone, Copy)]
        enum TestKeyspaceSet { $($variant),* }
        impl storage::keyspace::KeyspaceSet for TestKeyspaceSet {
            fn iter() -> impl Iterator<Item = Self> { [$(Self::$variant),*].into_iter() }
            fn id(&self) -> storage::keyspace::KeyspaceId {
                match *self { $(Self::$variant => storage::keyspace::KeyspaceId($id)),* }
            }
            fn name(&self) -> &'static str {
                match *self { $(Self::$variant => $name),* }
            }
            fn prefix_length(&self) -> Option<usize> {
                None
            }
        }
    };
}

pub fn create_storage<KS: KeyspaceSet>(path: &Path) -> Result<Arc<MVCCStorage<WALClient>>, StorageOpenError> {
    let wal = WAL::create(path).unwrap();
    let resources = create_rocks_resources();
    let storage = MVCCStorage::create::<KS>("storage", path, WALClient::new(wal), &resources)?;
    Ok(Arc::new(storage))
}

pub fn checkpoint_storage(storage: &MVCCStorage<WALClient>) -> CheckpointReader {
    let checkpoint = CheckpointWriter::new(storage.path().parent().unwrap()).unwrap();
    storage.checkpoint(&checkpoint).unwrap();
    checkpoint.finish().unwrap()
}

pub fn load_storage<KS: KeyspaceSet>(
    path: &Path,
    wal: WAL,
    checkpoint: Option<CheckpointReader>,
) -> Result<Arc<MVCCStorage<WALClient>>, StorageOpenError> {
    let resources = create_rocks_resources();
    let storage = MVCCStorage::load::<KS>("storage", path, WALClient::new(wal), &checkpoint, &resources)?;
    Ok(Arc::new(storage))
}
