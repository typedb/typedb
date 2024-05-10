/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::Path;
use std::sync::Arc;
use durability::DurabilityService;
use durability::wal::WAL;
use storage::keyspace::KeyspaceSet;
use storage::{MVCCStorage, StorageOpenError};
use storage::recovery::checkpoint::Checkpoint;
use test_utils::create_tmp_dir;
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
        }
    };
}

pub fn create_storage<KS: KeyspaceSet>(path: &Path) -> Result<Arc<MVCCStorage<WAL>>, StorageOpenError> {
    let wal = WAL::create(&path).unwrap();
    let storage = MVCCStorage::create::<KS>("storage", &path, wal)?;
    Ok(Arc::new(storage))
}

pub fn checkpoint_storage(storage: &MVCCStorage<WAL>) -> Checkpoint {
    let mut checkpoint = Checkpoint::new(storage.path().parent().unwrap()).unwrap();
    storage.checkpoint(&checkpoint).unwrap();
    checkpoint.finish().unwrap();
    checkpoint
}

pub fn load_storage<KS: KeyspaceSet>(path: &Path, wal: WAL, checkpoint: Option<Checkpoint>) -> Result<Arc<MVCCStorage<WAL>>, StorageOpenError> {
    let storage = MVCCStorage::load::<KS>("storage", path, wal, checkpoint)?;
    Ok(Arc::new(storage))
}