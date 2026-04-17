/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use durability::wal::WAL;
use encoding::EncodingKeyspace;
use storage::{MVCCStorage, durability_client::WALClient};
use test_utils::{TempDir, create_tmp_dir, init_logging};

pub fn create_core_storage() -> (TempDir, Arc<MVCCStorage<WALClient>>) {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage =
        Arc::new(MVCCStorage::create::<EncodingKeyspace>("db_storage", &storage_path, WALClient::new(wal)).unwrap());
    (storage_path, storage)
}
