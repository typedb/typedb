/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::keyspace::KeyspacesError;
use error::typedb_error;
use kv::KVStoreError;
use std::sync::Arc;

typedb_error!(
    pub MVCCStorageError(component = "MVCC Storage", prefix = "MST") {
        FailedToDeleteStorage(
            1,
            "Failed to delete MVCC storage '{storage_name}'.",
            storage_name: String,
            source: Arc<std::io::Error>
        ),
        KeyspaceError(
            2,
            "Error in keyspace '{keyspace_name}' of MVV storage '{storage_name}'.",
            storage_name: String,
            keyspace_name: &'static str,
            typedb_source: Arc<dyn KVStoreError>
        ),
        KeyspaceDeleteError(
            3,
            "Failed to delete keyspaces for storage '{storage_name}'.",
            storage_name: String,
            typedb_source: KeyspacesError
        ),
    }
);
