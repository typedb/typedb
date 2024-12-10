/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// RocksDB properties. The full list of available properties is available here:
// https://github.com/facebook/rocksdb/blob/20357988345b02efcef303bc274089111507e160/include/rocksdb/db.h#L750
pub(crate) mod rocksdb {
    pub(crate) const PROPERTY_ESTIMATE_LIVE_DATA_SIZE: &str = "rocksdb.estimate-live-data-size";
    pub(crate) const PROPERTY_ESTIMATE_NUM_KEYS: &str = "rocksdb.estimate-num-keys";
}
