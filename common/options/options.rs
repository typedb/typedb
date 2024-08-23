/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use resource::constants::server::{DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS, DEFAULT_TRANSACTION_PARALLEL};

#[derive(Debug)]
pub struct TransactionOptions {
    pub parallel: bool,
    pub schema_lock_acquire_timeout_millis: u64,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            parallel: DEFAULT_TRANSACTION_PARALLEL,
            schema_lock_acquire_timeout_millis: DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS,
        }
    }
}
