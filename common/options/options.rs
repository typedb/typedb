/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use resource::constants::server::{
    DEFAULT_ANSWER_COUNT_LIMIT_GRPC, DEFAULT_ANSWER_COUNT_LIMIT_HTTP, DEFAULT_INCLUDE_INSTANCE_TYPES,
    DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS, DEFAULT_TRANSACTION_PARALLEL, DEFAULT_TRANSACTION_TIMEOUT_MILLIS,
};

#[derive(Debug)]
pub struct TransactionOptions {
    pub parallel: bool,
    pub schema_lock_acquire_timeout_millis: u64,
    pub transaction_timeout_millis: u64,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            parallel: DEFAULT_TRANSACTION_PARALLEL,
            schema_lock_acquire_timeout_millis: DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS,
            transaction_timeout_millis: DEFAULT_TRANSACTION_TIMEOUT_MILLIS,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct QueryOptions {
    pub include_instance_types: bool,
    pub answer_count_limit: Option<usize>,
}

impl QueryOptions {
    pub fn default_grpc() -> Self {
        Self {
            include_instance_types: DEFAULT_INCLUDE_INSTANCE_TYPES,
            answer_count_limit: DEFAULT_ANSWER_COUNT_LIMIT_GRPC,
        }
    }

    pub fn default_http() -> Self {
        Self {
            include_instance_types: DEFAULT_INCLUDE_INSTANCE_TYPES,
            answer_count_limit: DEFAULT_ANSWER_COUNT_LIMIT_HTTP,
        }
    }
}
