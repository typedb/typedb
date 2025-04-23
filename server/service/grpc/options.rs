/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use options::{QueryOptions, TransactionOptions};
use resource::constants::server::{
    DEFAULT_ANSWER_COUNT_LIMIT_GRPC, DEFAULT_INCLUDE_INSTANCE_TYPES, DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS,
    DEFAULT_TRANSACTION_PARALLEL, DEFAULT_TRANSACTION_TIMEOUT_MILLIS,
};
use typedb_protocol::options::{Query as QueryOptionsProto, Transaction as TransactionOptionsProto};

pub(crate) fn transaction_options_from_proto(proto: Option<TransactionOptionsProto>) -> TransactionOptions {
    let Some(proto) = proto else {
        return TransactionOptions::default();
    };

    TransactionOptions {
        parallel: proto.parallel.unwrap_or(DEFAULT_TRANSACTION_PARALLEL),
        schema_lock_acquire_timeout_millis: proto
            .schema_lock_acquire_timeout_millis
            .unwrap_or(DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS),
        transaction_timeout_millis: proto.transaction_timeout_millis.unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_MILLIS),
    }
}

pub(crate) fn query_options_from_proto(proto: Option<QueryOptionsProto>) -> QueryOptions {
    let Some(proto) = proto else {
        return QueryOptions::default_grpc();
    };

    QueryOptions {
        include_instance_types: proto.include_instance_types.unwrap_or(DEFAULT_INCLUDE_INSTANCE_TYPES),
        answer_count_limit: DEFAULT_ANSWER_COUNT_LIMIT_GRPC,
    }
}
