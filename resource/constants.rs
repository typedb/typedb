/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod server {
    use std::time::Duration;

    pub const ASCII_LOGO: &str = include_str!("typedb-ascii.txt");

    pub const DISTRIBUTION_NAME: &str = "TypeDB CE";
    pub const VERSION: &str = include_str!("VERSION");

    pub const GRPC_CONNECTION_KEEPALIVE: Duration = Duration::from_secs(2 * 60 * 60);

    pub const DEFAULT_TRANSACTION_TIMEOUT_MILLIS: u64 = Duration::from_secs(5 * 60).as_millis() as u64;
    pub const DEFAULT_PREFETCH_SIZE: u64 = 32;
    pub const DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS: u64 = Duration::from_secs(10).as_millis() as u64;
    pub const DEFAULT_TRANSACTION_PARALLEL: bool = true;

    pub const PERF_COUNTERS_ENABLED: bool = true;

    // TODO: Move these fields to config
    pub const MONITORING_PORT: u16 = 4104;
    pub const MONITORING_ENABLED: bool = true;
    pub const REPORTING_ENABLED: bool = false;

    pub const SERVER_ID_FILE_NAME: &str = "_server_id";
    pub const SERVER_ID_LENGTH: u64 = 16;
    pub const SERVER_ID_ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    pub const AUTHENTICATOR_USERNAME_FIELD: &str = "username";
    pub const AUTHENTICATOR_PASSWORD_FIELD: &str = "password";
    pub const AUTHENTICATOR_CACHE_TTL: Duration = Duration::from_secs(3 * 60 * 60);
    pub const AUTHENTICATOR_CACHE_TTI: Duration = Duration::from_secs(60 * 60);

    pub const DEFAULT_ADDRESS: &str = "0.0.0.0:1729";
    pub const DEFAULT_USER_NAME: &str = "admin";
    pub const DEFAULT_USER_PASSWORD: &str = "password";

    pub const SYSTEM_FILE_PREFIX: &str = "_";
}

pub mod database {
    pub const QUERY_PLAN_CACHE_FLUSH_STATISTICS_CHANGE_PERCENT: f64 = 0.05;
    pub const QUERY_PLAN_CACHE_SIZE: u64 = 100;
    pub const STATISTICS_DURABLE_WRITE_CHANGE_PERCENT: f64 = 0.05;

    #[macro_export]
    macro_rules! internal_database_prefix {
        () => {
            "_"
        };
    }
    pub const INTERNAL_DATABASE_PREFIX: &str = internal_database_prefix!();
}

pub mod concept {
    // TODO: this should be parametrised into the database options? Would be great to have it be changable at runtime!
    pub const RELATION_INDEX_THRESHOLD: u64 = 5;
}

pub mod traversal {
    pub const CONSTANT_CONCEPT_LIMIT: usize = 1000;
}

pub mod snapshot {
    pub const BUFFER_KEY_INLINE: usize = 40;
    pub const BUFFER_VALUE_INLINE: usize = 64;
}

pub mod storage {
    pub const TIMELINE_WINDOW_SIZE: usize = 100;
    pub const WAL_SYNC_INTERVAL_MICROSECONDS: u64 = 1000;
    pub const WATERMARK_WAIT_INTERVAL_MICROSECONDS: u64 = 50;
    pub const COMMIT_WAIT_FOR_FSYNC: bool = true;

    pub const ROCKSDB_CACHE_SIZE_MB: u64 = 1024;
}

pub mod encoding {
    use std::sync::atomic::AtomicU16;

    pub const LABEL_NAME_STRING_INLINE: usize = 32;
    pub const LABEL_SCOPE_STRING_INLINE: usize = 32;
    pub const LABEL_SCOPED_NAME_STRING_INLINE: usize = LABEL_NAME_STRING_INLINE + LABEL_SCOPE_STRING_INLINE;
    pub const DEFINITION_NAME_STRING_INLINE: usize = 64;
    pub const AD_HOC_BYTES_INLINE: usize = 128;

    pub type DefinitionIDUInt = u16;
    pub type DefinitionIDAtomicUInt = AtomicU16;

    pub type StructFieldIDUInt = u16;
}

pub mod diagnostics {
    use std::time::Duration;

    pub const UNKNOWN_STR: &'static str = "Unknown";

    // pub const DATABASE_METRICS_UPDATE_INTERVAL: Duration = Duration::from_secs(600);
    // TODO: Return the value above
    pub const DATABASE_METRICS_UPDATE_INTERVAL: Duration = Duration::from_secs(30);

    pub const REPORTING_URI: &str = "https://diagnostics.typedb.com/";
    pub const REPORT_INTERVAL: Duration = Duration::from_secs(3600);
    pub const DISABLED_REPORTING_FILE_NAME: &str = "_reporting_disabled";
}
