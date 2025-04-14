/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod common {
    pub const SECONDS_IN_MINUTE: u64 = 60;
    pub const MINUTES_IN_HOUR: u64 = 60;
    pub const HOURS_IN_DAY: u64 = 24;
    pub const DAYS_IN_MONTH: u64 = 30; // Approximate
    pub const DAYS_IN_YEAR: u64 = 365;

    pub const SECONDS_IN_HOUR: u64 = SECONDS_IN_MINUTE * MINUTES_IN_HOUR;
    pub const SECONDS_IN_DAY: u64 = SECONDS_IN_HOUR * HOURS_IN_DAY;
    pub const SECONDS_IN_MONTH: u64 = SECONDS_IN_DAY * DAYS_IN_MONTH;
    pub const SECONDS_IN_YEAR: u64 = SECONDS_IN_DAY * DAYS_IN_YEAR;

    pub const ERROR_QUERY_POINTER_LINES_BEFORE: usize = 2;
    pub const ERROR_QUERY_POINTER_LINES_AFTER: usize = 2;
}

pub mod server {
    use std::time::Duration;

    use crate::constants::common::{SECONDS_IN_HOUR, SECONDS_IN_MINUTE, SECONDS_IN_YEAR};

    pub const DISTRIBUTION: &str = "TypeDB CE";
    pub const VERSION: &str = include_str!("../VERSION");
    pub const ASCII_LOGO: &str = include_str!("typedb-ascii.txt");

    pub const GRPC_CONNECTION_KEEPALIVE: Duration = Duration::from_secs(2 * SECONDS_IN_HOUR);

    // TODO: Maybe we start moving these options to separate crates?
    pub const DEFAULT_PREFETCH_SIZE: u64 = 32;
    pub const DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS: u64 = Duration::from_secs(10).as_millis() as u64;
    pub const DEFAULT_TRANSACTION_TIMEOUT_MILLIS: u64 = Duration::from_secs(5 * SECONDS_IN_MINUTE).as_millis() as u64;
    pub const DEFAULT_TRANSACTION_PARALLEL: bool = true;
    pub const DEFAULT_INCLUDE_INSTANCE_TYPES: bool = true;
    pub const DEFAULT_INCLUDE_INSTANCE_TYPES_FETCH: bool = false;
    pub const DEFAULT_ANSWER_COUNT_LIMIT_GRPC: Option<usize> = None;
    pub const DEFAULT_ANSWER_COUNT_LIMIT_HTTP: Option<usize> = Some(10_000);

    pub const PERF_COUNTERS_ENABLED: bool = true;

    pub const MONITORING_DEFAULT_PORT: u16 = 4104;

    pub const SERVER_ID_FILE_NAME: &str = "_server_id";
    pub const SERVER_ID_LENGTH: u64 = 16;
    pub const SERVER_ID_ALPHABET: [char; 36] = [
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
        'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    ];

    pub const MIN_AUTHENTICATION_TOKEN_TTL_SECONDS: u64 = 1;
    pub const MAX_AUTHENTICATION_TOKEN_TTL_SECONDS: u64 = 1 * SECONDS_IN_YEAR;
    pub const DEFAULT_AUTHENTICATION_TOKEN_TTL_SECONDS: u64 = 4 * SECONDS_IN_HOUR;
    pub const MIN_AUTHENTICATION_TOKEN_TTL: Duration = Duration::from_secs(MIN_AUTHENTICATION_TOKEN_TTL_SECONDS);
    pub const MAX_AUTHENTICATION_TOKEN_TTL: Duration = Duration::from_secs(MAX_AUTHENTICATION_TOKEN_TTL_SECONDS);
    pub const DEFAULT_AUTHENTICATION_TOKEN_TTL: Duration =
        Duration::from_secs(DEFAULT_AUTHENTICATION_TOKEN_TTL_SECONDS);

    pub const DATABASE_METRICS_UPDATE_INTERVAL: Duration = Duration::from_secs(10 * SECONDS_IN_MINUTE);

    pub const DEFAULT_ADDRESS: &str = "0.0.0.0:1729";
    pub const DEFAULT_HTTP_ADDRESS: &str = "0.0.0.0:8000";
    pub const DEFAULT_USER_NAME: &str = "admin";
    pub const DEFAULT_USER_PASSWORD: &str = "password";
    pub const DEFAULT_DATA_DIR: &str = "data";

    pub const SYSTEM_FILE_PREFIX: &str = "_";

    pub const SENTRY_REPORTING_URI: &str =
        "https://3d710295c75c81492e57e1997d9e01e1@o4506315929812992.ingest.sentry.io/4506316048629760";
}

pub mod database {
    use std::time::Duration;

    // anything lower than 2.0 will cause too much replanning
    // anything over 8.0 often does not plan frequently enough, as the data scales
    pub const QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION: f64 = 5.0;
    pub const QUERY_PLAN_CACHE_SIZE: u64 = 100;
    pub const STATISTICS_DURABLE_WRITE_CHANGE_PERCENT: f64 = 0.04;

    pub const STATISTICS_UPDATE_INTERVAL: Duration = Duration::from_millis(50);
    pub const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);

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
    pub const FIXED_BATCH_ROWS_MAX: u32 = 64;
    pub const BATCH_DEFAULT_CAPACITY: usize = 10;
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

    use crate::constants::common::SECONDS_IN_HOUR;

    pub const UNKNOWN_STR: &str = "Unknown";

    pub const POSTHOG_BATCH_REPORTING_URI: &str = "https://us.i.posthog.com/batch/";
    // The key is write-only and safe to expose
    pub const POSTHOG_API_KEY: &str = "phc_pYoyROZCtNDL8obeJfLZ8cP0UKzIAxmd0JcQQ03i07T";

    pub const REPORT_INTERVAL: Duration = Duration::from_secs(1 * SECONDS_IN_HOUR);
    pub const REPORT_ONCE_DELAY: Duration = Duration::from_secs(1 * SECONDS_IN_HOUR);

    pub const REPORT_INITIAL_RETRY_DELAY: Duration = Duration::from_millis(500);
    pub const REPORT_RETRY_DELAY_EXPONENTIAL_MULTIPLIER: u32 = 2;
    pub const REPORT_MAX_RETRY_NUM: u32 = 3;

    pub const DISABLED_REPORTING_FILE_NAME: &str = "_reporting_disabled";
}
