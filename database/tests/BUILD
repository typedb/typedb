# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rustfmt_test")
package(default_visibility = ["//visibility:public",])

rust_test(
    name = "test_database",
    srcs = glob([
        "database.rs",
    ]),
    deps = [
        "//common/logger",
        "//common/options",
        "//database",
        "//diagnostics",
        "//durability",
        "//encoding",
        "//storage",
        "//storage/tests:test_utils_storage",
        "//util/test:test_utils",

        "@crates//:rocksdb",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test_transaction",
    srcs = glob([
        "transaction.rs",
    ]),
    deps = [
        "//common/logger",
        "//common/options",
        "//database",
        "//diagnostics",
        "//encoding",
        "//storage",
        "//util/test:test_utils",

        "@crates//:rocksdb",
        "@crates//:tracing",
        "@crates//:tokio",
    ]
)

rust_test(
    name = "test_statistics_synchronization",
    srcs = glob([
        "statistics_synchronization.rs",
    ]),
    deps = [
        "//common/options",
        "//database",
        "//diagnostics",
        "//executor",
        "//query",
        "//storage",
        "//util/test:test_utils",

        "@typeql//rust:typeql",
    ]
)

rust_test(
    name = "test_transaction_isolation",
    srcs = glob([
        "test_transaction_isolation.rs",
    ]),
    deps = [
        "//common/options",
        "//database",
        "//diagnostics",
        "//encoding",
        "//executor",
        "//query",
        "//storage",
        "//util/test:test_utils",

        "@typeql//rust:typeql",
    ]
)

rust_test(
    name = "test_diagnostics_posthog_pii",
    srcs = glob([
        "diagnostics_posthog_pii.rs",
    ]),
    deps = [
        "//diagnostics",

        "@crates//:serde_json",
    ]
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":test_database",
        ":test_transaction",
        ":test_statistics_synchronization",
        ":test_transaction_isolation",
        ":test_diagnostics_posthog_pii",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
