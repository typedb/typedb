# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")
package(default_visibility = ["//visibility:public",])

rust_library(
    name = "test_utils_database",
    srcs = ["test_utils_database.rs"],
    deps = [
        "//common/options",
        "//database",
        "//diagnostics",
        "//storage",
    ]
)

rust_test(
    name = "test_database",
    srcs = glob([
        "database.rs",
    ]),
    deps = [
        "//common/options",
        "//database",
        "//diagnostics",
        "//durability",
        "//encoding",
        "//storage",
        "//storage/tests:test_utils_storage",

        ":test_utils_database",
        "//util/test:test_utils",

        "@crates//:rocksdb",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test_import",
    srcs = glob([
        "import.rs",
    ]),
    deps = [
        "//database",

        ":test_utils_database",
        "//util/test:test_utils",

        "@crates//:tracing",
    ]
)

rust_test(
    name = "test_migration",
    srcs = glob([
        "migration.rs",
    ]),
    deps = [
        "//common/concurrency",
        "//common/options",
        "//concept",
        "//database",
        "//encoding",
        "//resource",
        "//storage",

        ":test_utils_database",
        "//util/test:test_utils",

        "@crates//:tokio",
    ]
)

rust_test(
    name = "test_vector_durability",
    srcs = glob([
        "vector_durability.rs",
    ]),
    deps = [
        "//common/options",
        "//concept",
        "//database",
        "//diagnostics",
        "//encoding",
        "//executor",
        "//query",
        "//resource",
        "//storage",
        "//storage/tests:test_utils_storage",
        "//util/test:test_utils",

        "@typeql//rust:typeql",
    ]
)

[
    rust_test(
        name = "test_vector_isolation_" + family,
        crate_root = "vector_isolation/" + family + ".rs",
        srcs = [
            "vector_isolation/common.rs",
            "vector_isolation/" + family + ".rs",
        ],
        deps = [
            "//common/options",
            "//concept",
            "//database",
            "//encoding",
            "//executor",
            "//function",
            "//query",
            "//resource",
            "//storage",

            ":test_utils_database",
            "//util/test:test_utils",

            "@typeql//rust:typeql",
        ]
    )
    for family in [
        "one_writer_multiple_readers",
        "two_writers",
        "parallel",
    ]
]

rust_test(
    name = "test_transaction",
    srcs = glob([
        "transaction.rs",
    ]),
    deps = [
        "//common/options",
        "//database",
        "//diagnostics",
        "//encoding",
        "//storage",

        ":test_utils_database",
        "//util/test:test_utils",

        "@crates//:rocksdb",
        "@crates//:tracing",
        "@crates//:tokio",
    ]
)

rust_test(
    name = "test_statistics_backwards_compatibility",
    srcs = glob([
        "statistics_backwards_compatibility.rs",
    ]),
    deps = [
        "//common/options",
        "//concept",
        "//database",
        "//diagnostics",
        "//durability",
        "//executor",
        "//query",
        "//storage",

        ":test_utils_database",
        "//util/test:test_utils",

        "@typeql//rust:typeql",
    ]
)

rust_test(
    name = "test_statistics_synchronization",
    srcs = glob([
        "statistics_synchronization.rs",
    ]),
    deps = [
        "//common/options",
        "//concept",
        "//database",
        "//executor",
        "//query",
        "//resource",
        "//storage",

        ":test_utils_database",
        "//util/test:test_utils",

        "@typeql//rust:typeql",
    ]
)

rust_test(
    name = "test_cleanup_records",
    srcs = glob([
        "cleanup_records.rs",
    ]),
    deps = [
        "//common/options",
        "//database",
        "//diagnostics",
        "//durability",
        "//executor",
        "//query",
        "//storage",

        ":test_utils_database",
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
        "//encoding",
        "//executor",
        "//query",
        "//storage",

        ":test_utils_database",
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
        ":test_import",
        ":test_migration",
        ":test_vector_durability",
        ":test_vector_isolation_one_writer_multiple_readers",
        ":test_vector_isolation_two_writers",
        ":test_vector_isolation_parallel",
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
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
