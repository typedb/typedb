# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rustfmt_test")
package(default_visibility = ["//visibility:public",])

rust_test(
    name = "test_encryption",
    srcs = ["encryption.rs"],
    data = ["//server:config.yml"],
    deps = [
        "//resource",
        "//server",
        "//util/test:test_utils",

        "@crates//:tokio",
        "@crates//:tokio-rustls",
    ]
)

rust_test(
    name = "test_migration",
    srcs = ["migration.rs"],
    data = glob(["fixtures/*"]),
    deps = [
        "//common/concurrency",
        "//common/options",
        "//concept",
        "//database",
        "//diagnostics",
        "//resource",
        "//server",
        "//storage",
        "//util/test:test_utils",

        "@crates//:prost",
        "@crates//:tokio",
        "@typedb_protocol//grpc/rust:typedb_protocol",
    ],
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":test_encryption",
        ":test_migration",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
