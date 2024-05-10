# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")
package(default_visibility = ["//visibility:public",])

rust_test(
    name = "test_storage",
    srcs = glob([
        "test_storage.rs",
        "test_common/*.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//common/primitive",
        "//storage",
        "//storage/durability",
        "//resource",
        "//util/test:test_utils",

        "@crates//:itertools",
        "@crates//:rand",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test_snapshot",
    srcs = glob([
        "test_snapshot.rs",
        "test_common/*.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//common/primitive",
        "//storage",
        "//storage/durability",
        "//resource",
        "//util/test:test_utils",
        "@crates//:rand",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test_recovery",
    srcs = glob([
        "test_recovery.rs",
        "test_common/*.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//common/primitive",
        "//storage",
        "//storage/durability",
        "//resource",
        "//util/test:test_utils",

        "@crates//:itertools",
        "@crates//:rand",
        "@crates//:tracing",
    ]
)


rust_test(
    name = "test_mvcc",
    srcs = glob([
        "test_mvcc.rs",
        "test_common/*.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//common/primitive",
        "//storage",
        "//storage/durability",
        "//resource",
        "//util/test:test_utils",
        "@crates//:rand",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test_isolation",
    srcs = glob([
        "test_isolation.rs",
        "test_common/*.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//common/primitive",
        "//storage",
        "//storage/durability",
        "//resource",
        "//util/test:test_utils",

        "@crates//:rand",
        "@crates//:tracing",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
