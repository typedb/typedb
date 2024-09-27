# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_test")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

rust_test(
    name = "test_database",
    srcs = ["database.rs"],
    deps = [
        "//tests/behaviour/steps:steps",

        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//connection:database.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_transaction",
    srcs = ["transaction.rs"],
    deps = [
        "//tests/behaviour/steps:steps",

        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//connection:transaction.feature"],
    crate_features = ["bazel"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
