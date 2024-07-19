# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "test_function",
    srcs = ["function"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/validation:function.feature"],
)

rust_test(
    name = "test_data_validation",
    srcs = ["data_validation"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/validation:data-validation.feature"],
)

rust_test(
    name = "test_migration",
    srcs = ["migration.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/validation:migration.feature"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
