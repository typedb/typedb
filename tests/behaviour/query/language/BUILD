# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "test_expressions",
    srcs = ["expressions.rs"],
    deps = [
        "//tests/behaviour/steps:steps",

        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//query/language:expression.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_define",
    srcs = ["define.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//query/language:define.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_redefine",
    srcs = ["redefine.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//query/language:redefine.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_insert",
    srcs = ["insert.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//query/language:insert.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_match",
    srcs = ["match.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//query/language:match.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_reduce",
    srcs = ["reduce.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//query/language:reduce.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_pipelines",
    srcs = ["pipelines.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//query/language:pipelines.feature"],
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
