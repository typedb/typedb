# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "test_basic",
    srcs = ["basic.rs"],
    deps = [
        "//tests/behaviour/steps:steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//query/functions:basic.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_definition",
    srcs = ["definition.rs"],
    deps = [
        "//tests/behaviour/steps:steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//query/functions:definition.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_recursion",
    srcs = ["recursion.rs"],
    deps = [
        "//tests/behaviour/steps:steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//query/functions:recursion.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_signature",
    srcs = ["signature.rs"],
    deps = [
        "//tests/behaviour/steps:steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//query/functions:signature.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_structure",
    srcs = ["structure.rs"],
    deps = [
        "//tests/behaviour/steps:steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//query/functions:structure.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_usage",
    srcs = ["usage.rs"],
    deps = [
        "//tests/behaviour/steps:steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//query/functions:usage.feature"],
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
