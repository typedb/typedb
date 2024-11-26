# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "test_attribute",
    srcs = ["attribute.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//concept/thing:attribute.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_entity",
    srcs = ["entity.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//concept/thing:entity.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_has",
    srcs = ["has.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//concept/thing:has.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_relation",
    srcs = ["relation.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//concept/thing:relation.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_roleplayer",
    srcs = ["roleplayer.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//concept/thing:roleplayer.feature"],
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
