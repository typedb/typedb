# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "attribute",
    srcs = ["attribute.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/thing:attribute.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "entity",
    srcs = ["entity.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/thing:entity.feature"],
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
