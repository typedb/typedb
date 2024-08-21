# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "test_attributetype",
    srcs = ["attribute_type.rs"],
    timeout = "long",
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/type:attributetype.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_entitytype",
    srcs = ["entity_type.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/type:entitytype.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_relationtype",
    srcs = ["relation_type.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/type:relationtype.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_plays",
    srcs = ["plays.rs"],
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/type:plays.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_owns",
    srcs = ["owns.rs"],
    timeout = "long",
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/type:owns.feature"],
)

rust_test(
    name = "test_owns_annotations",
    srcs = ["owns_annotations.rs"],
    timeout = "long",
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = ["@vaticle_typedb_behaviour//concept/type:owns-annotations.feature"],
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
