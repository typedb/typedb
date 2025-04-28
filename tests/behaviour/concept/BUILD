# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "test_concept",
    crate_root = "main.rs",
    srcs = glob(["main.rs", "migration/*.rs", "thing/*.rs", "type/*.rs"]),
    timeout = "long",
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = [
        "@typedb_behaviour//concept/migration:data-validation.feature",
        "@typedb_behaviour//concept/migration:migration.feature",

        "@typedb_behaviour//concept/thing:attribute.feature",
        "@typedb_behaviour//concept/thing:entity.feature",
        "@typedb_behaviour//concept/thing:has.feature",
        "@typedb_behaviour//concept/thing:links.feature",
        "@typedb_behaviour//concept/thing:relation.feature",

        "@typedb_behaviour//concept/type:attributetype.feature",
        "@typedb_behaviour//concept/type:entitytype.feature",
        "@typedb_behaviour//concept/type:relationtype.feature",
        "@typedb_behaviour//concept/type:plays.feature",
        "@typedb_behaviour//concept/type:owns.feature",
        "@typedb_behaviour//concept/type:owns-annotations.feature",
    ],
    crate_features = ["bazel"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*.rs"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
