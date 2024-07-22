# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//tests/behaviour:__subpackages__"])

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library")

rust_library(
    name = "steps",
    srcs = glob(["**/*.rs"]),
    crate_root = "lib.rs",
    deps = [
        "//common/lending_iterator",
        "//concept:concept",
        "//database:database",
        "//encoding:encoding",
        "//server:server",
        "//storage:storage",
        "//durability:durability",
        "//util/test:test_utils",

        "@crates//:chrono",
        "@crates//:chrono-tz",
        "@crates//:cucumber",
        "@crates//:futures",
        "@crates//:itertools",
        "@crates//:macro_rules_attribute",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "concept/*",
        "concept/thing/*",
        "concept/type_/*",
        "connection/*",
    ]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
