# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_library")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
package(default_visibility = [
    ":__subpackages__",
    "//answer:__pkg__",
    "//function:__subpackages__",
    "//database:__pkg__",
    "//ir:__pkg__",
    "//traversal:__subpackages__",
    "//tests/behaviour:__subpackages__",
])

rust_library(
    name = "concept",
    srcs = glob([
        "*.rs",
        "type_/**/*.rs",
        "thing/*.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/iterator",
        "//common/lending_iterator",
        "//common/primitive",
        "//durability",
        "//encoding",
        "//resource",
        "//storage",

        "@crates//:bincode",
        "@crates//:chrono",
        "@crates//:chrono-tz",
        "@crates//:itertools",
        "@crates//:regex",
        "@crates//:serde",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "thing/*",
        "type_/*",
        "type_/type_manager/**/*",
    ]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
