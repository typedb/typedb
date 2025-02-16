# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
package(default_visibility = [
    ":__subpackages__",
    "//answer:__subpackages__",
    "//executor:__subpackages__",
    "//function:__subpackages__",
    "//database:__subpackages__",
    "//ir:__subpackages__",
    "//compiler:__subpackages__",
    "//traversal:__subpackages__",
    "//query:__subpackages__",
    "//server:__subpackages__",
    "//system:__subpackages__",
    "//tests/behaviour:__subpackages__",
])

rust_library(
    name = "concept",
    srcs = glob([
        "*.rs",
        "type_/**/*.rs",
        "thing/**/*.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/error",
        "//common/iterator",
        "//common/lending_iterator",
        "//common/primitive",
        "//durability",
        "//encoding",
        "//resource",
        "//storage",

        "@typeql//rust:typeql",

        "@crates//:bincode",
        "@crates//:chrono",
        "@crates//:chrono-tz",
        "@crates//:itertools",
        "@crates//:regex",
        "@crates//:serde",
        "@crates//:tracing",
    ],
    proc_macro_deps = [
        "@crates//:paste",
    ],
)

rust_test(
    name = "test_crate_concept",
    crate = ":concept",
    deps = [],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "thing/*",
        "thing/thing_manager/**/*",
        "type_/*",
        "type_/type_manager/**/*",
    ]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
