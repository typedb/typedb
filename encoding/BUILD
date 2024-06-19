# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
package(default_visibility = [
    ":__subpackages__",
    "//answer:__subpackages__",
    "//concept:__subpackages__",
    "//database:__subpackages__",
    "//function:__subpackages__",
    "//ir:__subpackages__",
    "//tests/behaviour:__subpackages__",
])

rust_library(
    name = "encoding",
    crate_root = "encoding.rs",
    srcs = glob([
        "*.rs",
        "*/*.rs",
        "*/*/*.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/lending_iterator",
        "//common/logger",
        "//common/primitive",
        "//durability",
        "//resource",
        "//storage",

        "@crates//:bincode",
        "@crates//:chrono",
        "@crates//:chrono-tz",
        "@crates//:tracing",
        "@crates//:seahash",
        "@crates//:serde",
    ],
)

rust_test(
    name = "encoding_unit_tests",
    crate = ":encoding",
    deps = [
        "@crates//:rand",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "graph/*",
        "graph/definition/*",
        "graph/thing/*",
        "graph/type_/*",
        "layout/*",
        "value/*",
    ]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
