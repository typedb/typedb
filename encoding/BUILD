# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_library")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
package(default_visibility = [
    ":__subpackages__",
    "//concept:__subpackages__",
    "//database:__subpackages__",
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

        "@crates//:chrono",
        "@crates//:tracing",
        "@crates//:seahash",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "graph/*",
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
