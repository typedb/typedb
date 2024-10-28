# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "executor",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
        "*/*.rs",
        "*/*/*.rs",
        "*/*/*/*.rs",
    ], exclude=["tests/**"]),
    deps = [
        "//answer",
        "//common/error",
        "//common/lending_iterator",
        "//common/logger",
        "//common/macros",
        "//compiler",
        "//concept",
        "//encoding",
        "//ir",
        "//resource",
        "//storage",

        "@vaticle_typeql//rust:typeql",

        "@crates//:itertools",
        "@crates//:tracing",
        "@crates//:tokio",
    ],
)

rust_test(
    name = "test_crate_executor",
    crate = ":executor",
    deps = [],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "**/*.rs"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
