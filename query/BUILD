# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "query",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
    ]),
    deps = [
        "//answer",
        "//common/structural_equality",
        "//concept",
        "//compiler",
        "//common/lending_iterator",
        "//common/error",
        "//function",
        "//encoding",
        "//executor",
        "//ir",
        "//storage",
        "//resource",

        "@typeql//rust:typeql",

        "@crates//:tokio",
        "@crates//:moka",
    ],
)

rust_test(
    name = "test_crate_query",
    crate = ":query",
    deps = [],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
