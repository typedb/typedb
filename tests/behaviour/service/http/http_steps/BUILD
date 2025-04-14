# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//tests/behaviour:__subpackages__"])

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library")

rust_library(
    name = "http_steps",
    srcs = glob(["**/*.rs"]),
    crate_root = "lib.rs",
    deps = [
        "//server:server",
        "//encoding:encoding",
        "//common/error:error",
        "//common/options:options",
        "//tests/behaviour/steps/params:params",
        "//util/test:test_utils",

        "@crates//:async-std",
        "@crates//:chrono",
        "@crates//:cucumber",
        "@crates//:futures",
        "@crates//:hyper",
        "@crates//:hyper-rustls",
        "@crates//:itertools",
        "@crates//:regex",
        "@crates//:serde",
        "@crates//:serde_json",
        "@crates//:smol",
        "@crates//:tokio",
        "@crates//:uuid",
        "@crates//:url",
        "@crates//:macro_rules_attribute",

        "@typeql//rust:typeql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "**/*"]),
    exclude = glob(["**/Cargo.*"]),
    license_type = "mpl-header",
)
