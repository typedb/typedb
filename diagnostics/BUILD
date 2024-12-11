# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library")
package(default_visibility = ["//visibility:public",])

rust_library(
    name = "diagnostics",
    srcs = glob([
        "*.rs",
    ]),
    deps = [ # TODO: Revisit and clean up
        "//common/error",
        "//common/concurrency",
        "//common/logger",
        "//resource",

        "@crates//:chrono",
        "@crates//:hyper",
        "@crates//:reqwest",
        "@crates//:serde_json",
        "@crates//:sysinfo",
        "@crates//:tokio",
        "@crates//:tonic",
        "@crates//:tonic-types",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
