# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")
package(default_visibility = ["//visibility:public",])

rust_library(
    name = "lib_benchmark",
    srcs = ["common.rs"],
    crate_root = "common.rs",
    deps = [
        "//common/options",
        "//common/primitive",
        "//compiler",
        "//database",
        "//diagnostics",
        "//executor",
        "//query",
        "//resource",
        "//storage",
        "//util/test:test_utils",

        "@typeql//rust:typeql",

        "@crates//:itertools",
        "@crates//:xoshiro",
    ]
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":lib_benchmark",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    license_type = "mpl-header",
)
