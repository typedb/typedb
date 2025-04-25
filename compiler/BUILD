# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "compiler",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
        "annotation/**/*.rs",
        "transformation/**/*.rs",
        "executable/**/*.rs",
    ]),
    deps = [
        "//common/error",
        "//answer",
        "//concept",
        "//encoding",
        "//ir",
        "//resource",
        "//storage",

        "@typeql//rust:typeql",

        "@crates//:itertools",
        "@crates//:tracing",
        "@crates//:chrono",
    ],
)

rust_test(
    name = "test_crate_compiler",
    crate = ":compiler",
    deps = [
        "//durability",
        "//util/test:test_utils",
        "//resource",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "annotation/**/*.rs",
        "executable/**/*.rs",
        "transformation/**/*.rs",
    ]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
