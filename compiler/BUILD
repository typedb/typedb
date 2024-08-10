# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "compiler",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
        "expression/**/*.rs",
        "inference/**/*.rs",
        "instruction/**/*.rs",
        "instruction/expressions/**/*.rs",
        "planner/**/*.rs",
        "optimisation/**/*.rs",
        "write/*.rs",
    ]),
    deps = [
        "//answer",
        "//concept",
        "//encoding",
        "//ir",
        "//storage",

        "@vaticle_typeql//rust:typeql",

        "@crates//:itertools",
        "@crates//:chrono",
    ],
)

rust_test(
    name = "test_crate_compiler",
    crate = ":compiler",
    deps = [
        "//durability",
        "//util/test:test_utils",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "inference/*.rs",
        "instruction/**/*.rs",
        "planner/**/*.rs",
        "expression/**/*.rs",
        "optimisation/**/*.rs"
    ]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
