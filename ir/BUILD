# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "ir",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
        "expressions/*.rs",
        "expressions/builtins/*.rs",
        "inference/*.rs",
        "optimisation/*.rs",
        "pattern/*.rs",
        "program/*.rs",
        "translator/*.rs",
    ]),
    deps = [
        "//answer",
        "//concept",
        "//durability",
        "//encoding",
        "//storage",
        "//common/primitive",

        "@vaticle_typeql//rust:typeql",

        "@crates//:itertools",

        # For the parsing of literals
        "@crates//:chrono",
        "@crates//:chrono-tz",

        "//util/test:test_utils", # dev dependency
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*.rs"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
