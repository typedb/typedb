# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "ir",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
        "expressions/*.rs",
        "expressions/builtins/*.rs",
        "optimisation/*.rs",
        "pattern/*.rs",
        "pipeline/*.rs",
        "translation/*.rs",
    ]),
    deps = [
        "//answer",
        "//common/bytes",
        "//common/primitive",
        "//common/structural_equality",
        "//common/error",
        "//concept",
        "//durability",
        "//encoding",
        "//storage",

        "@typeql//rust:typeql",

        "@crates//:itertools",

        # For the parsing of literals
        "@crates//:chrono",
        "@crates//:chrono-tz",
        "@crates//:regex",

        "//util/test:test_utils", # dev dependency
    ],
)

rust_test(
    name = "test_crate_ir",
    crate = ":ir",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*.rs", "expressions/builtins/*.rs"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
