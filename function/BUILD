# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "function",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
    ]),
    deps = [
        "//answer",
        "//common/bytes",
        "//common/primitive",
        "//resource",
        "//concept",
        "//compiler",
        "//durability",
        "//encoding",
        "//ir",
        "//storage",

        "@vaticle_typeql//rust:typeql",

        "@crates//:itertools",
    ],
)

rust_test(
    name = "test_crate_function",
    crate = ":function",
    deps = ["//util/test:test_utils"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
