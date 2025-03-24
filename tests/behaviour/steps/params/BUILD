# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//tests/behaviour:__subpackages__"])

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library")

rust_library(
    name = "params",
    srcs = glob(["**/*.rs"]),
    crate_root = "lib.rs",
    deps = [
        "//concept:concept",
        "//encoding:encoding",
        "//storage:storage",
        "//util/test:test_utils",

        "@typeql//rust:typeql",

        "@crates//:chrono",
        "@crates//:chrono-tz",
        "@crates//:cucumber",
        "@crates//:itertools",
        "@crates//:regex",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
