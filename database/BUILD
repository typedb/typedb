# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_library")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

rust_library(
    name = "database",
    srcs = glob([
        "*.rs"
    ]),
    deps = [
        "//common/logger",
        "//concept",
        "//encoding",
        "//function",
        "//storage",
        "//durability",

        "@crates//:itertools",
        "@crates//:rocksdb",
        "@crates//:tracing",
    ],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
