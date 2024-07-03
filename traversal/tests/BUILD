# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test")
package(default_visibility = ["//visibility:public",])

rust_test(
    name = "executor",
    crate_root = "executor.rs",
    srcs =  ["executor.rs"],
    deps = [
        "//answer",
        "//concept",
        "//common/bytes",
        "//common/lending_iterator",
        "//durability",
        "//encoding",
        "//ir:ir",
        "//traversal",
        "//storage",

        "//util/test:test_utils",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
