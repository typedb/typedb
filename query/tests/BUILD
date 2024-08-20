# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test")
package(default_visibility = ["//visibility:public",])

deps = [
    "//answer",
    "//concept",
    "//common/bytes",
    "//common/lending_iterator",
    "//compiler",
    "//durability",
    "//encoding",
    "//executor",
    "//ir",
    "//query:query",
    "//storage",
    "@vaticle_typeql//rust:typeql",
    "//util/test:test_utils",
]

rust_test(
    name = "test_define",
    crate_root = "define.rs",
    srcs = ["define.rs", "common.rs"],
    deps = deps,
)
#
rust_test(
    name = "test_pipeline",
    crate_root = "pipeline.rs",
    srcs = ["pipeline.rs", "common.rs"],
    deps = deps,
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
