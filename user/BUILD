# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library")
package(default_visibility = ["//visibility:public"])

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "user",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
    ]),
    deps = [
        "//answer",
        "//common/lending_iterator",
        "//common/error",
        "//database",
        "//executor",
        "//query",
        "//storage",
        "//system",
        "@vaticle_typeql//rust:typeql",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
