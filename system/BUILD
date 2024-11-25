# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "system",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
    ]),
    deps = [
        "//answer",
        "//common/lending_iterator",
        "//common/options",
        "//compiler",
        "//concept",
        "//database",
        "//executor",
        "//function",
        "//query",
        "//storage",
        "@vaticle_typeql//rust:typeql",
        "@crates//:pwhash",
        "@crates//:uuid",
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
