# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")
package(default_visibility = ["//visibility:public",])

rust_test(
    name = "test_definition",
    srcs = glob([
        "test_definition.rs",
    ]),
    deps = [
        "//answer",
        "//common/bytes",
        "//common/logger",
        "//concept",
        "//durability",
        "//encoding",
        "//storage",
        "//ir",
        "//resource",
        "//util/test:test_utils",

        "@crates//:rand",
        "@crates//:tracing",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
