# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")
package(default_visibility = ["//visibility:public",])

rust_test(
    name = "test_type_vertex",
    srcs = glob([
        "test_type_vertex.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//encoding",
        "//storage",
        "//durability",
        "//util/test:test_utils",

        "@crates//:rand",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test_attribute_vertex",
    srcs = glob([
        "test_attribute_vertex.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//encoding",
        "//storage",
        "//durability",
        "//resource",
        "//util/test:test_utils",

        "@crates//:rand",
        "@crates//:tracing",
        "@crates//:seahash",
    ]
)

rust_test(
    name = "test_definitions",
    srcs = glob([
        "test_definitions.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//encoding",
        "//storage",
        "//durability",
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
