# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")
load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "concept",
    srcs = glob([
        "*.rs",
        "type_/**/*.rs",
        "thing/**/*.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/error",
        "//common/iterator",
        "//common/lending_iterator",
        "//common/primitive",
        "//diagnostics",
        "//durability",
        "//encoding",
        "//resource",
        "//storage",

        "@typeql//rust:typeql",

        "@crates//:bincode",
        "@crates//:chrono",
        "@crates//:chrono-tz",
        "@crates//:itertools",
        "@crates//:macro_rules_attribute",
        "@crates//:regex",
        "@crates//:sentry",
        "@crates//:serde",
        "@crates//:tracing",
    ],
    proc_macro_deps = [
        "@crates//:paste",
    ],
)

rust_test(
    name = "test_crate_concept",
    crate = ":concept",
    deps = [],
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":concept",
        ":test_crate_concept",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "thing/*",
        "thing/statistics/**/*",
        "thing/thing_manager/**/*",
        "type_/*",
        "type_/type_manager/**/*",
    ]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
