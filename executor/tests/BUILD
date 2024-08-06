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
    "//storage",
    "@vaticle_typeql//rust:typeql",
    "//util/test:test_utils",


]

rust_test(
    name = "test_compile_execute",
    crate_root = "compile_execute.rs",
    srcs = ["compile_execute.rs", "common.rs"],
    deps = deps + [
        "@crates//:itertools",
    ],
)

rust_test(
    name = "test_execute_has",
    crate_root = "execute_has.rs",
    srcs = ["execute_has.rs", "common.rs"],
    deps = deps,
)

rust_test(
    name = "test_execute_links",
    crate_root = "execute_links.rs",
    srcs = ["execute_links.rs", "common.rs"],
    deps = deps,
)

rust_test(
    name = "test_select",
    crate_root = "execute_select.rs",
    srcs = ["execute_select.rs", "common.rs"],
    deps = deps,
)

rust_test(
    name = "test_insert",
    crate_root = "insert.rs",
    srcs = ["insert.rs", "common.rs"],
    deps = deps + [
        "//query:query",
    ],
)

rust_test(
    name = "test_execute_expression",
    crate_root = "execute_expression.rs",
    srcs = ["execute_expression.rs"],
    deps = deps + [
        "@crates//:itertools"
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
