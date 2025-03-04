# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
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
    "//resource",
    "//storage",

    "@typeql//rust:typeql",

    "//concept/tests:test_utils_concept",
    "//encoding/tests:test_utils_encoding",
    "//util/test:test_utils",
    "@crates//:itertools",
]

rust_test(
    name = "test_compile_execute",
    crate_root = "compile_execute.rs",
    srcs = ["compile_execute.rs"],
    deps = deps + [
        "//function:function",
        "//query:query",
    ],
)

rust_test(
    name = "test_execute_isa",
    crate_root = "execute_isa.rs",
    srcs = ["execute_isa.rs"],
    deps = deps,
)

rust_test(
    name = "test_execute_has",
    crate_root = "execute_has.rs",
    srcs = ["execute_has.rs"],
    deps = deps,
)

rust_test(
    name = "test_execute_links",
    crate_root = "execute_links.rs",
    srcs = ["execute_links.rs"],
    deps = deps,
)

rust_test(
    name = "test_execute_relation_index",
    crate_root = "execute_relation_index.rs",
    srcs = ["execute_relation_index.rs"],
    deps = deps,
)

rust_test(
    name = "test_select",
    crate_root = "execute_select.rs",
    srcs = ["execute_select.rs"],
    deps = deps,
)

rust_test(
    name = "test_writes",
    crate_root = "writes.rs",
    srcs = ["writes.rs"],
    deps = deps + [
        "//query:query",
    ],
)

rust_test(
    name = "test_execute_expression",
    crate_root = "execute_expression.rs",
    srcs = ["execute_expression.rs"],
    deps = deps,
)

rust_test(
    name = "test_pipelines",
    crate_root = "pipelines.rs",
    srcs = ["pipelines.rs"],
    deps = deps + [
        "//function",
        "//query:query",
    ],
)

rust_test(
    name = "test_functions",
    crate_root = "execute_function.rs",
    srcs = ["execute_function.rs"],
    env = {"RUST_MIN_STACK": "40960000"},
    deps = deps + [
        "//function",
        "//query:query",
    ],
)

rust_test(
    name = "test_efficiency",
    crate_root = "efficiency.rs",
    srcs = ["efficiency.rs"],
    deps = deps,
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
