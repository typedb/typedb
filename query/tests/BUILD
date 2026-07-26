# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test", "rustfmt_test")
package(default_visibility = ["//visibility:public",])

deps = [
    "//answer",
    "//concept",
    "//common/bytes",
    "//common/error",
    "//common/lending_iterator",
    "//common/options:options",
    "//compiler",
    "//durability",
    "//encoding",
    "//executor",
    "//function",
    "//ir",
    "//query:query",
    "//resource",
    "//storage",
    "@typeql//rust:typeql",

    "//concept/tests:test_utils_concept",
    "//encoding/tests:test_utils_encoding",
    "//util/test:test_utils",

    "@crates//:itertools",
]

rust_test(
    name = "test_define",
    crate_root = "test_define.rs",
    srcs = ["test_define.rs"],
    deps = deps,
)

rust_test(
    name = "test_fetch",
    crate_root = "test_fetch.rs",
    srcs = ["test_fetch.rs"],
    deps = deps,
)

rust_test(
    name = "test_unimplemented",
    crate_root = "test_unimplemented.rs",
    srcs = ["test_unimplemented.rs"],
    deps = deps,
)

rust_test(
    name = "test_pipeline_stages_limit",
    crate_root = "pipeline_stages_limit.rs",
    srcs = ["pipeline_stages_limit.rs"],
    deps = deps,
)

rust_test(
    name = "test_query_profile",
    crate_root = "test_query_profile.rs",
    srcs = ["test_query_profile.rs"],
    deps = deps,
)

rust_test(
    name = "test_planner",
    crate_root = "test_planner.rs",
    srcs = ["test_planner.rs"],
    deps = deps,
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":test_define",
        ":test_fetch",
        ":test_unimplemented",
        ":test_pipeline_stages_limit",
        ":test_query_profile",
        ":test_planner",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
