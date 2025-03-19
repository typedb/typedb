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
    "//common/error",
    "//common/lending_iterator",
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
    crate_root = "define.rs",
    srcs = ["define.rs"],
    deps = deps,
)

rust_test(
    name = "test_fetch",
    crate_root = "fetch.rs",
    srcs = ["fetch.rs"],
    deps = deps,
)

rust_test(
    name = "test_unimplemented",
    crate_root = "unimplemented.rs",
    srcs = ["unimplemented.rs"],
    deps = deps,
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
