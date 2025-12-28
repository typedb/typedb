# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
package(default_visibility = [
    ":__subpackages__",
    "//answer:__subpackages__",
    "//compiler:__subpackages__",
    "//concept:__subpackages__",
    "//database:__subpackages__",
    "//encoding:__subpackages__",
    "//executor:__subpackages__",
    "//function:__subpackages__",
    "//inference:__subpackages__",
    "//ir:__subpackages__",
    "//server:__subpackages__",
    "//storage:__subpackages__",
    "//traversal:__subpackages__",
    "//query:__subpackages__",
    "//tests/behaviour:__subpackages__",
])

rust_library(
    name = "kv",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
        "*/*.rs",
        "*/*/*.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/error",
        "//common/lending_iterator",
        "//common/logger",
        "//common/primitive",
        "//common/structural_equality",
        "//resource",

        "@typeql//rust:typeql", # leaky but enables generic TypeDBError

        "@crates//:itertools",
        "@crates//:tracing",
        "@crates//:rocksdb",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "**/*.rs",
    ]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":kv",
    ],
    size = "small",
)
