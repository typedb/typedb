# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test")
package(default_visibility = ["//visibility:public",])

executor_deps = [
    "//answer",
    "//concept",
    "//common/bytes",
    "//common/lending_iterator",
    "//durability",
    "//encoding",
    "//ir:ir",
    "//traversal",
    "//storage",
]

rust_test(
    name = "executor",
    crate_root = "executor.rs",
    srcs = ["executor.rs", "common.rs"],
    deps = executor_deps + [
        "//util/test:test_utils",
        "@vaticle_typeql//rust:typeql",
    ],
)

rust_test(
    name = "executor_has",
    crate_root = "executor_has.rs",
    srcs = ["executor_has.rs", "common.rs"],
    deps = executor_deps + [
        "//util/test:test_utils",
        "@vaticle_typeql//rust:typeql",
    ],
)

rust_test(
    name = "executor_rp",
    crate_root = "executor_rp.rs",
    srcs = ["executor_rp.rs", "common.rs"],
    deps = executor_deps + [
        "//util/test:test_utils",
        "@vaticle_typeql//rust:typeql",
    ],
)

rust_test(
    name = "select",
    crate_root = "select.rs",
    srcs = ["select.rs", "common.rs"],
    deps = executor_deps + [
        "//util/test:test_utils",
        "@vaticle_typeql//rust:typeql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
