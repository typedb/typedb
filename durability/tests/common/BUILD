# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
package(default_visibility = ["//visibility:public",])


rust_library(
    name = "durability_test_common",
    crate_root = "test_common.rs",
    srcs = ["test_common.rs"],
    deps = [
        "//durability:durability",
        "@crates//:bincode",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob(["Cargo.*"]),
    license_type = "mpl-header",
)
