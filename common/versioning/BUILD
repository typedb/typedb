# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")

package(default_visibility = ["//visibility:public"])

rust_library(
    name = "versioning",
    crate_root = "versioning.rs",
    srcs = glob([
        "*.rs",
    ]),
    deps = [
        "@crates//:serde",
    ]
)

rust_test(
    name = "test_crate_versioning",
    crate = ":versioning",
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":versioning",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
