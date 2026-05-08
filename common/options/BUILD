# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")

package(default_visibility = ["//visibility:public"])

rust_library(
    name = "options",
    crate_root = "options.rs",
    srcs = glob([
        "*.rs",
    ]),
    deps = [
        "//resource",
        "@crates//:serde",
    ]
)

rust_test(
    name = "test_crate_options",
    crate = ":options",
    deps = [
        "@crates//:serde_yaml2",
    ],
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":options",
        ":test_crate_options",
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
