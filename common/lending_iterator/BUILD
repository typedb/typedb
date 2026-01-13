# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "lending_iterator",
    crate_root = "lending_iterator.rs",
    srcs = glob([
        "*.rs",
    ])
)

rust_test(
    name = "test_crate_lending_iterator",
    crate = ":lending_iterator",
    deps = [],
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":lending_iterator",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)


