# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rustfmt_test")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "resource",
    crate_root = "resource.rs",
    srcs = glob([
        "*.rs",
    ]),
    deps = [
        "@crates//:itertools",
    ],
    compile_data = [
        "typedb-admin-ascii.txt",
        "typedb-ascii.txt",
        "//:VERSION",
    ],
)

filegroup(
    name = "server-logo",
    srcs = ["typedb-ascii.txt"],
)

filegroup(
    name = "admin-logo",
    srcs = ["typedb-admin-ascii.txt"],
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":resource",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
        "typedb-ascii.txt",
        "typedb-admin-ascii.txt",
    ]),
    license_type = "mpl-header",
)
