# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_binary")
load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

package(default_visibility = ["//visibility:public"])

rust_binary(
    name = "typedb_admin_bin",
    srcs = ["main.rs"],
    deps = [
        "//admin/client",
        "//resource",
        "@crates//:clap",
        "@crates//:tokio",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["**/*"]),
    exclude = glob([
        "Cargo.*",
        "client/**",
    ]),
    license_type = "mpl-header",
)
