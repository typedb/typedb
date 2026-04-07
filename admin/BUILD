# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_library")
load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

package(default_visibility = ["//visibility:public"])

rust_library(
    name = "admin",
    crate_name = "typedb_admin",
    srcs = glob(["src/**/*.rs"], exclude = ["src/main.rs"]),
    crate_root = "src/lib.rs",
    tags = [
        "crate-name=typedb-admin",
    ],
    deps = [
        "//server/service/admin/proto:server_admin_proto",
        "@crates//:clap",
        "@crates//:tokio",
        "@crates//:tonic",
    ],
)

rust_binary(
    name = "typedb_admin",
    srcs = ["src/main.rs"],
    deps = [
        ":admin",
        "@crates//:clap",
        "@crates//:tokio",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["**/*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
