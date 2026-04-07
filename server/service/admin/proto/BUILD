# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//visibility:public"])

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@typedb_dependencies//builder/proto_grpc/rust:compile.bzl", "rust_tonic_compile")
load("@rules_rust//rust:defs.bzl", "rust_library")

proto_library(
    name = "admin-service-proto",
    srcs = ["admin_service.proto"],
)

rust_tonic_compile(
    name = "admin_proto_src",
    srcs = [":admin-service-proto"],
    packages = ["typedb.admin"],
)

rust_library(
    name = "server_admin_proto",
    srcs = [":admin_proto_src"],
    crate_root = ":admin_proto_src",
    deps = [
        "@crates//:prost",
        "@crates//:tonic",
    ],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    size = "small",
    include = glob(["*"]),
    license_type = "mpl-header",
)
