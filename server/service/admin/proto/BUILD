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
    # Bazel compiles the proto via rust_tonic_compile above; cargo builds use the checked-in
    # lib.rs + build.rs (tonic-build) instead. These tags tell the cargo sync tool how to
    # generate this crate's manifest for the cargo build.
    tags = [
        "cargo.entry.point.path=lib.rs",
        "cargo.build.deps.tonic-build=version=^0.12",
    ],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    size = "small",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
