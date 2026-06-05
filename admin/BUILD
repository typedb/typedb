# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test")
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

rust_test(
    name = "test_admin_assembly",
    srcs = ["tests/admin_assembly.rs"],
    deps = [
        "@crates//:tokio",
    ],
    data = ["//:assemble-typedb-all"],
    env = select({
        "@typedb_bazel_distribution//platform:is_linux_x86_64": {"TYPEDB_ASSEMBLY_ARCHIVE": "typedb-all-linux-x86_64.tar.gz"},
        "@typedb_bazel_distribution//platform:is_linux_arm64": {"TYPEDB_ASSEMBLY_ARCHIVE": "typedb-all-linux-arm64.tar.gz"},
        "@typedb_bazel_distribution//platform:is_mac_x86_64": {"TYPEDB_ASSEMBLY_ARCHIVE": "typedb-all-mac-x86_64.zip"},
        "@typedb_bazel_distribution//platform:is_mac_arm64": {"TYPEDB_ASSEMBLY_ARCHIVE": "typedb-all-mac-arm64.zip"},
        "@typedb_bazel_distribution//platform:is_windows_x86_64": {"TYPEDB_ASSEMBLY_ARCHIVE": "typedb-all-windows-x86_64.zip"},
    }),
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
