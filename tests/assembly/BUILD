# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "test_assembly",
    srcs = ["assembly.rs"],
    deps = [
        "@crates//:tokio",
    ],
    data = ["//:assemble-typedb-server"],
    env = select({
        "@vaticle_bazel_distribution//platform:is_linux_x86_64" : {"TYPEDB_ASSEMBLY_ARCHIVE": "typedb-all-linux-x86_64.tar.gz" },
        "@vaticle_bazel_distribution//platform:is_linux_arm64" : {"TYPEDB_ASSEMBLY_ARCHIVE":"typedb-all-linux-arm64.tar.gz"},
        "@vaticle_bazel_distribution//platform:is_mac_x86_64" : {"TYPEDB_ASSEMBLY_ARCHIVE": "typedb-all-mac-x86_64.zip"},
        "@vaticle_bazel_distribution//platform:is_mac_arm64" : {"TYPEDB_ASSEMBLY_ARCHIVE":"typedb-all-mac-arm64.zip"},
        # "@vaticle_bazel_distribution//platform:is_windows_x86_64" : ":assemble-windows-x86_64-zip"},
    }),
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
