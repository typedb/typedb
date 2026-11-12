# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rustfmt_test")

env = select({
    "@typedb_bazel_distribution//platform:is_linux_x86_64" : {"TYPEDB_ASSEMBLY_ARCHIVE": "typedb-all-linux-x86_64.tar.gz" },
    "@typedb_bazel_distribution//platform:is_linux_arm64" : {"TYPEDB_ASSEMBLY_ARCHIVE":"typedb-all-linux-arm64.tar.gz"},
    "@typedb_bazel_distribution//platform:is_mac_x86_64" : {"TYPEDB_ASSEMBLY_ARCHIVE": "typedb-all-mac-x86_64.zip"},
    "@typedb_bazel_distribution//platform:is_mac_arm64" : {"TYPEDB_ASSEMBLY_ARCHIVE":"typedb-all-mac-arm64.zip"},
    "@typedb_bazel_distribution//platform:is_windows_x86_64" :  {"TYPEDB_ASSEMBLY_ARCHIVE":"typedb-all-windows-x86_64.zip"}
})

rust_test(
    name = "test_assembly",
    srcs = ["assembly.rs"],
    deps = [
        "@crates//:tokio",
    ],
    data = ["//:assemble-typedb-all", ":script.tql"],
    env = env,
)

upgrade_platforms = {
    "linux_x86_64": ("linux-x86_64", "tar.gz"),
    "linux_arm64": ("linux-arm64", "tar.gz"),
    "mac_x86_64": ("mac-x86_64", "zip"),
    "mac_arm64": ("mac-arm64", "zip"),
    "windows_x86_64": ("windows-x86_64", "zip"),
}

rust_test(
    name = "test_upgrade",
    srcs = ["upgrade.rs"],
    data = ["//:assemble-typedb-all"] + select({
        "@typedb_bazel_distribution//platform:is_" + key : ["@typedb_previous_artifact_{}//file".format(platform)]
        for key, (platform, _) in upgrade_platforms.items()
    }),
    env = select({
        "@typedb_bazel_distribution//platform:is_" + key : {
            "TYPEDB_ASSEMBLY_ARCHIVE": "typedb-all-{}.{}".format(platform, ext),
            "TYPEDB_PREVIOUS_ARCHIVE": "$(rootpath @typedb_previous_artifact_{}//file)".format(platform),
        }
        for key, (platform, ext) in upgrade_platforms.items()
    }),
    timeout = "long",
)

rust_test(
    name = "test_fail_points",
    srcs = ["fail_points.rs"],
    deps = [
        "//common/fail_point",
        "//resource",

        "@crates//:tokio",
    ],
    data = ["//:assemble-typedb-all", ":script.tql"],
    env = env,
    timeout = "eternal",
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":test_assembly",
        ":test_upgrade",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
        "script.tql",
    ]),
    license_type = "mpl-header",
)
