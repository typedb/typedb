# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rustfmt_test")

upgrade_platforms = {
    "linux_x86_64": ("linux-x86_64", "tar.gz"),
    "linux_arm64": ("linux-arm64", "tar.gz"),
    "mac_x86_64": ("mac-x86_64", "zip"),
    "mac_arm64": ("mac-arm64", "zip"),
    "windows_x86_64": ("windows-x86_64", "zip"),
}

rust_test(
    name = "to_vector_storage",
    srcs = ["to_vector_storage.rs"],
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

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":to_vector_storage",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
