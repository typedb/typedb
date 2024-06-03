# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "typedb_java_test")

typedb_java_test(
    name = "assembly",
    test_class = "com.vaticle.typedb.core.test.assembly.AssemblyTest",
    srcs = ["AssemblyTest.java"],
    deps = [
        "//tool/runner:typedb-runner",
        "@maven//:com_vaticle_typedb_typedb_console_runner",
    ],
    server_artifacts = {
        "@vaticle_bazel_distribution//platform:is_linux_arm64": "//server:assemble-linux-arm64-targz",
        "@vaticle_bazel_distribution//platform:is_linux_x86_64": "//server:assemble-linux-x86_64-targz",
        "@vaticle_bazel_distribution//platform:is_mac_arm64": "//server:assemble-mac-arm64-zip",
        "@vaticle_bazel_distribution//platform:is_mac_x86_64": "//server:assemble-mac-x86_64-zip",
        "@vaticle_bazel_distribution//platform:is_windows_x86_64": "//server:assemble-windows-x86_64-zip",
    },
    console_artifacts = {
        "@vaticle_bazel_distribution//platform:is_linux_arm64": "@vaticle_typedb_console_artifact_linux-arm64//file",
        "@vaticle_bazel_distribution//platform:is_linux_x86_64": "@vaticle_typedb_console_artifact_linux-x86_64//file",
        "@vaticle_bazel_distribution//platform:is_mac_arm64": "@vaticle_typedb_console_artifact_mac-arm64//file",
        "@vaticle_bazel_distribution//platform:is_mac_x86_64": "@vaticle_typedb_console_artifact_mac-x86_64//file",
        "@vaticle_bazel_distribution//platform:is_windows_x86_64": "@vaticle_typedb_console_artifact_windows-x86_64//file",
    },
    data = [":console-script"],
)

typedb_java_test(
    name = "docker",
    test_class = "com.vaticle.typedb.core.test.assembly.DockerTest",
    srcs = ["DockerTest.java"],
    deps = [
        "@maven//:com_vaticle_typedb_typedb_console_runner",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_zeroturnaround_zt_exec",
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    server_artifacts = {
        "@vaticle_bazel_distribution//platform:is_linux_arm64": "//server:assemble-linux-arm64-targz",
        "@vaticle_bazel_distribution//platform:is_linux_x86_64": "//server:assemble-linux-x86_64-targz",
        "@vaticle_bazel_distribution//platform:is_mac_arm64": "//server:assemble-mac-arm64-zip",
        "@vaticle_bazel_distribution//platform:is_mac_x86_64": "//server:assemble-mac-x86_64-zip",
        "@vaticle_bazel_distribution//platform:is_windows_x86_64": "//server:assemble-windows-x86_64-zip",
    },
    console_artifacts = {
        "@vaticle_bazel_distribution//platform:is_linux_arm64": "@vaticle_typedb_console_artifact_linux-arm64//file",
        "@vaticle_bazel_distribution//platform:is_linux_x86_64": "@vaticle_typedb_console_artifact_linux-x86_64//file",
        "@vaticle_bazel_distribution//platform:is_mac_arm64": "@vaticle_typedb_console_artifact_mac-arm64//file",
        "@vaticle_bazel_distribution//platform:is_mac_x86_64": "@vaticle_typedb_console_artifact_mac-x86_64//file",
        "@vaticle_bazel_distribution//platform:is_windows_x86_64": "@vaticle_typedb_console_artifact_windows-x86_64//file",
    },
    data = [
        "//:assemble-docker.tar"
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = ["console-script"],
    license_type = "mpl-header",
)
