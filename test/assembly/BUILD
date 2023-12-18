#
# Copyright (C) 2022 Vaticle
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("//test/runner:rules.bzl", "typedb_java_test")


typedb_java_test(
    name = "assembly",
    test_class = "com.vaticle.typedb.core.test.assembly.AssemblyTest",
    srcs = ["AssemblyTest.java"],
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
    license_type = "agpl-header",
)
