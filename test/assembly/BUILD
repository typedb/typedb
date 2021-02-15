#
# Copyright (C) 2021 Grakn Labs
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

load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_common//test/assembly:rules.bzl", "server_java_test")


server_java_test(
    name = "assembly",
    test_class = "grakn.core.test.assembly.AssemblyTest",
    srcs = ["AssemblyTest.java"],
    server_mac_artifact = "//server:assemble-mac-zip",
    server_linux_artifact = "//server:assemble-linux-targz",
    server_windows_artifact = "//server:assemble-windows-zip",
)

java_test(
    name = "docker",
    srcs = ["DockerTest.java"],
    test_class = "grakn.core.test.assembly.DockerTest",
    deps = [
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_zeroturnaround_zt_exec",
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    data = [
        "//:assemble-docker.tar"
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)
