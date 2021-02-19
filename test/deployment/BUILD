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

load("@graknlabs_common//test:rules.bzl", "native_grakn_artifact")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

native_grakn_artifact(
    name = "native-grakn-artifact",
    mac_artifact = "//server:assemble-mac-zip",
    linux_artifact = "//server:assemble-linux-targz",
    windows_artifact = "//server:assemble-windows-zip",
    output = "grakn-core-server-native.tar.gz",
    visibility = ["//test:__subpackages__"],
)

java_test(
    name = "apt",
    test_class = "grakn.core.test.deployment.AptTest",
    srcs = ["AptTest.java"],
    deps = [
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_zeroturnaround_zt_exec",
        "@maven//:com_eclipsesource_minimal_json_minimal_json",
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    data = ["@graknlabs_grakn_core_workspace_refs//:refs.json", "//:VERSION"],
    tags = ["no-sandbox"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)
