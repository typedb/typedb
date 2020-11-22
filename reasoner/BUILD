#
# Copyright (C) 2020 Grakn Labs
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
load("@graknlabs_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@graknlabs_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

package(default_visibility = ["//query:__subpackages__", "//reasoner:__subpackages__"])

native_java_libraries(
    name = "reasoner",
    srcs = glob(["*.java"]),
    deps = [
        # Internal dependencies
        "//common:common",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",

        # External dependencies from Maven
    ],
    native_libraries_deps = [
        "//concept:concept",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-reasoner:{pom_version}"],
)

native_java_libraries(
    name = "reasoner-execution",
    srcs = glob([
        "execution/*.java",
        "execution/actor/*.java",
        "execution/framework/*.java",
    ], exclude=[
        "execution/*Test.java"
    ]),
    deps = [
        "@graknlabs_common//:common",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api"
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    visibility = ["//visibility:__subpackages__"]
)

host_compatible_java_test(
    name = "execution-test",
    test_class = "grakn.reasoner.execution.ExecutionTest",
    srcs = ["execution/ExecutionTest.java"],
    deps = [
        "@graknlabs_common//:common",
    ],
    native_libraries_deps = [
        "//reasoner:reasoner-execution",
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    resources = [
        "//common/test:logback"
    ],
    resource_strip_prefix = "common/test",
)


checkstyle_test(
    name = "checkstyle",
    include = glob(["**/*"]),
    license_type = "agpl",
)
