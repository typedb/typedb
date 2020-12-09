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

package(default_visibility = [
    "//query:__subpackages__",
    "//reasoner:__subpackages__",
    "//rocks:__pkg__",
    "//test/integration/reasoner:__pkg__",
])

native_java_libraries(
    name = "reasoner",
    srcs = glob(
        ["*.java",
         "resolution/*.java",
         "resolution/framework/*.java",
         "resolution/resolver/*.java",
         ],
        exclude = ["*Test.java", "*/*Test.java"],
    ),
    native_libraries_deps = [
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    deps = [
        # Internal dependencies
        "//common",
        "//graph", # TODO: this is bad and needs to go

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java/common",
        "@graknlabs_graql//java/pattern",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api"
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-reasoner:{pom_version}"],
    visibility = ["//visibility:public"]
)

host_compatible_java_test(
    name = "test-resolution",
    srcs = ["resolution/ResolutionTest.java"],
    test_class = "grakn.core.reasoner.resolution.ResolutionTest",
    deps = [
        "@graknlabs_common//:common",
    ],
    native_libraries_deps = [
        "//concept:concept",
        "//reasoner:reasoner",
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
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
