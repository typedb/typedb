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
package(
    default_visibility = [
        "//:__pkg__",
        "//rocks:__pkg__",
        "//reasoner:__pkg__",
        "//test:__subpackages__",
    ]
)

native_java_libraries(
    name = "logic",
    srcs = glob(["*.java", "**/*.java"], exclude = ["*Test.java", "*/*Test.java"],),
    deps = [
        # Internal dependencies
        "//common:common",
        "//graph:graph",

        # Internal Repository Dependencies
        "@graknlabs_common//:common",
        "@graknlabs_graql//java/common:common",
        "@graknlabs_graql//java/pattern:pattern",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
    ],
    native_libraries_deps = [
        "//concept:concept",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-logic:{pom_version}"],
    visibility = ["//visibility:public"],
)


host_compatible_java_test(
    name = "test-concludable",
    srcs = [
        "resolvable/ConcludableTest.java",
    ],
    test_class = "grakn.core.logic.resolvable.ConcludableTest",
    native_libraries_deps = [
        "//logic:logic",
        "//pattern:pattern",
    ],
    deps = [
        # Internal dependencies

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java/common",
        "@graknlabs_graql//java/pattern",
        "@graknlabs_graql//java:graql",
    ],
)


checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    license_type = "agpl",
)
