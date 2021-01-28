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
load("@graknlabs_dependencies//builder/java:rules.bzl", "native_java_libraries")
package(
    default_visibility = [
        "//:__pkg__",
        "//rocks:__pkg__",
        "//server:__pkg__",
        "//test:__subpackages__",
        "//query:__subpackages__",
        "//reasoner:__subpackages__",
        "//traversal:__pkg__",
        "//logic:__pkg__"
    ]
)

native_java_libraries(
    name = "concept",
    srcs = glob([
        "**/*.java",
    ]),
    deps = [
        # Internal dependencies
        "//common:common",
        "//concurrent:concurrent",
        "//graph:graph",

        # Internal Repository Dependencies
        "@graknlabs_common//:common",
        "@graknlabs_graql//java/common:common",
        "@graknlabs_graql//java/pattern:pattern",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
    ],
    native_libraries_deps = [
        "//traversal:traversal",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-concept:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
