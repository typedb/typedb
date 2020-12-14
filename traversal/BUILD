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

load("@graknlabs_dependencies//library/ortools:deps.bzl", "google_or_tools")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_dependencies//builder/java:rules.bzl", "native_java_libraries")

package(
    default_visibility = [
        "//pattern:__subpackages__",
        "//query:__subpackages__",
        "//reasoner:__subpackages__",
        "//rocks:__pkg__",
    ]
)

native_java_libraries(
    name = "traversal",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"]),
    deps = [
        # Internal dependencies
        "//common:common",
        "//graph:graph",

        # Grakn Labs Dependencies
        "@graknlabs_common//:common",
        "@graknlabs_graql//java/common:common",
        "@graknlabs_graql//java/pattern:pattern",

        # External dependencies from Grakn Labs

        # External dependencies from Maven
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api",
    ],
    mac_deps = [
        "@maven//:com_google_ortools_ortools_darwin",
        "@maven//:com_google_ortools_ortools_darwin_java",
    ],
    linux_deps = [
        "@maven//:com_google_ortools_ortools_linux_x86_64",
        "@maven//:com_google_ortools_ortools_linux_x86_64_java"
    ],
    windows_deps = [
        "@maven//:com_google_ortools_ortools_win32_x86_64",
        "@maven//:com_google_ortools_ortools_win32_x86_64_java"
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-traversal:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
