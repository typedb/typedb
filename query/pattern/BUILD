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
package(default_visibility = ["//query:__subpackages__"])

java_library(
    name = "pattern",
    srcs = glob([
        "*.java",
        "constraint/*.java",
        "constraint/thing/*.java",
        "constraint/type/*.java",
        "variable/*.java",
    ]),
    deps = [
        # Internal dependencies
        "//common:common",
        "//concept:concept",
        "//traversal:traversal",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_grabl_tracing//client:client",
        "@graknlabs_graql//java/common:common",
        "@graknlabs_graql//java/pattern:pattern",

        # External dependencies from Maven
        "@maven//:com_google_code_findbugs_jsr305",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-query-pattern:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
