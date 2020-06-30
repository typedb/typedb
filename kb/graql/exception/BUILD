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
#
package(default_visibility = ["//visibility:public"])

load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "exception",
    srcs = glob([
        "*.java",
    ]),
    tags = ["maven_coordinates=io.grakn.core:grakn-kb-graql-exception:{pom_version}"],
    deps = [
        # Internal dependencies
        "//common",
        "//kb/concept/api",

        # External dependenicies from @graknlabs
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_apache_tinkerpop_gremlin-core",
        "@maven//:com_google_guava_guava",
        "@maven//:org_slf4j_slf4j-api",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":exception",
    ],
)
