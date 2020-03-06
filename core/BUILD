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

load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

# This package is currently a bit of a catch-all for thing sa lot of different packages depend on
# Over time hopefully it will emerge what it should be named or how it should be split up
# TODO definitely not in a final state, goal is to properly name or delete it!

java_library(
    name = "core",
    srcs = glob(["*.java"]),
    deps = [
        "//common:common",
        "//kb/concept/api:api",

        # TEMPORARY
        "//graph",



        # External dependenicies from @graknlabs
        "@graknlabs_graql//java:graql",
        "@graknlabs_common//:common",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core"
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-core:{pom_version}"],
)



checkstyle_test(
    name = "checkstyle",
    targets = [
        ":core"
    ],
)