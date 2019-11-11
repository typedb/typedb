#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2019 Grakn Labs Ltd
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

java_library(
    name = "graph-cql",
    srcs = glob(
        ["**/*.java"],
        exclude = ["test/**"],
    ),
    deps = [
        # Internal dependencies
        "//graph",

        # External dependencies from @graknlabs

        # External dependencies from Maven
        "//dependencies/maven/artifacts/com/datastax/oss:java-driver-core",
        "//dependencies/maven/artifacts/com/datastax/oss:java-driver-query-builder",
        "//dependencies/maven/artifacts/io/vavr:vavr",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-graph-cql:{pom_version}"],

)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graph-cql"
    ],
)