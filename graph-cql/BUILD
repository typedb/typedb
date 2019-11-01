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
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/io/vavr:vavr",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-graph-cql:{pom_version}"],

)
