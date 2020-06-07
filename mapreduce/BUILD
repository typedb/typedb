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

java_library(
    name = "mapreduce",
    srcs = glob(
        ["**/*.java"],
        exclude = ["test/**"],
    ),
    deps = [
        # Internal dependencies
        "//graph",
        "//common",
        # External dependencies from @graknlabs

        # External dependencies from Maven
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-mapreduce-client-core",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-common",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-annotations",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:hadoop-gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph-gremlin",
        "//dependencies/maven/artifacts/commons-configuration:commons-configuration",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/com/datastax/oss:java-driver-core",

#        The following 2 dependencies should be removed once GraknInputFormat and InputFormatCQL stop depending on old Thrift trickery
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-thrift",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift"
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-graph-hadoop:{pom_version}"],

)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":mapreduce"
    ]
)