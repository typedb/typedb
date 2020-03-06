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
    name = "graph",
    srcs = glob(
        ["**/*.java"],
        exclude = ["test/**"],
    ),
    deps = [
        # External dependencies from Maven
        "//dependencies/maven/artifacts/commons-collections:commons-collections",
        "//dependencies/maven/artifacts/commons-configuration:commons-configuration",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/com/datastax/oss:java-driver-core",
        "//dependencies/maven/artifacts/com/datastax/oss:java-driver-query-builder",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind",
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/io/vavr:vavr",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph-gremlin",
        "//dependencies/maven/artifacts/org/javatuples:javatuples",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
    runtime_deps = [
            "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
            "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-graph:{pom_version}"],

)


checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graph"
    ],
)
