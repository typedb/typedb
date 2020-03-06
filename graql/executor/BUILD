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
    name = "executor",
    srcs = glob([
        "**/*.java",
    ]),
    tags = ["maven_coordinates=io.grakn.core:grakn-graql-executor:{pom_version}"],
    deps = [
        # Internal dependencies
        "//common",
        "//kb/concept/api",
        "//kb/concept/manager",
        "//kb/graql/reasoner",
        "//kb/graql/executor",
        "//kb/graql/exception",
        "//kb/graql/planning",
        "//kb/keyspace",
        "//concept/answer",
        "//graql/analytics",
        "//graql/reasoner",  # TODO should this be pointing the other way?
        "//graql/planning",
        "//core",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
        "@graknlabs_benchmark//lib:server-instrumentation",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-cli",
        "//dependencies/maven/artifacts/commons-configuration",
        "//dependencies/maven/artifacts/commons-lang",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
        "//dependencies/maven/artifacts/io/grpc:grpc-api",
        "//dependencies/maven/artifacts/io/zipkin/brave",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-common",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-mapreduce-client-core",
        "//dependencies/maven/artifacts/org/apache/spark:spark-core-2-11",
        "//dependencies/maven/artifacts/org/apache/spark:spark-launcher-2-11",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:hadoop-gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:spark-gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph-gremlin",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":executor",
    ],
)
