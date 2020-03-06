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

load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "grakn-test-server",
    srcs = glob(["*.java"]),
    data = [
        "//server:conf/grakn.properties",
        "//test-integration/resources:cassandra-embedded",
    ],
    resources = ["//test-integration/resources:logback-test"],
    visibility = ["//visibility:public"],
    deps = [
        "//common",
        "//concept/impl",
        "//kb/concept/api",
        "//kb/keyspace",
        "//kb/graql/planning",
        "//kb/graql/executor",
        "//concept/manager",
        "//concept/structure",
        "//graql/reasoner",
        "//graql/planning",
        "//graql/executor",
        "//keyspace",
        "//core",
        "//dependencies/maven/artifacts/com/datastax/oss:java-driver-core",
        "//dependencies/maven/artifacts/commons-io",
        "//dependencies/maven/artifacts/commons-lang",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-api",
        "//dependencies/maven/artifacts/junit",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:hadoop-gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph-gremlin",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//graph",
        "//kb/server",
        "//kb/concept/manager",
        "//server",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-test-server",
    ],
)
