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

java_library(
    name = "grakn-test-server",
    srcs = glob(["*.java"]),
    data = [
        "//server:conf/grakn.properties",
        "//test/resources:cassandra-embedded",
    ],
    resources = ["//test/resources:logback-test"],
    visibility = ["//visibility:public"],
    deps = [
        "//common",
        "//concept/impl",
        "//concept/answer",
        "//concept/manager",
        "//concept/structure",
        "//kb/concept/api",
        "//kb/keyspace",
        "//kb/graql/planning",
        "//kb/graql/executor",
        "//kb/server",
        "//kb/concept/manager",
        "//graql/reasoner",
        "//graql/planning",
        "//graql/executor",
        "//keyspace",
        "//core",
        "//graph",
        "//server",
        "@maven//:com_datastax_oss_java-driver-core",
        "@maven//:commons_io_commons_io",
        "@maven//:commons_lang_commons_lang",
        "@maven//:io_grpc_grpc-core",
        "@maven//:io_grpc_grpc-api",
        "@maven//:junit_junit",
        "@maven//:org_apache_cassandra_cassandra-all",
        "@maven//:org_apache_tinkerpop_hadoop-gremlin",
        "@maven//:org_apache_tinkerpop_tinkergraph-gremlin",
        "@maven//:org_slf4j_slf4j-api",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-test-server",
    ],
)
