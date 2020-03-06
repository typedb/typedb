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

java_test(
    name = "keyspace-schema-cache-it",
    srcs = ["KeyspaceSchemaCacheIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.keyspace.KeyspaceSchemaCacheIT",
    deps = [
        "//kb/server",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_client_java//:client-java",
    ],
)

java_test(
    name = "attribute-manager-it",
    srcs = ["AttributeManagerIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.keyspace.AttributeManagerIT",
    deps = [
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "shard-manager-it",
    srcs = ["ShardManagerIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.keyspace.ShardManagerIT",
    deps = [
        "//common",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "statistics-delta-it",
    srcs = ["StatisticsDeltaIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.keyspace.StatisticsDeltaIT",
    deps = [
        "//kb/concept/api",
        "//kb/keyspace",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "keyspace-statistics-it",
    srcs = ["KeyspaceStatisticsIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.keyspace.KeyspaceStatisticsIT",
    deps = [
        "//concept/answer",
        "//core",
        "//kb/concept/api",
        "//kb/keyspace",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":keyspace-schema-cache-it",
        ":attribute-manager-it",
        ":shard-manager-it",
        ":statistics-delta-it",
        ":keyspace-statistics-it"
    ],
)
