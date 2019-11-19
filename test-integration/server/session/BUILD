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

load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "session-it",
    srcs = ["SessionIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.session.SessionIT",
    deps = [
        "//kb",
        "//kb/concept/api",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "transaction-oltp-it",
    size = "large",
    srcs = ["TransactionOLTPIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.session.TransactionOLTPIT",
    deps = [
        "//common",
        "//concept/answer",
        "//concept/impl",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//graph",
        "//kb",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:concept-downcasting",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "keyspace-schema-cache-it",
    srcs = ["cache/KeyspaceSchemaCacheIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.session.cache.KeyspaceSchemaCacheIT",
    deps = [
        "//kb",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_client_java//:client-java",
    ],
)

java_test(
    name = "transaction-cache-it",
    srcs = ["cache/TransactionCacheIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    data = ["//test-integration/resources:grakn-small-tx-cache"],
    test_class = "grakn.core.server.session.cache.TransactionCacheIT",
    deps = [
        "//core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "attribute-manager-it",
    srcs = ["AttributeManagerIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.session.AttributeManagerIT",
    deps = [
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb",
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
    test_class = "grakn.core.server.session.ShardManagerIT",
    deps = [
        "//common",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":session-it",
        ":transaction-oltp-it",
        ":keyspace-schema-cache-it",
        ":transaction-cache-it",
        ":attribute-manager-it",
        ":shard-manager-it",
    ],
)
