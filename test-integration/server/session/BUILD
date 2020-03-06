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
    name = "session-it",
    srcs = ["SessionIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.session.SessionIT",
    deps = [
        "//kb/server",
        "//kb/concept/api",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "transaction-it",
    size = "large",
    srcs = ["TransactionIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.session.TransactionIT",
    deps = [
        "//common",
        "//concept/answer",
        "//concept/impl",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//graph",
        "//kb/server",
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
    name = "transaction-cache-it",
    srcs = ["cache/TransactionCacheIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    data = ["//test-integration/resources:grakn-small-tx-cache"],
    test_class = "grakn.core.server.session.cache.TransactionCacheIT",
    deps = [
        "//core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
    ],
)


checkstyle_test(
    name = "checkstyle",
    targets = [
        ":session-it",
        ":transaction-it",
        ":transaction-cache-it",
    ],
)
