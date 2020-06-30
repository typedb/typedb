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

java_test(
    name = "session-it",
    srcs = ["SessionIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.server.session.SessionIT",
    deps = [
        "//kb/server",
        "//kb/concept/api",
        "//server",
        "//test/rule:grakn-test-server",
    ],
)

java_test(
    name = "transaction-it",
    size = "large",
    srcs = ["TransactionIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.server.session.TransactionIT",
    deps = [
        "//common",
        "//concept/answer",
        "//concept/impl",
        "//core",
        "//graph",
        "//kb/server",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//server",
        "//test/rule:grakn-test-server",
        "//test/integration/util:concept-downcasting",
        "//test/integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_tinkerpop_gremlin_core",
        "@maven//:org_hamcrest_hamcrest_library",
    ],
)

java_test(
    name = "transaction-cache-it",
    srcs = ["cache/TransactionCacheIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    data = ["//test/resources:grakn-small-tx-cache"],
    test_class = "grakn.core.server.session.cache.TransactionCacheIT",
    deps = [
        "//core",
        "//kb/server",
        "//kb/concept/api",
        "//test/rule:grakn-test-server",
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
        "@maven//:org_apache_tinkerpop_gremlin_core",
        "@maven//:org_hamcrest_hamcrest_library",
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
