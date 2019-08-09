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
    name = "uncomitted-statistics-delta-it",
    test_class = "grakn.core.server.statistics.UncomittedStatisticsDeltaIT",
    srcs = ["UncomittedStatisticsDeltaIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//concept",
        "@graknlabs_graql//java:graql",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "keyspace-statistics-it",
    test_class = "grakn.core.server.statistics.KeyspaceStatisticsIT",
    srcs = ["KeyspaceStatisticsIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//concept",
        "@graknlabs_graql//java:graql",
        "@graknlabs_client_java//:client-java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"]
)


checkstyle_test(
    name = "checkstyle",
    targets = [
        ":uncomitted-statistics-delta-it",
        ":keyspace-statistics-it",
    ],
)