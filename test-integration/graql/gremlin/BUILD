#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
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
    name = "janus-micro-benchmark",
    size = "enormous",
    srcs = ["JanusMicroBenchmark.java"],
    test_class = "grakn.core.graql.gremlin.JanusMicroBenchmark",
    classpath_resources = [ "//test-integration/resources:logback-test"],
    data = [
        "//test-integration/resources:cassandra-embedded-no-caches",
        "//test-integration/resources:grakn-properties-no-caches"],
    deps = [
        "//common",
        "//concept",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core"
    ],
)


checkstyle_test(
    name = "checkstyle",
    targets = [
        ":janus-micro-benchmark",
    ],
)