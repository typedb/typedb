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
    name = "chu-liu-edmonds-test",
    test_class = "grakn.core.graql.gremlin.spanningtree.ChuLiuEdmondsTest",
    srcs = ["ChuLiuEdmondsTest.java", "graph/DenseWeightedGraph.java"],
    deps = [
        "@graknlabs_graql//java:graql",
        "//server:server",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "small"
)

java_test(
    name = "fibonacci-heap-test",
    test_class = "grakn.core.graql.gremlin.spanningtree.datastructure.FibonacciHeapTest",
    srcs = ["datastructure/FibonacciHeapTest.java"],
    deps = ["//server", "//dependencies/maven/artifacts/com/google/guava:guava"],
    size = "small"
)

java_test(
    name = "fibonacci-queue-test",
    test_class = "grakn.core.graql.gremlin.spanningtree.datastructure.FibonacciQueueTest",
    srcs = ["datastructure/FibonacciQueueTest.java"],
    deps = ["//server", "//dependencies/maven/artifacts/com/google/guava:guava"],
    size = "small"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":chu-liu-edmonds-test",
        ":fibonacci-heap-test",
        ":fibonacci-queue-test"
    ]
)