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
    name = "chu-liu-edmonds-test",
    size = "small",
    srcs = [
        "ChuLiuEdmondsTest.java",
        "graph/DenseWeightedGraph.java",
    ],
    test_class = "grakn.core.graql.gremlin.spanningtree.ChuLiuEdmondsTest",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/graql/planning",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "fibonacci-heap-test",
    size = "small",
    srcs = ["datastructure/FibonacciHeapTest.java"],
    test_class = "grakn.core.graql.gremlin.spanningtree.datastructure.FibonacciHeapTest",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/graql/planning",
    ],
)

java_test(
    name = "fibonacci-queue-test",
    size = "small",
    srcs = ["datastructure/FibonacciQueueTest.java"],
    test_class = "grakn.core.graql.gremlin.spanningtree.datastructure.FibonacciQueueTest",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/graql/planning",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":chu-liu-edmonds-test",
        ":fibonacci-heap-test",
        ":fibonacci-queue-test",
    ],
)
