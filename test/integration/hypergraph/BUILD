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
    name = "test-hypergraph",
    test_class = "hypergraph.HypergraphTest",
    srcs = ["HypergraphTest.java"],
    deps = [
        "//:hypergraph",
        "//concept:concept",
        "//core:hypergraph",
    ]
)

java_test(
    name = "test-connection",
    test_class = "hypergraph.ConnectionTest",
    srcs = ["ConnectionTest.java"],
    deps = [
        "//:hypergraph",
        "//core:hypergraph",
    ]
)

java_test(
    name = "test-keyspace",
    test_class = "hypergraph.KeyspaceTest",
    srcs = ["KeyspaceTest.java"
            ],
    deps = [
        "//:hypergraph",
        "//core:hypergraph",
    ]
)

java_test(
    name = "test-session",
    test_class = "hypergraph.SessionTest",
    srcs = ["SessionTest.java"],
    deps = [
        "//:hypergraph",
        "//core:hypergraph",
    ]
)

java_test(
    name = "test-transaction",
    test_class = "hypergraph.TransactionTest",
    srcs = ["TransactionTest.java"],
    deps = [
        "//:hypergraph",
        "//core:hypergraph",
    ]
)

java_test(
    name = "test-concept",
    test_class = "hypergraph.ConceptTest",
    srcs = ["ConceptTest.java"],
    deps = [
        "//:hypergraph",
        "//concept:concept",
        "//core:hypergraph",
    ]
)

java_test(
    name = "test-traversal",
    test_class = "hypergraph.TraversalTest",
    srcs = ["TraversalTest.java"],
    deps = [
        "//:hypergraph",
        "//concept:concept",
        "//core:hypergraph",
        "//traversal:traversal",
    ]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":test-hypergraph",
        ":test-connection",
        ":test-keyspace",
        ":test-session",
        ":test-transaction",
        ":test-concept",
        ":test-traversal",
    ]
)