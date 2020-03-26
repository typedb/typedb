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
    srcs = ["HypergraphTest.java"],
    test_class = "hypergraph.HypergraphTest",
    deps = [
        "//:hypergraph",
        "//concept",
        "//core:hypergraph",
    ],
)

java_test(
    name = "test-connection",
    srcs = ["ConnectionTest.java"],
    test_class = "hypergraph.ConnectionTest",
)

java_test(
    name = "test-keyspace",
    srcs = [
        "KeyspaceTest.java",
    ],
    test_class = "hypergraph.KeyspaceTest",
)

java_test(
    name = "test-session",
    srcs = ["SessionTest.java"],
    test_class = "hypergraph.SessionTest",
)

java_test(
    name = "test-transaction",
    srcs = ["TransactionTest.java"],
    test_class = "hypergraph.TransactionTest",
)

java_test(
    name = "test-concept",
    srcs = ["ConceptTest.java"],
    test_class = "hypergraph.ConceptTest",
)

java_test(
    name = "test-traversal",
    srcs = ["TraversalTest.java"],
    test_class = "hypergraph.TraversalTest",
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
    ],
)
