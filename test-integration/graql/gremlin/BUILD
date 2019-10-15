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
    name = "conjunction-query-test",
    size = "small",
    srcs = [
        "ConjunctionQueryTest.java",
        "GraqlMatchers.java",
    ],
    test_class = "grakn.core.graql.gremlin.ConjunctionQueryTest",
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "graql:graql-tmp-fix",
        "//kb",
        "//kb/concept/api",
        "//server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "graql-traversal-test",
    size = "small",
    srcs = [
        "GraqlMatchers.java",
        "GraqlTraversalIT.java",
    ],
    test_class = "grakn.core.graql.gremlin.GraqlTraversalTest",
    deps = [
        "//common",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "graql:graql-tmp-fix",
        "//kb",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graql-traversal-test",
        ":conjunction-query-test",
    ],
)
