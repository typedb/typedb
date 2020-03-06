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
        "//graql/executor",
        "//graql/planning",
        "//kb/concept/api",
        "//kb/concept/manager",
        "//kb/graql/planning",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "graql-traversal-it",
    size = "small",
    srcs = [
        "GraqlMatchers.java",
        "GraqlTraversalIT.java",
    ],
    test_class = "grakn.core.graql.gremlin.GraqlTraversalIT",
    deps = [
        "//common",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//graql/executor",
        "//graql/planning",
        "//kb/concept/api",
        "//kb/concept/manager",
        "//kb/graql/planning",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graql-traversal-it",
        ":conjunction-query-test",
    ],
)
