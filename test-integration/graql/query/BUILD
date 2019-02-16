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

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "graql-get-it",
    test_class = "grakn.core.graql.query.GraqlGetIT",
    srcs = ["GraqlGetIT.java"],
    deps = [
        "//common:common",
        "//graql/java:graql",
        "//server:server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
    ],
    size = "medium"
)

java_test(
    name = "graql-define-it",
    test_class = "grakn.core.graql.query.GraqlDefineIT",
    srcs = ["GraqlDefineIT.java"],
    deps = [
        "//common:common",
        "//graql/java:graql",
        "//server:server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",

        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
    ],
    size = "medium"
)

java_test(
    name = "graql-delete-it",
    test_class = "grakn.core.graql.query.GraqlDeleteIT",
    srcs = ["GraqlDeleteIT.java"],
    deps = [
        "//common:common",
        "//graql/java:graql",
        "//server:server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
    size = "medium"
)

java_test(
    name = "graql-insert-it",
    test_class = "grakn.core.graql.query.GraqlInsertIT",
    srcs = ["GraqlInsertIT.java"],
    deps = [
        "//common:common",
        "//graql/java:graql",
        "//server:server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",

        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",

    ],
    size = "medium"
)

java_test(
    name = "graql-undefine-it",
    test_class = "grakn.core.graql.query.GraqlUndefineIT",
    srcs = ["GraqlUndefineIT.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",

        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",

    ],
    size = "medium"
)

java_test(
    name = "query-builder-it",
    test_class = "grakn.core.graql.query.QueryBuilderIT",
    srcs = ["QueryBuilderIT.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
    size = "medium"
)

java_test(
    name = "query-admin-it",
    test_class = "grakn.core.graql.query.QueryAdminIT",
    srcs = ["QueryAdminIT.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",

        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    size = "medium"
)

java_test(
    name = "query-error-it",
    test_class = "grakn.core.graql.query.QueryErrorIT",
    srcs = ["QueryErrorIT.java"],
    deps = [
        "//common:common",
        "//graql/java:graql",
        "//server:server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",

        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",

    ],
    size = "medium"
)

java_test(
    name = "query-planner-it",
    test_class = "grakn.core.graql.query.QueryPlannerIT",
    srcs = ["QueryPlannerIT.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",

        "//dependencies/maven/artifacts/com/google/guava:guava",

    ],
    size = "medium"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graql-get-it",
        ":graql-define-it",
        ":graql-delete-it",
        ":graql-insert-it",
        ":graql-undefine-it",
        ":query-builder-it",
        ":query-admin-it",
        ":query-error-it",
        ":query-planner-it"
    ]
)
