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
    name = "graql-get-it",
    size = "medium",
    srcs = ["GraqlGetIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.query.GraqlGetIT",
    deps = [
        "@maven//:org_hamcrest_hamcrest_library",
        "//kb/graql/exception",
        "//kb/server",
        "//test/integration/graql/graph:movie-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "graql-define-it",
    size = "medium",
    srcs = ["GraqlDefineIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.query.GraqlDefineIT",
    deps = [
        "@maven//:org_hamcrest_hamcrest_library",
        "//kb/graql/exception",
        "//kb/server",
        "//test/integration/graql/graph:movie-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "graql-delete-it",
    size = "medium",
    srcs = ["GraqlDeleteIT.java"],
    classpath_resources = [
        "//test/resources:logback-test",
    ],
    test_class = "grakn.core.graql.query.GraqlDeleteIT",
    deps = [
        "//kb/server",
        "//test/integration/graql/graph:movie-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "graql-insert-it",
    size = "medium",
    srcs = ["GraqlInsertIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.query.GraqlInsertIT",
    deps = [
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "//kb/concept/api",
        "//kb/graql/exception",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/integration/graql/graph:movie-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "graql-undefine-it",
    size = "large",
    srcs = ["GraqlUndefineIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.query.GraqlUndefineIT",
    deps = [
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "//kb/concept/api",
        "//kb/graql/exception",
        "//kb/server",
        "//test/integration/graql/graph:movie-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "number-casting-it",
    size = "medium",
    srcs = ["NumberCastingIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.query.NumberCastingIT",
    deps = [
        "//concept/answer",
        "//common",
        "@maven//:com_google_guava_guava",
        "//kb/concept/api",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "query-builder-it",
    size = "medium",
    srcs = ["QueryBuilderIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.query.QueryBuilderIT",
    deps = [
        "//kb/graql/exception",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/integration/graql/graph:movie-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "query-admin-it",
    size = "medium",
    srcs = ["QueryAdminIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.query.QueryAdminIT",
    deps = [
        "@maven//:com_google_guava_guava",
        "//kb/server",
        "//test/integration/graql/graph:movie-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "query-error-it",
    size = "medium",
    srcs = ["QueryErrorIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.query.QueryErrorIT",
    deps = [
        "//common",
        "@maven//:org_hamcrest_hamcrest_library",
        "//kb/concept/api",
        "//kb/graql/exception",
        "//kb/server",
        "//test/integration/graql/graph:movie-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "query-planner-it",
    size = "medium",
    srcs = ["QueryPlannerIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.query.QueryPlannerIT",
    deps = [
        "//common",
        "@maven//:com_google_guava_guava",
        "//graql/planning",
        "//kb/concept/api",
        "//kb/graql/planning",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
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
        ":query-planner-it",
        ":number-casting-it",
    ],
)
