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
    name = "aggregate-query-it",
    test_class = "grakn.core.graql.query.AggregateQueryIT",
    srcs = ["AggregateQueryIT.java"],
    deps = [
        "//common",
        "//server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
    ],
    size = "medium"
)

checkstyle_test(
 name = "aggregate-query-it-checkstyle",
 target = ":aggregate-query-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "define-query-it",
    test_class = "grakn.core.graql.query.DefineQueryIT",
    srcs = ["DefineQueryIT.java"],
    deps = [
        "//common",
        "//server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",

        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
    ],
    size = "medium"
)

checkstyle_test(
 name = "define-query-it-checkstyle",
 target = ":define-query-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "delete-query-it",
    test_class = "grakn.core.graql.query.DeleteQueryIT",
    srcs = ["DeleteQueryIT.java"],
    deps = [
        "//common",
        "//server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
    size = "medium"
)

checkstyle_test(
 name = "delete-query-it-checkstyle",
 target = ":delete-query-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "insert-query-it",
    test_class = "grakn.core.graql.query.InsertQueryIT",
    srcs = ["InsertQueryIT.java"],
    deps = [
        "//common",
        "//server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",

        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",

    ],
    size = "medium"
)

checkstyle_test(
 name = "insert-query-it-checkstyle",
 target = ":insert-query-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "undefine-query-it",
    test_class = "grakn.core.graql.query.UndefineQueryIT",
    srcs = ["UndefineQueryIT.java"],
    deps = [
        "//server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",

        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",

    ],
    size = "medium"
)

checkstyle_test(
 name = "undefine-query-it-checkstyle",
 target = ":undefine-query-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "query-builder-it",
    test_class = "grakn.core.graql.query.QueryBuilderIT",
    srcs = ["QueryBuilderIT.java"],
    deps = [
        "//server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
    size = "medium"
)

checkstyle_test(
 name = "query-builder-it-checkstyle",
 target = ":query-builder-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "query-admin-it",
    test_class = "grakn.core.graql.query.QueryAdminIT",
    srcs = ["QueryAdminIT.java"],
    deps = [
        "//server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",

        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    size = "medium"
)

checkstyle_test(
 name = "query-admin-it-checkstyle",
 target = ":query-admin-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "query-error-it",
    test_class = "grakn.core.graql.query.QueryErrorIT",
    srcs = ["QueryErrorIT.java"],
    deps = [
        "//common",
        "//server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",

        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",

    ],
    size = "medium"
)

checkstyle_test(
 name = "query-error-it-checkstyle",
 target = ":query-error-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "query-planner-it",
    test_class = "grakn.core.graql.query.QueryPlannerIT",
    srcs = ["QueryPlannerIT.java"],
    deps = [
        "//server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",

        "//dependencies/maven/artifacts/com/google/guava:guava",

    ],
    size = "medium"
)

checkstyle_test(
 name = "query-planner-it-checkstyle",
 target = ":query-planner-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)
