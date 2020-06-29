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

java_library(
    name = "load-test",
    srcs = ["LoadTest.java"],
    deps = [
        "//test/behaviour/resolution/framework/common:common",
        "//kb/server:server",
    ],
    visibility = ["//visibility:public"]
)

java_test(
    name = "query-builder-it",
    srcs = [
        "QueryBuilderIT.java",
    ],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.QueryBuilderIT",
    deps = [
        "//test/behaviour/resolution/framework/common",
        "//test/behaviour/resolution/framework/resolve",
        "//test/behaviour/resolution/framework/test:load-test",
        "//test/rule:grakn-test-server",
        "//concept/answer:answer",
        "//kb/server:server",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "//test/behaviour/resolution/framework/test/cases/complex_recursion:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/complex_recursion:data.gql",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    size="medium"
)

java_test(
    name = "test-query-builder",
    srcs = [
        "TestQueryBuilder.java",
    ],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.TestQueryBuilder",
    deps = [
        "//test/behaviour/resolution/framework/common",
        "//test/behaviour/resolution/framework/resolve",
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/server:server",
        "//kb/concept/api:api",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    size="small"
)

java_test(
    name = "test-schema-manager",
    srcs = [
        "TestSchemaManager.java",
    ],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.TestSchemaManager",
    deps = [
        "//test/behaviour/resolution/framework/common",
        "//test/behaviour/resolution/framework/complete",
        "//test/rule:grakn-test-server",
        "//kb/server:server",
        "//kb/concept/api:api",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
    ],
    data = [
            "//test/behaviour/resolution/framework/test/cases/complex_recursion:schema.gql",
            "//test/behaviour/resolution/framework/test/cases/complex_recursion:data.gql",
        ],
    classpath_resources = ["//test/resources:logback-test"],
    size="medium"
)

java_test(
    name = "test-completer",
    srcs = [
        "TestCompleter.java",
    ],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.TestCompleter",
    deps = [
        "//test/behaviour/resolution/framework/common",
        "//test/behaviour/resolution/framework/complete",
        "//test/rule:grakn-test-server",
        "//test/behaviour/resolution/framework/test:load-test",
        "//concept/answer:answer",
        "//kb/server:server",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "//test/behaviour/resolution/framework/test/cases/transitivity:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/transitivity:data.gql",
        "//test/behaviour/resolution/framework/test/cases/basic_recursion:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/basic_recursion:data.gql",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    size="medium"
)

java_test(
    name = "test-resolution",
    srcs = [
        "TestResolution.java",
    ],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.TestResolution",
    deps = [
        "//test/behaviour/resolution/framework/common:common",
        "//test/behaviour/resolution/framework:resolution-lib",
        "//test/rule:grakn-test-server",
        "//kb/server:server",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "//test/behaviour/resolution/framework/test/cases/transitivity:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/transitivity:data.gql",
        "//test/behaviour/resolution/framework/test/cases/complex_recursion:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/complex_recursion:data.gql",
        "//test/behaviour/resolution/framework/test/cases/basic_recursion:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/basic_recursion:data.gql",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    size="large"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":load-test",
        ":query-builder-it",
        ":test-query-builder",
        ":test-schema-manager",
        ":test-completer",
        ":test-resolution",
    ],
)
