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
    test_class = "grakn.core.test.behaviour.resolution.test.QueryBuilderIT",
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
        "//test/behaviour/resolution/framework/test/cases/case1:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case1:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case2:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case2:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case3:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case3:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case4:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case4:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case5:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case5:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case8:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case8:data.gql",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    size="medium"
)

java_test(
    name = "test-query-builder",
    srcs = [
        "TestQueryBuilder.java",
    ],
    test_class = "grakn.core.test.behaviour.resolution.test.TestQueryBuilder",
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
    test_class = "grakn.core.test.behaviour.resolution.test.TestSchemaManager",
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
            "//test/behaviour/resolution/framework/test/cases/case2:schema.gql",
            "//test/behaviour/resolution/framework/test/cases/case2:data.gql",
        ],
    classpath_resources = ["//test/resources:logback-test"],
    size="medium"
)

java_test(
    name = "test-completer",
    srcs = [
        "TestCompleter.java",
    ],
    test_class = "grakn.core.test.behaviour.resolution.test.TestCompleter",
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
        "//test/behaviour/resolution/framework/test/cases/case1:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case1:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case4:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case4:data.gql",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    size="medium"
)

java_test(
    name = "test-resolution",
    srcs = [
        "TestResolution.java",
    ],
    test_class = "grakn.core.test.behaviour.resolution.test.TestResolution",
    deps = [
        "//test/behaviour/resolution/framework/common",
        "//test/behaviour/resolution/framework:resolution-lib",
        "//test/rule:grakn-test-server",
        "//kb/server:server",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "//test/behaviour/resolution/framework/test/cases/case1:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case1:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case2:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case2:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case3:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case3:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case4:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case4:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case5:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case5:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case6:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case6:data.gql",
        "//test/behaviour/resolution/framework/test/cases/case7:schema.gql",
        "//test/behaviour/resolution/framework/test/cases/case7:data.gql",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    size="medium"
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
