java_library(
    name = "grakn-for-test",
    srcs = ["GraknForTest.java"],
    deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
    ],
    data = [
        "@graknlabs_grakn_core//:assemble-linux-targz",
    ],
    visibility = ["//visibility:public"]
)

java_library(
    name = "load-test",
    srcs = ["LoadTest.java"],
    deps = [
        "//resolution/common:common",
        # External dependencies from @graknlabs
        "@graknlabs_client_java//:client-java",
    ],
    visibility = ["//visibility:public"]
)

java_test(
    name = "QueryBuilderIT",
    srcs = [
        "QueryBuilderIT.java",
    ],
    test_class = "grakn.verification.resolution.test.QueryBuilderIT",
    deps = [
        "//resolution/common",
        "//resolution/resolve",
        "//resolution/test:grakn-for-test",
        "//resolution/test:load-test",

        # External dependencies from @graknlabs
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "@graknlabs_grakn_core//:assemble-linux-targz", # Make sure to pass the path in args below
        "//resolution/test/cases/case1:schema.gql",
        "//resolution/test/cases/case1:data.gql",
        "//resolution/test/cases/case2:schema.gql",
        "//resolution/test/cases/case2:data.gql",
        "//resolution/test/cases/case3:schema.gql",
        "//resolution/test/cases/case3:data.gql",
        "//resolution/test/cases/case4:schema.gql",
        "//resolution/test/cases/case4:data.gql",
        "//resolution/test/cases/case5:schema.gql",
        "//resolution/test/cases/case5:data.gql",
    ],
    args = [ # The order of the arguments matter
        "grakn-core", # Keep at index 0, will be accessible at args[1]
        "$(location @graknlabs_grakn_core//:assemble-linux-targz)", # Keep at index 1, will be accessible at args[2]
    ],
    size="medium"
)

java_test(
    name = "TestQueryBuilder",
    srcs = [
        "TestQueryBuilder.java",
    ],
    test_class = "grakn.verification.resolution.test.TestQueryBuilder",
    deps = [
        "//resolution/common",
        "//resolution/resolve",
        "//dependencies/maven/artifacts/com/google/guava",

        # External dependencies from @graknlabs
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
    ],
    size="small"
)

java_test(
    name = "TestSchemaManager",
    srcs = [
        "TestSchemaManager.java",
    ],
    test_class = "grakn.verification.resolution.test.TestSchemaManager",
    deps = [
        "//resolution/common",
        "//resolution/complete",
        "//resolution/test:grakn-for-test",

        # External dependencies from @graknlabs
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
    ],
    data = [
        # TODO Why does this work without the schema and data here?
        "@graknlabs_grakn_core//:assemble-linux-targz", # Make sure to pass the path in args below
    ],
    args = [ # The order of the arguments matter
        "grakn-core", # Keep at index 0, will be accessible at args[1]
        "$(location @graknlabs_grakn_core//:assemble-linux-targz)", # Keep at index 1, will be accessible at args[2]
    ],
    size="medium"
)

java_test(
    name = "TestCompleter",
    srcs = [
        "TestCompleter.java",
    ],
    test_class = "grakn.verification.resolution.test.TestCompleter",
    deps = [
        "//resolution/common",
        "//resolution/complete",
        "//resolution/test:grakn-for-test",
        "//resolution/test:load-test",

        # External dependencies from @graknlabs
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "//resolution/test/cases/case1:schema.gql",
        "//resolution/test/cases/case1:data.gql",
        "//resolution/test/cases/case4:schema.gql",
        "//resolution/test/cases/case4:data.gql",
        "@graknlabs_grakn_core//:assemble-linux-targz", # Make sure to pass the path in args below
    ],
    args = [ # The order of the arguments matter
        "grakn-core", # Keep at index 0, will be accessible at args[1]
        "$(location @graknlabs_grakn_core//:assemble-linux-targz)", # Keep at index 1, will be accessible at args[2]
    ],
    size="medium"
)

java_test(
    name = "TestResolution",
    srcs = [
        "TestResolution.java",
    ],
    test_class = "grakn.verification.resolution.test.TestResolution",
    deps = [
        "//resolution:resolution-lib",
        "//resolution/test:grakn-for-test",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "//resolution/test/cases/case1:schema.gql",
        "//resolution/test/cases/case1:data.gql",
        "//resolution/test/cases/case2:schema.gql",
        "//resolution/test/cases/case2:data.gql",
        "//resolution/test/cases/case3:schema.gql",
        "//resolution/test/cases/case3:data.gql",
        "//resolution/test/cases/case4:schema.gql",
        "//resolution/test/cases/case4:data.gql",
        "//resolution/test/cases/case5:schema.gql",
        "//resolution/test/cases/case5:data.gql",
        "//resolution/test/cases/case6:schema.gql",
        "//resolution/test/cases/case6:data.gql",
        "//resolution/test/cases/case7:schema.gql",
        "//resolution/test/cases/case7:data.gql",
        "@graknlabs_grakn_core//:assemble-linux-targz", # Make sure to pass the path in args below
    ],
    args = [ # The order of the arguments matter
        "grakn-core", # Keep at index 0, will be accessible at args[1]
        "$(location @graknlabs_grakn_core//:assemble-linux-targz)", # Keep at index 1, will be accessible at args[2]
    ],
    size="medium"
)