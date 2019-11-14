load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "grakn-graql-commands-e2e",
    srcs = [
        "DistributionE2EConstants.java",
        "GraknGraqlCommandsE2E.java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    data = ["//:assemble-mac-zip"],
    test_class = "grakn.core.distribution.GraknGraqlCommandsE2E",
    deps = [
        "//common",
        "//dependencies/maven/artifacts/commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "@graknlabs_client_java//:client-java",
    ],
)

java_test(
    name = "grakn-graql-commands-with-a-running-grakn-e2e",
    srcs = [
        "DistributionE2EConstants.java",
        "GraknGraqlCommands_WithARunningGraknE2E.java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    data = ["//:assemble-mac-zip"],
    test_class = "grakn.core.distribution.GraknGraqlCommands_WithARunningGraknE2E",
    deps = [
        "//common",
        "//dependencies/maven/artifacts/commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//server",
    ],
)

java_test(
    name = "concurrency-e2e",
    size = "enormous",
    srcs = [
        "ConcurrencyE2E.java",
        "DistributionE2EConstants.java",
        "element/AttributeElement.java",
        "element/Element.java",
        "element/Record.java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    data = ["//:assemble-mac-zip"],
    test_class = "grakn.core.distribution.ConcurrencyE2E",
    deps = [
        "//common",
        "//dependencies/maven/artifacts/commons-io",
        "//dependencies/maven/artifacts/commons-lang",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "@com-google-guava-guava//jar",
        "@graknlabs_client_java//:client-java",
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "attribute-uniqueness-e2e",
    srcs = [
        "AttributeUniquenessE2E.java",
        "DistributionE2EConstants.java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    data = ["//:assemble-mac-zip"],
    test_class = "grakn.core.distribution.AttributeUniquenessE2E",
    deps = [
        # Internal dependencies
        "//common",

        # External dependencies from @graknlabs
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/commons-io",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-graql-commands-e2e",
        ":grakn-graql-commands-with-a-running-grakn-e2e",
        ":attribute-uniqueness-e2e",
        "concurrency-e2e",
    ],
)
