load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "grakn-graql-commands-e2e",
    test_class = "grakn.core.distribution.GraknGraqlCommandsE2E",
    srcs = [ "GraknGraqlCommandsE2E.java", "DistributionE2EConstants.java" ],
    deps = [
        "//server",
        "//common",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "@graknlabs_client_java//:client-java",
    ],
    data = ["//:assemble-mac-zip"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "grakn-graql-commands-with-a-running-grakn-e2e",
    test_class = "grakn.core.distribution.GraknGraqlCommands_WithARunningGraknE2E",
    srcs = [ "GraknGraqlCommands_WithARunningGraknE2E.java", "DistributionE2EConstants.java" ],
    deps = [
        "//server",
        "//common",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec"
    ],
    data = ["//:assemble-mac-zip"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "concurrency-e2e",
    test_class = "grakn.core.distribution.ConcurrencyE2E",
    srcs = ["ConcurrencyE2E.java", "DistributionE2EConstants.java"],
    deps = [
        "//server",
        "//common",
        "//concept",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
    ],
    size = "large",
    data = ["//:assemble-mac-zip"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "attribute-uniqueness-e2e",
    test_class = "grakn.core.distribution.AttributeUniquenessE2E",
    srcs = ["AttributeUniquenessE2E.java", "DistributionE2EConstants.java"],
    deps = [
        # Internal dependencies
        "//common:common",
        "//concept:concept",
        "//server:server",

        # External dependencies from @graknlabs
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    data = ["//:assemble-mac-zip"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-graql-commands-e2e",
        ":grakn-graql-commands-with-a-running-grakn-e2e",
        ":attribute-uniqueness-e2e",
        "concurrency-e2e"
    ]
)
