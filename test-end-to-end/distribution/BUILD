load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

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
    ],
    data = ["//:distribution-zip-mac"],
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
    data = ["//:distribution-zip-mac"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-graql-commands-e2e",
        ":grakn-graql-commands-with-a-running-grakn-e2e"
    ]
)
