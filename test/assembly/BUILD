load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "grakn-graql-commands-e2e",
    srcs = [
        "AssemblyE2EConstants.java",
        "GraknGraqlCommandsE2E.java",
    ],
    classpath_resources = ["//test/integration/resources:logback-test"],
    data = ["//:assemble-mac-zip"],
    test_class = "grakn.core.assembly.GraknGraqlCommandsE2E",
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
        "AssemblyE2EConstants.java",
        "GraknGraqlCommands_WithARunningGraknE2E.java",
    ],
    classpath_resources = ["//test/integration/resources:logback-test"],
    data = ["//:assemble-mac-zip"],
    test_class = "grakn.core.assembly.GraknGraqlCommands_WithARunningGraknE2E",
    deps = [
        "//common",
        "//dependencies/maven/artifacts/commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//server",
    ],
)

test_suite(
    name = "test-assembly",
    tests = [
        "//test/assembly:grakn-graql-commands-e2e",
        "//test/assembly:grakn-graql-commands-with-a-running-grakn-e2e",
    ]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-graql-commands-e2e",
        ":grakn-graql-commands-with-a-running-grakn-e2e",
    ],
)
