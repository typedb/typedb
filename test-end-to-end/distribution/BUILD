java_test(
    name = "grakn-graql-commands-e2e",
    test_class = "ai.grakn.distribution.GraknGraqlCommandsE2E",
    srcs = [ "GraknGraqlCommandsE2E.java", "DistributionE2EConstants.java" ],
    deps = [
        "//grakn-graql:grakn-graql",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
    ],
    data = ["//:distribution"]
)

java_test(
    name = "grakn-graql-commands-with-a-running-grakn-e2e",
    test_class = "ai.grakn.distribution.GraknGraqlCommands_WithARunningGraknE2E",
    srcs = [ "GraknGraqlCommands_WithARunningGraknE2E.java", "DistributionE2EConstants.java" ],
    deps = [
        "//grakn-graql:grakn-graql",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec"
    ],
    data = ["//:distribution"]
)