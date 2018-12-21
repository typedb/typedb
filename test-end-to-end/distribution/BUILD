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
    data = ["//:distribution"]
)

checkstyle_test(
 name = "grakn-graql-commands-e2e-checkstyle",
 target = ":grakn-graql-commands-e2e",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

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
    data = ["//:distribution"]
)

checkstyle_test(
 name = "grakn-graql-commands-with-a-running-grakn-e2e-checkstyle",
 target = ":grakn-graql-commands-with-a-running-grakn-e2e",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)
