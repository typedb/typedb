java_test(
    name = "graql-shell-it",
    srcs = ["ConsoleSessionIT.java"],
    test_class = "grakn.core.graql.shell.GraqlShellIT",
    deps = [
        "//server",
        "//console",
        "//common",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/sharegov:mjson",
        "//dependencies/maven/artifacts/com/google/auto/value:auto-value",
        "//dependencies/maven/artifacts/commons-io:commons-io"
    ],
    data = ["file-(with-parentheses).gql"],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)