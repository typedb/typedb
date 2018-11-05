java_test(
    name = "graql-shell-it",
    srcs = ["GraqlShellIT.java"],
    test_class = "ai.grakn.test.graql.shell.GraqlShellIT",
    deps = [
        "//grakn-graql",
        "//console",
        "//test-integration/rule:cassandra-context",
        "//test-integration/rule:server-context",
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