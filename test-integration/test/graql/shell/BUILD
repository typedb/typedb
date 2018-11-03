java_test(
    name = "graql-shell-it",
    srcs = ["GraqlShellIT.java"],
    test_class = "ai.grakn.test.graql.shell.GraqlShellIT",
    deps = [
        "//grakn-graql",
        "//console",
        "//test-integration/rule:grakn-server",
        "//test-integration/util:grakn-test-util",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/sharegov:mjson",
        "//dependencies/maven/artifacts/com/google/auto/value:auto-value",
        "//dependencies/maven/artifacts/commons-io:commons-io"
    ],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)