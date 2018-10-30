java_test(
    name = "isa-explicit-it",
    test_class = "ai.grakn.test.graql.internal.IsaExplicitIT",
    srcs = ["IsaExplicitIT.java"],
    deps = [
        "//test-integration/rule:cassandra-context",
        "//test-integration/rule:server-context",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//grakn-graql"],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "graql-traversal-it",
    test_class = "ai.grakn.graql.internal.gremlin.GraqlTraversalIT",
    srcs = ["GraqlTraversalIT.java"],
    deps = [
        "//test-integration/rule:cassandra-context",
        "//test-integration/rule:server-context",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//grakn-graql"],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)