java_test(
    name = "benchmark-it",
    srcs = ["BenchmarkIT.java"],
    test_class = "ai.grakn.test.graql.reasoner.BenchmarkIT",
    deps = [
        "//server",
        "//client-java",
        "//test-integration/rule:cassandra-context",
        "//test-integration/rule:server-context",
        "//test-integration/util:grakn-test-util",
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "medium",
    resources = ["//server:grakn-properties", "linearTransitivity.gql", "multiJoin.gql"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)