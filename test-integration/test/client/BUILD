java_test(
    name = "remote-concept-it",
    srcs = ["RemoteConceptIT.java"],
    test_class = "ai.grakn.test.client.RemoteConceptIT",
    deps = [
        "//server",
        "//client-java",
        "//grakn-graql",
        "//test-integration/rule:cassandra-context",
        "//test-integration/rule:server-context",
        "//test-integration/util:grakn-test-util",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "remote-query-it",
    srcs = ["RemoteQueryIT.java"],
    test_class = "ai.grakn.test.client.RemoteQueryIT",
    deps = [
        "//server",
        "//client-java",
        "//grakn-graql",
        "//test-integration/rule:cassandra-context",
        "//test-integration/rule:server-context",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
