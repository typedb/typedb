java_test(
    name = "remote-concept-it",
    srcs = ["RemoteConceptIT.java"],
    test_class = "ai.grakn.test.client.RemoteConceptIT",
    deps = [
        "//server",
        "//client-java",
        "//grakn-graql",
        "//test-integration/rule:concurrent-grakn-server",
        "//test-integration/util:grakn-test-util",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
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
        "//test-integration/rule:concurrent-grakn-server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
