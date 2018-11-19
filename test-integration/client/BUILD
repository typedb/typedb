java_test(
    name = "remote-concept-it",
    srcs = ["RemoteConceptIT.java"],
    test_class = "grakn.core.client.RemoteConceptIT",
    deps = [
        "//server",
        "//client-java",
        "//test-integration/rule:grakn-test-server",
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
    test_class = "grakn.core.client.RemoteQueryIT",
    deps = [
        "//server",
        "//client-java",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
