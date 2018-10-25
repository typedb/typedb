java_test(
    name = "attribute-deduplicator-it",
    srcs = ["AttributeDeduplicatorIT.java"],
    test_class = "ai.grakn.test.server.attribute.deduplicator.AttributeDeduplicatorIT",
    deps = [
        "//server",
        "//test-integration/rule:cassandra-context",
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"],
    resources = ["//server:grakn-properties"],
    size = "large"
)

java_test(
    name = "server-start-it",
    srcs = ["ServerStartIT.java"],
    test_class = "ai.grakn.test.server.ServerStartIT",
    deps = [
        "//server",
        "//test-integration/rule:cassandra-context",
        "//test-integration/rule:server-context",
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"],
    size = "large"
)