java_test(
    name = "attribute-deduplicator-it",
    test_class = "grakn.core.test.server.AttributeDeduplicatorIT",
    srcs = ["AttributeDeduplicatorIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:concurrent-grakn-server",
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"],
    size = "large"
)

java_test(
    name = "server-start-it",
    test_class = "grakn.core.test.server.ServerStartIT",
    srcs = ["ServerStartIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:cassandra-context",
        "//test-integration/rule:server-context",
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"],
    size = "large"
)