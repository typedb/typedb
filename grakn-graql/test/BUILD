java_test(
    name = "keyspace-test",
    test_class = "ai.grakn.KeyspaceTest",
    srcs = ["KeyspaceTest.java"],
    deps = ["//grakn-graql"],
    size = "small"
)

java_test(
    name = "in-memory-session-test",
    test_class = "ai.grakn.InMemorySessionTest",
    srcs = ["InMemorySessionTest.java"],
    deps = ["//grakn-graql", "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core"],
    size = "small"
)