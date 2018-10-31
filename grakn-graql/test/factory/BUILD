java_test(
    name = "tx-factory-builder-test",
    test_class = "ai.grakn.factory.TxFactoryBuilderTest",
    srcs = ["TxFactoryBuilderTest.java"],
    resources = ["inmemory-graph.properties"],
    deps = ["//grakn-graql", "//dependencies/maven/artifacts/org/mockito:mockito-core"],
    size = "small"
)

java_test(
    name = "grakn-tx-tinker-factory-test",
    test_class = "ai.grakn.factory.GraknTxTinkerFactoryTest",
    srcs = ["GraknTxTinkerFactoryTest.java"],
    deps = [
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph-gremlin"
    ],
    resources = ["inmemory-graph.properties"],
    size = "small"
)