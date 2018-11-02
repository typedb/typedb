java_test(
    name = "benchmark-big-it",
    test_class = "ai.grakn.graql.internal.reasoner.BenchmarkBigIT",
    srcs = ["BenchmarkBigIT.java"],
    deps = [
        "//server",
        "//client-java",
        "//grakn-graql",
        "//test-integration/rule:grakn-server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "medium",
    resources = [
           "//test-integration/test/graql/reasoner/resources:multi-join",
           "//test-integration/test/graql/reasoner/resources:linear-transivity"
           ],
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "benchmark-small-it",
    test_class = "ai.grakn.graql.internal.reasoner.BenchmarkSmallIT",
    srcs = ["BenchmarkSmallIT.java"],
    deps = [
        "//server",
        "//client-java",
        "//grakn-graql",
        "//test-integration/rule:grakn-server",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//test-integration/test/graql/reasoner/graph:linear-transitivity-matrix-graph",
        "//test-integration/test/graql/reasoner/graph:transitivity-chain-graph",
        "//test-integration/test/graql/reasoner/graph:diagonal-graph",
        "//test-integration/test/graql/reasoner/graph:path-tree-graph",
        "//test-integration/test/graql/reasoner/graph:transitivity-matrix-graph",
    ],
    size = "medium",
    resources = [
        "//test-integration/test/graql/reasoner/resources:generic-schema",
        "//test-integration/test/graql/reasoner/resources:unification-with-types"
        ],
    classpath_resources = ["//test-integration/resources:logback-test"]
)