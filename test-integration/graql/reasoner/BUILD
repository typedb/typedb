java_test(
    name = "reasoning-it",
    size = "enormous",
    srcs = ["ReasoningIT.java"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.ReasoningIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "recursion-it",
    size = "large",
    srcs = ["RecursionIT.java"],
    classpath_resources = [
        "//test-integration/resources:logback-test",
        "//test-integration/resources:cassandra-embedded",
    ],
    resources = [
        "//test-integration/graql/reasoner/resources/recursion:nguyen",
        "//test-integration/graql/reasoner/resources/recursion:recursion-schemas",
    ],
    test_class = "grakn.core.graql.reasoner.RecursionIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/graql/reasoner/graph:dual-linear-transitivity-graph",
        "//test-integration/graql/reasoner/graph:linear-transitivity-matrix-graph",
        "//test-integration/graql/reasoner/graph:nguyen-graph",
        "//test-integration/graql/reasoner/graph:path-matrix-graph",
        "//test-integration/graql/reasoner/graph:path-tree-graph",
        "//test-integration/graql/reasoner/graph:path-tree-symmetric-graph",
        "//test-integration/graql/reasoner/graph:tail-recursion-graph",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "geo-inference-it",
    size = "small",
    srcs = ["GeoInferenceIT.java"],
    classpath_resources = [
        "//test-integration/resources:logback-test",
        "//test-integration/resources:cassandra-embedded",
    ],
    test_class = "grakn.core.graql.reasoner.GeoInferenceIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/graql/reasoner/graph:geo-graph",
        "//test-integration/rule:grakn-test-server",
    ],
)
