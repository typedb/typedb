java_test(
    name = "atomic-query-equivalence-it",
    srcs = ["AtomicQueryEquivalenceIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.AtomicQueryEquivalenceIT",
    deps = [
            "//grakn-graql",
            "//test-integration/rule:cassandra-context",
            "//test-integration/rule:server-context",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava"
        ],
    resources = ["//test-integration/test/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "atomic-query-it",
    srcs = ["AtomicQueryIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.AtomicQueryIT",
    deps = [
            "//grakn-graql",
            "//test-integration/rule:cassandra-context",
            "//test-integration/rule:server-context",
            "//test-integration/test/graql/reasoner/graph:geo-graph",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/test/graql/reasoner/resources:materialisation"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "atomic-query-unification-it",
    srcs = ["AtomicQueryUnificationIT.java", "TestQueryPattern.java"],
    test_class = "ai.grakn.graql.internal.reasoner.AtomicQueryUnificationIT",
    deps = [
            "//grakn-graql",
            "//test-integration/rule:cassandra-context",
            "//test-integration/rule:server-context",
            "//test-integration/test/graql/reasoner/graph:geo-graph",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//dependencies/maven/artifacts/commons-collections:commons-collections",
        ],
    resources = ["//test-integration/test/graql/reasoner/resources:generic-schema", "//test-integration/test/graql/reasoner/resources:unification-with-types"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)