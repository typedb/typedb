java_test(
    name = "query-equivalence-it",
    srcs = ["AtomicQueryEquivalenceIT.java"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryEquivalenceIT",
    deps = [
            "//server",
            "//test-integration/rule:concurrent-grakn-server",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava"
        ],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "query-it",
    srcs = ["AtomicQueryIT.java"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryIT",
    deps = [
            "//server",
            "//test-integration/rule:concurrent-grakn-server",
            "//test-integration/graql/reasoner/graph:geo-graph",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/graql/reasoner/resources:materialisation"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "query-unification-it",
    srcs = ["AtomicQueryUnificationIT.java", "TestQueryPattern.java"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryUnificationIT",
    deps = [
            "//server",
            "//test-integration/rule:concurrent-grakn-server",
            "//test-integration/graql/reasoner/graph:geo-graph",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//dependencies/maven/artifacts/commons-collections:commons-collections",
        ],
    resources = [
        "//test-integration/graql/reasoner/resources:generic-schema",
        "//test-integration/graql/reasoner/resources:unification-with-types"
        ],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)