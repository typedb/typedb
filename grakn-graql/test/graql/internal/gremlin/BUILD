java_test(
    name = "graql-traversal-test",
    test_class = "ai.grakn.graql.internal.gremlin.GraqlTraversalTest",
    srcs = ["GraqlTraversalTest.java", "GraqlMatchers.java"],
    deps = [
        "//grakn-graql",
        "//util",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
    ],
    size = "small"
)

java_test(
    name = "conjunction-query-test",
    test_class = "ai.grakn.graql.internal.gremlin.ConjunctionQueryTest",
    srcs = ["ConjunctionQueryTest.java", "GraqlMatchers.java"],
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//grakn-graql"],
    size = "small"
)