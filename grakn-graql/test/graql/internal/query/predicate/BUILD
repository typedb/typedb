java_test(
    name = "eq-predicate-test",
    test_class = "ai.grakn.graql.internal.query.predicate.EqPredicateTest",
    srcs = ["EqPredicateTest.java"],
    deps = [ "//grakn-graql"],
    size = "small"
)

java_test(
    name = "regex-predicate-test",
    test_class = "ai.grakn.graql.internal.query.predicate.RegexPredicateTest",
    srcs = ["RegexPredicateTest.java"],
    deps = [
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//grakn-graql"],
    size = "small"
)

java_test(
    name = "comparator-predicate-test",
    test_class = "ai.grakn.graql.internal.query.predicate.ComparatorPredicateTest",
    srcs = ["ComparatorPredicateTest.java"],
    deps = [
        "//grakn-graql",
        "//util",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    size = "small"
)