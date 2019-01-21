load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "eq-predicate-test",
    test_class = "grakn.core.graql.query.predicate.EqPredicateTest",
    srcs = ["EqPredicateTest.java"],
    deps = [ "//server"],
    size = "small"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":eq-predicate-test",
        ":regex-predicate-test",
        ":comparator-predicate-test"
    ]
)

java_test(
    name = "regex-predicate-test",
    test_class = "grakn.core.graql.query.predicate.RegexPredicateTest",
    srcs = ["RegexPredicateTest.java"],
    deps = [
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//server"],
    size = "small"
)

java_test(
    name = "comparator-predicate-test",
    test_class = "grakn.core.graql.query.predicate.ComparatorPredicateTest",
    srcs = ["ComparatorPredicateTest.java"],
    deps = [
        "//server",
        "//common",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    size = "small"
)
