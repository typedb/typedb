load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":regex-predicate-test",
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
