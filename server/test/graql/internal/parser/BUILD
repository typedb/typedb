java_test(
    name = "gramlin-visitor-test",
    test_class = "grakn.core.graql.internal.parser.GremlinVisitorTest",
    srcs = ["GremlinVisitorTest.java"],
    deps = [
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//server"],
    size = "small"
)

java_test(
    name = "query-parser-test",
    test_class = "grakn.core.graql.internal.parser.QueryParserTest",
    srcs = ["QueryParserTest.java"],
    deps = [
        "//server",
        "//common",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    size = "medium"
)

java_test(
    name = "query-to-string-test",
    test_class = "grakn.core.graql.internal.parser.QueryToStringTest",
    srcs = ["QueryToStringTest.java"],
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//server"],
    size = "small"
)