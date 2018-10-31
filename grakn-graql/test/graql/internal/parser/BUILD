java_test(
    name = "gramlin-visitor-test",
    test_class = "ai.grakn.graql.internal.parser.GremlinVisitorTest",
    srcs = ["GremlinVisitorTest.java"],
    deps = [
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//grakn-graql"],
    size = "small"
)

java_test(
    name = "query-parser-test",
    test_class = "ai.grakn.graql.internal.parser.QueryParserTest",
    srcs = ["QueryParserTest.java"],
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//grakn-graql"],
    size = "medium"
)

java_test(
    name = "query-to-string-test",
    test_class = "ai.grakn.graql.internal.parser.QueryToStringTest",
    srcs = ["QueryToStringTest.java"],
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//grakn-graql"],
    size = "small"
)