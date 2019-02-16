load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "graql-traversal-test",
    test_class = "grakn.core.graql.internal.gremlin.GraqlTraversalTest",
    srcs = ["GraqlTraversalTest.java", "GraqlMatchers.java"],
    deps = [
        "//common:common",
        "//graql/java:graql",
        "//server:server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
    ],
    size = "small"
)

java_test(
    name = "conjunction-query-test",
    test_class = "grakn.core.graql.internal.gremlin.ConjunctionQueryTest",
    srcs = ["ConjunctionQueryTest.java", "GraqlMatchers.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
    ],
    size = "small"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graql-traversal-test",
        ":conjunction-query-test"
    ],
)
