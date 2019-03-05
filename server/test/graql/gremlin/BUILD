load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "graql-traversal-test",
    test_class = "grakn.core.graql.gremlin.GraqlTraversalTest",
    srcs = ["GraqlTraversalTest.java", "GraqlMatchers.java"],
    deps = [
        "//common:common",
        "//concept:concept",
        "//server:server",

        "@graknlabs_graql//java:graql",

        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
    ],
    size = "small"
)

java_test(
    name = "conjunction-query-test",
    test_class = "grakn.core.graql.gremlin.ConjunctionQueryTest",
    srcs = ["ConjunctionQueryTest.java", "GraqlMatchers.java"],
    deps = [
        "//concept:concept",
        "//server:server",

        "@graknlabs_graql//java:graql",

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
