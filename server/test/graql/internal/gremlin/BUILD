load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "graql-traversal-test",
    test_class = "grakn.core.graql.internal.gremlin.GraqlTraversalTest",
    srcs = ["GraqlTraversalTest.java", "GraqlMatchers.java"],
    deps = [
        "//server",
        "//common",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
    ],
    size = "small"
)

checkstyle_test(
 name = "graql-traversal-test-checkstyle",
 target = ":graql-traversal-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "conjunction-query-test",
    test_class = "grakn.core.graql.internal.gremlin.ConjunctionQueryTest",
    srcs = ["ConjunctionQueryTest.java", "GraqlMatchers.java"],
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//server"],
    size = "small"
)

checkstyle_test(
 name = "conjunction-query-test-checkstyle",
 target = ":conjunction-query-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)
