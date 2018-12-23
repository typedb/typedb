load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "eq-predicate-test",
    test_class = "grakn.core.graql.query.predicate.EqPredicateTest",
    srcs = ["EqPredicateTest.java"],
    deps = [ "//server"],
    size = "small"
)

checkstyle_test(
 name = "eq-predicate-test-checkstyle",
 target = ":eq-predicate-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
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

checkstyle_test(
 name = "regex-predicate-test-checkstyle",
 target = ":regex-predicate-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
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

checkstyle_test(
 name = "comparator-predicate-test-checkstyle",
 target = ":comparator-predicate-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
)
