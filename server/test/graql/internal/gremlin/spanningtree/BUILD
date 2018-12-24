load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "chu-liu-edmonds-test",
    test_class = "grakn.core.graql.internal.gremlin.spanningtree.ChuLiuEdmondsTest",
    srcs = ["ChuLiuEdmondsTest.java"],
    deps = ["//server", "//dependencies/maven/artifacts/com/google/guava:guava"],
    size = "small"
)

checkstyle_test(
 name = "chu-liu-edmonds-test-checkstyle",
 target = ":chu-liu-edmonds-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_test(
    name = "fibonacci-heap-test",
    test_class = "grakn.core.graql.internal.gremlin.spanningtree.datastructure.FibonacciHeapTest",
    srcs = ["datastructure/FibonacciHeapTest.java"],
    deps = ["//server", "//dependencies/maven/artifacts/com/google/guava:guava"],
    size = "small"
)

checkstyle_test(
 name = "fibonacci-heap-test-checkstyle",
 target = ":fibonacci-heap-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],
)

java_test(
    name = "fibonacci-queue-test",
    test_class = "grakn.core.graql.internal.gremlin.spanningtree.datastructure.FibonacciQueueTest",
    srcs = ["datastructure/FibonacciQueueTest.java"],
    deps = ["//server", "//dependencies/maven/artifacts/com/google/guava:guava"],
    size = "small"
)

checkstyle_test(
 name = "fibonacci-queue-test-checkstyle",
 target = ":fibonacci-queue-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)
