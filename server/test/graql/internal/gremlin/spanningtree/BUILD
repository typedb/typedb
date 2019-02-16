load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "chu-liu-edmonds-test",
    test_class = "grakn.core.graql.internal.gremlin.spanningtree.ChuLiuEdmondsTest",
    srcs = ["ChuLiuEdmondsTest.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "small"
)

java_test(
    name = "fibonacci-heap-test",
    test_class = "grakn.core.graql.internal.gremlin.spanningtree.datastructure.FibonacciHeapTest",
    srcs = ["datastructure/FibonacciHeapTest.java"],
    deps = ["//server", "//dependencies/maven/artifacts/com/google/guava:guava"],
    size = "small"
)

java_test(
    name = "fibonacci-queue-test",
    test_class = "grakn.core.graql.internal.gremlin.spanningtree.datastructure.FibonacciQueueTest",
    srcs = ["datastructure/FibonacciQueueTest.java"],
    deps = ["//server", "//dependencies/maven/artifacts/com/google/guava:guava"],
    size = "small"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":chu-liu-edmonds-test",
        ":fibonacci-heap-test",
        ":fibonacci-queue-test"
    ]
)