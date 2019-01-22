load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "keyspace-test",
    test_class = "grakn.core.server.keyspace.KeyspacesTest",
    srcs = ["KeyspacesTest.java"],
    deps = ["//server"],
    size = "small"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":keyspace-test"
    ],
)
