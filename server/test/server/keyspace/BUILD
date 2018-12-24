java_test(
    name = "keyspace-test",
    test_class = "grakn.core.server.keyspace.KeyspacesTest",
    srcs = ["KeyspacesTest.java"],
    deps = ["//server"],
    size = "small"
)

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
 name = "keyspace-test-checkstyle",
 target = ":keyspace-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)
