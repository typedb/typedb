load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
     name = "grakn-config-test",
     test_class = "grakn.core.common.config.test.ConfigTest",
     srcs = ["ConfigTest.java"],
     deps = ["//common/config:config", '//common/exception:exception'],
     resources = ["//server:conf/grakn.properties"],
     size = "small"
 )

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-config-test"
    ],
)
