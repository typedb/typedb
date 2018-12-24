java_test(
     name = "grakn-config-test",
     test_class = "grakn.core.common.config.test.ConfigTest",
     srcs = ["ConfigTest.java"],
     deps = ["//common/config:config", '//common/exception:exception'],
     resources = ["//server:conf/grakn.properties"],
     size = "small"
 )

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
 name = "grakn-config-test-checkstyle",
 target = ":grakn-config-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],
)
