load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_library(
    name = "graql-test-util",
    srcs = ["GraqlTestUtil.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
      "//server",
      "//dependencies/maven/artifacts/commons-collections:commons-collections",
      "//dependencies/maven/artifacts/junit:junit"
    ]
)

checkstyle_test(
 name = "graql-test-util-checkstyle",
 target = ":graql-test-util",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],
)