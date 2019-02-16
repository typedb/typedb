load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_library(
    name = "graql-test-util",
    srcs = ["GraqlTestUtil.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
      "//graql/java:graql",
      "//server:server",
      "//dependencies/maven/artifacts/commons-collections:commons-collections",
      "//dependencies/maven/artifacts/junit:junit"
    ]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graql-test-util"
    ],
)
