java_library(
    name = "http",
    srcs = ["SimpleURI.java"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    visibility = ["//common:__subpackages__"],
)

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
 name = "http-checkstyle",
 target = ":http",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
)
