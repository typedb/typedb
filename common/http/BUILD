load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_library(
    name = "http",
    srcs = ["SimpleURI.java"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    visibility = ["//common:__subpackages__"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":http"
    ],
)
