load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_library(
    name = "util",
    srcs = ["CommonUtil.java",
            "//common/templates/util:grakn-version"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/code/findbugs:annotations",
    ],
    visibility = ["//common:__subpackages__"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":util"
    ],
)
