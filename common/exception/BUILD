load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_library(
    name = "exception",
    srcs = [
        "ErrorMessage.java",
        "GraknException.java",
        "Validator.java"
    ],
    deps = [
        "//dependencies/maven/artifacts/com/google/code/findbugs:annotations",
    ],
    visibility = ["//common:__subpackages__"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":exception"
    ],
)
