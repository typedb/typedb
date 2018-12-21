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

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
 name = "exception-checkstyle",
 target = ":exception",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)
