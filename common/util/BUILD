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

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
 name = "util-checkstyle",
 target = ":util",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
)