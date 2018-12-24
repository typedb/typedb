java_library(
    name = "config",
    srcs = ["Config.java", "ConfigKey.java", "SystemProperty.java"],
    deps = [
        "//common/util:util",
        "//common/exception:exception",
        "//dependencies/maven/artifacts/com/google/auto/value:auto-value",
        "//dependencies/maven/artifacts/com/google/code/findbugs:annotations",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
    visibility = ["//common:__subpackages__"],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
    ],
)

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
    name = "config-checkstyle",
    target = ":config",
    config = "//config/checkstyle:checkstyle.xml",
    suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
    license = "//config/checkstyle:checkstyle-file-header.txt",
)
