load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "common",
    srcs = [
        "ConjunctionFlatteningVisitor.java",
        "NegationRemovalVisitor.java",
        "PatternVisitor.java",
        "StatementVisitor.java",
        "Utils.java",
        "ResolutionConstraintException.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/server:server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":common",
    ],
)
