java_library(
    name = "common",
    srcs = [
        "ConjunctionFlatteningVisitor.java",
        "NegationRemovalVisitor.java",
        "PatternVisitor.java",
        "StatementVisitor.java",
        "Utils.java"
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/server:server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "common",
    targets = [
        ":resolve",
    ],
)
