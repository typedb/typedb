load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "resolve",
    srcs = [
        "QueryBuilder.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//test/behaviour/resolution/framework/common:common",
        "//concept/answer:answer",
        "//graql/reasoner:reasoner",
        "//kb/concept/api:api",
        "//kb/server:server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":resolve",
    ],
)