load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "query-pattern",
    srcs = ["QueryPattern.java"],
    visibility = ["//test-integration:__subpackages__"],
)

java_library(
    name = "relation-pattern",
    srcs = ["RelationPattern.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "@graknlabs_graql//java:graql",
        "//kb/concept/api:api",
        "//common:common",
        "//server:server",
        "//dependencies/maven/artifacts/com/google/guava",
        "//test-integration/graql/reasoner/pattern:query-pattern",
    ],
)

java_library(
    name = "resource-pattern",
    srcs = ["AttributePattern.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api:api",
        "//server:server",
        "//test-integration/graql/reasoner/pattern:query-pattern",
    ],
)

java_library(
    name = "type-pattern",
    srcs = ["TypePattern.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//kb/concept/api:api",
        "//server:server",
        "//test-integration/graql/reasoner/pattern:query-pattern",
        "//dependencies/maven/artifacts/com/google/guava",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":query-pattern",
        ":relation-pattern",
        ":resource-pattern",
        ":type-pattern"
    ]
)