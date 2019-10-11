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
        "//common",
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//test-integration/graql/reasoner/pattern:query-pattern",
        "@graknlabs_graql//java:graql",
    ],
)

java_library(
    name = "resource-pattern",
    srcs = ["AttributePattern.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//test-integration/graql/reasoner/pattern:query-pattern",
    ],
)

java_library(
    name = "type-pattern",
    srcs = ["TypePattern.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//test-integration/graql/reasoner/pattern:query-pattern",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":query-pattern",
        ":relation-pattern",
        ":resource-pattern",
        ":type-pattern",
    ],
)
