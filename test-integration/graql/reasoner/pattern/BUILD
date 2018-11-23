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
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/graql/reasoner/pattern:query-pattern",
    ],
)

java_library(
    name = "resource-pattern",
    srcs = ["ResourcePattern.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/graql/reasoner/pattern:query-pattern",
    ],
)

java_library(
    name = "type-pattern",
    srcs = ["TypePattern.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/graql/reasoner/pattern:query-pattern",
    ],
)
