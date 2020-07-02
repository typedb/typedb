load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "concept-downcasting",
    srcs = ["ConceptDowncasting.java"],
    visibility = ["//test/integration:__subpackages__"],
    deps = [
        "//concept/impl:impl",
        "//kb/concept/api",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":concept-downcasting",
    ],
)
