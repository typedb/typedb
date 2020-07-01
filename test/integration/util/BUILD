load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "graql-test-util",
    srcs = ["GraqlTestUtil.java"],
    visibility = ["//test/integration:__subpackages__"],
    deps = [
        "@maven//:commons_collections_commons_collections",
        "@maven//:junit_junit",
        "//kb/server",
        "//kb/concept/api",
        "@graknlabs_graql//java:graql",
    ],
)

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
        ":graql-test-util",
        ":concept-downcasting",
    ],
)
