load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")


java_library(
    name = "graql-test-util",
    srcs = ["GraqlTestUtil.java"],
    visibility = ["//test:__subpackages__"],
    deps = [
        "@maven//:commons_collections_commons_collections",
        "@maven//:junit_junit",
        "//kb/server",
        "//kb/concept/api",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graql-test-util",
    ],
)
