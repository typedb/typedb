load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "grakn-application-test",
    srcs = ["GraknApplicationTest.java"],
    test_class = "grakn.core.test.common.GraknApplicationTest",
    deps = [
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
    ],
)

java_library(
    name = "graql-test-util",
    srcs = ["GraqlTestUtil.java"],
    visibility = ["//test:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/junit",
        "//kb/server",
        "//kb/concept/api",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-application-test",
        ":graql-test-util",
    ],
)
