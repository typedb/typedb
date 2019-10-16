load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "grakn-application-test",
    srcs = ["GraknApplicationTest.java"],
    test_class = "grakn.core.test.common.GraknApplicationTest",
    deps = [
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-application-test",
    ],
)
