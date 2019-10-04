load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "grakn-application-test",
    test_class = "grakn.core.test.common.GraknApplicationTest",
    srcs = ["GraknApplicationTest.java"],
    deps = [
        "//common:common",
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
    ]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-application-test"
    ],
)
