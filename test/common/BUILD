load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "grakn-application-test",
    test_class = "GraknApplicationTest",
    srcs = ["GraknApplicationTest.java"],
    deps = [
        "//common:common",
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
        "//server:server", # NEEDS TO GO
        "@graknlabs_grakn_core//concept"
    ]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-application-test"
    ],
)
