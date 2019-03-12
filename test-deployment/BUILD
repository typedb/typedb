java_test(
    name = "test-deployment",
    test_class = "TestDeployment",
    srcs = ["TestDeployment.java"],
    deps = [
        "//common:common",
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
        "//server:server", # NEEDS TO GO
        "@graknlabs_grakn_core//concept"
    ]
)