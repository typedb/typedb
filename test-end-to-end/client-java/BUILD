java_test(
    name = "client-java-e2e",
    test_class = "ai.grakn.client.ClientJavaE2E",
    srcs = ["ClientJavaE2E.java", "ClientJavaE2EConstants.java"],
    deps = [
        "//grakn-graql:grakn-graql",
        "//client-java:client-java",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
#        "//dependencies/maven/artifacts/javax/ws/rs:jsr311-api",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    data = ["//:distribution"]
)