load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "client-java-e2e",
    test_class = "grakn.core.client.ClientJavaE2E",
    srcs = ["ClientJavaE2E.java", "ClientJavaE2EConstants.java"],
    deps = [
        "//client-java:client-java",
        "//common:common",
        "@graknlabs_graql//java:graql",
        "//server:server",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    data = ["//:distribution"]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":client-java-e2e"
    ],
)
