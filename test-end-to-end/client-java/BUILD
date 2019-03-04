load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "client-java-e2e",
    test_class = "grakn.core.client.ClientJavaE2E",
    srcs = ["ClientJavaE2E.java", "ClientJavaE2EConstants.java"],
    deps = [
        # Internal dependencies
        "//common:common",
        "//concept:concept",
        "//server:server",

        # External dependencies from @graknlabs
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    data = ["//:distribution-zip-mac"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":client-java-e2e"
    ],
)
