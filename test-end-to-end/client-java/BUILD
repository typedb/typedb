load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "client-java-e2e",
    srcs = [
        "ClientJavaE2E.java",
        "ClientJavaE2EConstants.java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    data = ["//:assemble-mac-zip"],
    test_class = "grakn.core.client.ClientJavaE2E",
    deps = [
        # Internal dependencies
        "//common:common",

        # External dependencies from @graknlabs
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":client-java-e2e",
    ],
)
