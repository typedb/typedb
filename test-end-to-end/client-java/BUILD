java_test(
    name = "client-java-e2e",
    test_class = "grakn.core.client.ClientJavaE2E",
    srcs = ["ClientJavaE2E.java", "ClientJavaE2EConstants.java"],
    deps = [
        "//client-java",
        "//server",
        "//common",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    data = ["//:distribution"]
)

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
 name = "client-java-e2e-checkstyle",
 target = ":client-java-e2e",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
)
