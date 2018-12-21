java_test(
    name = "attribute-deduplicator-e2e",
    test_class = "grakn.core.deduplicator.AttributeDeduplicatorE2E",
    srcs = ["AttributeDeduplicatorE2E.java", "AttributeDeduplicatorE2EConstants.java"],
    deps = [
        "//server",
        "//client-java",
        "//common",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    data = ["//:distribution"]
)

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
 name = "attribute-deduplicator-e2e-checkstyle",
 target = ":attribute-deduplicator-e2e",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
)
