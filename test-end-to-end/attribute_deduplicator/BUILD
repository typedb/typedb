java_test(
    name = "attribute-deduplicator-e2e",
    test_class = "ai.grakn.attribute_deduplicator.AttributeDeduplicatorE2E",
    srcs = ["AttributeDeduplicatorE2E.java", "AttributeDeduplicatorE2EConstants.java"],
    deps = [
        "//grakn-graql:grakn-graql",
        "//client-java:client-java",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    data = ["//:distribution"]
)