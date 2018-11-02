java_test(
    name = "batch-executor-client-it",
    srcs = ["BatchExecutorClientIT.java"],
    test_class = "ai.grakn.test.batch.BatchExecutorClientIT",
    deps = [
        "//client-java",
        "//grakn-graql",
        "//test-integration/rule:grakn-server",
        "//dependencies/maven/artifacts/org/mockito:mockito-core"
    ],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)