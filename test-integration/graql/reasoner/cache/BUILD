java_test(
    name = "lazy-query-cache-it",
    size = "medium",
    srcs = ["LazyQueryCacheIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    test_class = "grakn.core.graql.reasoner.cache.LazyQueryCacheIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "rule-cache-it",
    size = "medium",
    srcs = ["RuleCacheIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    test_class = "grakn.core.graql.reasoner.cache.RuleCacheIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)
