java_test(
    name = "lazy-query-cache-it",
    srcs = ["LazyQueryCacheIT.java"],
    test_class = "grakn.core.graql.reasoner.cache.LazyQueryCacheIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:concurrent-grakn-server",
                "//dependencies/maven/artifacts/com/google/guava:guava"
            ],
    resources = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "query-cache-it",
    srcs = ["QueryCacheIT.java"],
    test_class = "grakn.core.graql.reasoner.cache.QueryCacheIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:concurrent-grakn-server",
                "//dependencies/maven/artifacts/com/google/guava:guava"
            ],
    resources = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "rule-cache-it",
    srcs = ["RuleCacheIT.java"],
    test_class = "grakn.core.graql.reasoner.cache.RuleCacheIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:concurrent-grakn-server",
                "//dependencies/maven/artifacts/com/google/guava:guava"
            ],
    resources = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)