load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "query-cache-it",
    size = "medium",
    srcs = ["QueryCacheIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.graql.reasoner.cache.QueryCacheIT",
    deps = [
        "//common",
        "//concept/answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
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
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":query-cache-it",
        ":rule-cache-it",
    ],
)
