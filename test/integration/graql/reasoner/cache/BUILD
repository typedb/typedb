load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "query-cache-it",
    size = "medium",
    srcs = ["QueryCacheIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/resources:generic-schema-refactored"],
    test_class = "grakn.core.graql.reasoner.cache.QueryCacheIT",
    deps = [
        "//common",
        "//concept/answer",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
    ],
)

java_test(
    name = "rule-cache-it",
    size = "medium",
    srcs = ["RuleCacheIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = [],
    test_class = "grakn.core.graql.reasoner.cache.RuleCacheIT",
    deps = [
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
    ],
)

java_test(
    name = "semantic-difference-it",
    size = "medium",
    srcs = ["SemanticDifferenceIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = [
        "//test/integration/graql/reasoner/resources:generic-schema-refactored",
    ],
    test_class = "grakn.core.graql.reasoner.cache.SemanticDifferenceIT",
    deps = [
        "//common",
        "//concept/answer",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":query-cache-it",
        ":rule-cache-it",
        ":semantic-difference-it",
    ],
)
