load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "rule-cache-it",
    size = "medium",
    srcs = ["RuleCacheIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    test_class = "grakn.core.graql.reasoner.cache.RuleCacheIT",
    deps = [
        "//concept:concept",
        "//server:server",

        "@graknlabs_graql//java:graql",
        
        "//dependencies/maven/artifacts/com/google/guava",
        "//test-integration/rule:grakn-test-server",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":rule-cache-it"
    ],
)
