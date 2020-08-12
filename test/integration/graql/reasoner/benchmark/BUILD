load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "rule-scaling-it",
    size = "medium",
    srcs = ["RuleScalingIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.reasoner.benchmark.RuleScalingIT",
    deps = [
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "@maven//:org_slf4j_slf4j_api",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "benchmark-big-it",
    size = "enormous",
    srcs = ["BenchmarkBigIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    data = [
        "//test/integration/graql/reasoner/resources:linear-transivity",
        "//test/integration/graql/reasoner/resources:multi-join",
    ],
    test_class = "grakn.core.graql.reasoner.benchmark.BenchmarkBigIT",
    deps = [
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "@maven//:org_slf4j_slf4j_api",
        "//test/rule:grakn-test-server",
        "//concept/answer",
        "//kb/concept/api",
        "//kb/server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "benchmark-small-it",
    size = "large",
    srcs = ["BenchmarkSmallIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = [
        "//test/integration/graql/reasoner/resources:generic-schema",
        "//test/integration/graql/reasoner/resources:unification-with-types",
    ],
    test_class = "grakn.core.graql.reasoner.benchmark.BenchmarkSmallIT",
    deps = [
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "//kb/concept/api",
        "//kb/server",
        "//test/integration/graql/reasoner/graph:diagonal-graph",
        "//test/integration/graql/reasoner/graph:linear-transitivity-matrix-graph",
        "//test/integration/graql/reasoner/graph:path-tree-graph",
        "//test/integration/graql/reasoner/graph:transitivity-chain-graph",
        "//test/integration/graql/reasoner/graph:transitivity-matrix-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":rule-scaling-it",
        ":benchmark-big-it",
        ":benchmark-small-it",
    ],
)
