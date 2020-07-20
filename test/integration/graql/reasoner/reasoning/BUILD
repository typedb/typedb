load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "valuepredicate-it",
    size = "medium",
    srcs = ["ValuePredicateIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.ValuePredicateIT",
    deps = [
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "//graql/reasoner",
        "//kb/server",
        "//kb/concept/api",
        "//common",
        "//test/rule:grakn-test-server",
        "//test/common:graql-test-util",
        "@graknlabs_graql//java:graql",
        "@graknlabs_common//:common",
    ],
)

java_test(
    name = "recursion-it",
    size = "large",
    srcs = ["RecursionIT.java"],
    classpath_resources = [
        "//test/resources:logback-test",
        "//test/resources:cassandra-embedded",
    ],
    resources = [
        "//test/integration/graql/reasoner/resources/recursion:nguyen",
        "//test/integration/graql/reasoner/resources/recursion:recursion-schemas",
    ],
    test_class = "grakn.core.graql.reasoner.reasoning.RecursionIT",
    deps = [
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "//kb/server",
        "//test/integration/graql/reasoner/graph:dual-linear-transitivity-graph",
        "//test/integration/graql/reasoner/graph:linear-transitivity-matrix-graph",
        "//test/integration/graql/reasoner/graph:nguyen-graph",
        "//test/integration/graql/reasoner/graph:path-matrix-graph",
        "//test/integration/graql/reasoner/graph:path-tree-graph",
        "//test/integration/graql/reasoner/graph:path-tree-symmetric-graph",
        "//test/integration/graql/reasoner/graph:reachability-graph",
        "//test/integration/graql/reasoner/graph:tail-recursion-graph",
        "//test/rule:grakn-test-server",
        "//test/common:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":valuepredicate-it",
        ":recursion-it",
    ],
)
