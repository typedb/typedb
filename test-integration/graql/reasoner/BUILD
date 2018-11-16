java_test(
    name = "reasoning-it",
    size = "enormous",
    srcs = ["ReasoningIT.java"],
    classpath_resources = [
        "//test-integration/resources:logback-test",
        "//test-integration/resources:cassandra-embedded",
    ],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.ReasoningIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:concurrent-grakn-server",
    ],
)

java_test(
    name = "recursion-it",
    size = "large",
    srcs = ["RecursionIT.java"],
    classpath_resources = [
        "//test-integration/resources:logback-test",
        "//test-integration/resources:cassandra-embedded",
    ],
    resources = [
        "//test-integration/graql/reasoner/resources/recursion:recursion-schemas",
        "//test-integration/graql/reasoner/resources/recursion:nguyen"],
    test_class = "grakn.core.graql.reasoner.RecursionIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:concurrent-grakn-server",
        "//test-integration/graql/reasoner/graph:tail-recursion-graph",
        "//test-integration/graql/reasoner/graph:path-tree-symmetric-graph",
        "//test-integration/graql/reasoner/graph:path-tree-graph",
        "//test-integration/graql/reasoner/graph:path-matrix-graph",
        "//test-integration/graql/reasoner/graph:nguyen-graph",
        "//test-integration/graql/reasoner/graph:dual-linear-transitivity-graph",
        "//test-integration/graql/reasoner/graph:linear-transitivity-matrix-graph",
    ],
)

java_test(
    name = "query-validity-it",
    size = "medium",
    srcs = ["QueryValidityIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    test_class = "grakn.core.graql.reasoner.QueryValidityIT",
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:concurrent-grakn-server",
    ],
)

java_test(
    name = "resolution-plan-it",
    size = "medium",
    srcs = ["ResolutionPlanIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:resolution-plan"],
    test_class = "grakn.core.graql.reasoner.ResolutionPlanIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:concurrent-grakn-server",
    ],
)

java_test(
    name = "query-it",
    size = "medium",
    srcs = ["QueryIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.reasoner.QueryIT",
    deps = [
        "//server",
        "//test-integration/graql/reasoner/graph:geo-graph",
        "//test-integration/rule:concurrent-grakn-server",
    ],
)

java_test(
    name = "ontological-query-it",
    size = "large",
    srcs = ["OntologicalQueryIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    data = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    test_class = "grakn.core.graql.reasoner.OntologicalQueryIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/rule:concurrent-grakn-server",
        "//test-integration/util:graql-test-util",
    ],
)
