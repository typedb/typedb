java_test(
    name = "neq-predicate-it",
    size = "small",
    srcs = ["NeqPredicateIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.NeqPredicateIT",
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
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
        "//test-integration/graql/reasoner/resources/recursion:nguyen",
        "//test-integration/graql/reasoner/resources/recursion:recursion-schemas",
    ],
    test_class = "grakn.core.graql.reasoner.reasoning.RecursionIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/graql/reasoner/graph:dual-linear-transitivity-graph",
        "//test-integration/graql/reasoner/graph:linear-transitivity-matrix-graph",
        "//test-integration/graql/reasoner/graph:nguyen-graph",
        "//test-integration/graql/reasoner/graph:path-matrix-graph",
        "//test-integration/graql/reasoner/graph:path-tree-graph",
        "//test-integration/graql/reasoner/graph:path-tree-symmetric-graph",
        "//test-integration/graql/reasoner/graph:tail-recursion-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

java_test(
    name = "resource-attachment-it",
    size = "small",
    srcs = ["ResourceAttachmentIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.ResourceAttachmentIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

java_test(
    name = "type-generation-it",
    size = "small",
    srcs = ["TypeGenerationIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.TypeGenerationIT",
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

java_test(
    name = "type-hierarchies-it",
    size = "small",
    srcs = ["TypeHierarchiesIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.TypeHierarchiesIT",
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

java_test(
    name = "variable-roles-it",
    size = "small",
    srcs = ["VariableRolesIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.VariableRolesIT",
    deps = [
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)
